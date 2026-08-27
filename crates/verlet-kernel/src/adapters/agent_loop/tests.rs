use verlet_history::EventStore as _;
use verlet_history::ObservationStore as _;
use verlet_history::SessionStore as _;

#[derive(Default)]
struct RecordingClient {
    requests: std::sync::Mutex<Vec<verlet_provider::ProviderRequest>>,
    responses: std::sync::Mutex<Vec<verlet_provider::ProviderResponse>>,
    capabilities: Option<verlet_provider::ProviderCapabilityRecord>,
}

struct MutableTurnEndpointRouter {
    endpoint: std::sync::Mutex<Option<crate::adapters::agent_loop::ResolvedTurnEndpoint>>,
}

impl MutableTurnEndpointRouter {
    fn new(endpoint: crate::adapters::agent_loop::ResolvedTurnEndpoint) -> Self {
        Self {
            endpoint: std::sync::Mutex::new(Some(endpoint)),
        }
    }

    fn set(&self, endpoint: crate::adapters::agent_loop::ResolvedTurnEndpoint) {
        *self.endpoint.lock().unwrap() = Some(endpoint);
    }
}

impl crate::adapters::agent_loop::TurnEndpointRouter for MutableTurnEndpointRouter {
    fn resolve(&self) -> Option<crate::adapters::agent_loop::ResolvedTurnEndpoint> {
        self.endpoint.lock().unwrap().clone()
    }
}

struct GatedRecordingClient {
    requests: std::sync::Mutex<Vec<verlet_provider::ProviderRequest>>,
    responses: std::sync::Mutex<std::collections::VecDeque<verlet_provider::ProviderResponse>>,
    first_request_started: tokio::sync::Notify,
    release_first_request: tokio::sync::Notify,
}

impl GatedRecordingClient {
    fn new(responses: Vec<verlet_provider::ProviderResponse>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            responses: std::sync::Mutex::new(responses.into()),
            first_request_started: tokio::sync::Notify::new(),
            release_first_request: tokio::sync::Notify::new(),
        }
    }

    fn requests(&self) -> Vec<verlet_provider::ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl RecordingClient {
    fn with_responses(responses: Vec<verlet_provider::ProviderResponse>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            responses: std::sync::Mutex::new(responses.into_iter().rev().collect()),
            capabilities: None,
        }
    }

    fn with_capabilities(
        mut self,
        capabilities: verlet_provider::ProviderCapabilityRecord,
    ) -> Self {
        self.capabilities = Some(capabilities);
        self
    }

    fn requests(&self) -> Vec<verlet_provider::ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

enum ScriptedResponse {
    Pending,
    Error(verlet_provider::ProviderError),
    Response(verlet_provider::ProviderResponse),
}

struct ScriptedClient {
    requests: std::sync::Mutex<Vec<verlet_provider::ProviderRequest>>,
    responses: std::sync::Mutex<std::collections::VecDeque<ScriptedResponse>>,
}

struct StreamingClient {
    requests: std::sync::Mutex<Vec<verlet_provider::ProviderRequest>>,
    events: std::sync::Mutex<std::collections::VecDeque<Vec<verlet_provider::ProviderStreamEvent>>>,
}

struct BashMandateListClient {
    barrier: std::sync::Arc<tokio::sync::Barrier>,
}

struct TurnContextRecordingKernelToolProvider {
    snapshots:
        std::sync::Mutex<Vec<Option<crate::kernel::runtime_host::turn::TurnContextSnapshot>>>,
}

struct WitnessCheckingEchoProvider {
    store: std::sync::Arc<verlet_history::InMemorySessionStore>,
    expected_command_sha256: String,
    seen_arguments: std::sync::Mutex<Vec<serde_json::Value>>,
}

struct FinishSecondFirstToolProvider {
    second_finished: tokio::sync::Notify,
}

struct SerialBlockingToolProvider {
    tool_name: &'static str,
    started: tokio::sync::mpsc::UnboundedSender<String>,
    release_first: tokio::sync::Notify,
}

struct CancellationAcknowledgingThreadToolProvider {
    started: tokio::sync::mpsc::UnboundedSender<String>,
    acknowledged: tokio::sync::mpsc::UnboundedSender<String>,
}

struct NonObservingThreadToolProvider {
    started: tokio::sync::mpsc::UnboundedSender<String>,
    released: std::sync::atomic::AtomicBool,
    release: tokio::sync::Notify,
    never_launched: std::sync::atomic::AtomicBool,
}

struct PanickingAfterGraceToolProvider {
    started: tokio::sync::mpsc::UnboundedSender<()>,
    release: tokio::sync::Notify,
}

impl NonObservingThreadToolProvider {
    fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

struct ImmediateThreadToolProvider;

struct IsolatedFailureToolProvider;

#[derive(Default)]
struct RecoveryCountingToolProvider {
    invocations: std::sync::atomic::AtomicU64,
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for RecoveryCountingToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "recovery_tool",
            "Recovery contract test tool.",
            serde_json::json!({"type":"object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        self.invocations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            format!("executed:{}", call.arguments["input"].as_str().unwrap()),
            false,
        )))
    }
}

#[test]
fn recovery_action_truth_table_honors_effect_class_and_fingerprint() {
    for effect_class in [
        verlet_agent::manifest_schema::EffectClass::Pure,
        verlet_agent::manifest_schema::EffectClass::Idempotent,
        verlet_agent::manifest_schema::EffectClass::AtMostOnce,
    ] {
        for outcome_exists in [false, true] {
            for fingerprint_matches in [false, true] {
                let expected = if outcome_exists && fingerprint_matches {
                    crate::adapters::agent_loop::ToolRecoveryAction::Reuse
                } else if effect_class == verlet_agent::manifest_schema::EffectClass::AtMostOnce {
                    crate::adapters::agent_loop::ToolRecoveryAction::ConservativeFailure
                } else {
                    crate::adapters::agent_loop::ToolRecoveryAction::Reexecute
                };
                assert_eq!(
                    crate::adapters::agent_loop::tool_recovery_action(
                        effect_class,
                        outcome_exists,
                        fingerprint_matches
                    ),
                    expected,
                    "{effect_class:?} outcome_exists={outcome_exists} fingerprint_matches={fingerprint_matches}"
                );
            }
        }
    }
}

fn recovery_bind_receipt(
    effect_class: verlet_agent::manifest_schema::EffectClass,
) -> crate::agent::manifest_bind::AgentManifestBindReceipt {
    crate::agent::manifest_bind::AgentManifestBindReceipt {
        ref_uri: "agent://test/recovery".to_string(),
        manifest_hash: "snapshot-recovery".to_string(),
        model_profile_origin: None,
        placement_origin: None,
        workspace_origin: None,
        model_profile_id: "default".to_string(),
        provider_id: "test".to_string(),
        model_id: "model".to_string(),
        tool_ids: vec!["recovery".to_string()],
        operation_bindings: vec![crate::agent::manifest_bind::AgentManifestOperationBinding {
            name: "recovery".to_string(),
            artifact_hash: "test".to_string(),
            effect_class,
            attachment_config: verlet_wasm::WasmAttachmentConfig::default(),
            bound_parameters: std::collections::BTreeMap::new(),
            operations: vec!["recovery_tool".to_string()],
            direct_tools: vec![
                crate::agent::manifest_bind::AgentManifestDirectToolBinding {
                    tool_name: "recovery_tool".to_string(),
                    operation: "recovery_tool".to_string(),
                    effect_class,
                },
            ],
        }],
        skill_packages: Vec::new(),
        skill_discovery: None,
        static_context_segments: Vec::new(),
        tool_universes: Vec::new(),
        couplings: Vec::new(),
        effective_runtime: verlet_agent::manifest_schema::AgentManifestRuntimeDefaults::default(),
        overridden_keys: Vec::new(),
        placement: None,
        workspace: None,
    }
}

async fn append_recovery_bind_receipt(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    effect_class: verlet_agent::manifest_schema::EffectClass,
) {
    append_current_bind_receipt(store, coordinates, recovery_bind_receipt(effect_class)).await;
}

async fn append_current_bind_receipt(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    receipt: crate::agent::manifest_bind::AgentManifestBindReceipt,
) {
    let bind = verlet_history::NewEventRecord::witnessed(
        coordinates.clone(),
        verlet_history::EventKind::ManifestBindCompleted,
        serde_json::to_value(&receipt).unwrap(),
    );
    let bindings = receipt
        .operation_bindings
        .iter()
        .map(|binding| {
            verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::BindingAttached,
                serde_json::to_value(crate::agent::manifest_bind::binding_attached_payload(
                    binding,
                    "principal:test",
                ))
                .unwrap(),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::for_thread(coordinates)],
                    source_event_ids: vec![bind.id],
                    discharged_by: Some("binder:manifest".to_string()),
                    function: Some("bind/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )
        })
        .collect::<Vec<_>>();
    let mut events = Vec::with_capacity(1 + bindings.len());
    events.push(bind);
    events.extend(bindings);
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            events,
        )
        .await
        .unwrap();
}

#[test]
fn bash_effect_class_requires_one_exactly_attributable_operation() {
    let mut receipt = recovery_bind_receipt(verlet_agent::manifest_schema::EffectClass::Idempotent);
    receipt.operation_bindings[0].operations = vec!["safe".to_string()];
    receipt.operation_bindings[0].direct_tools.clear();

    assert_eq!(
        crate::adapters::agent_loop::effect_class_from_bind_receipt(
            &receipt,
            verlet_vbash::BASH_TOOL,
            &serde_json::json!({"command": "safe argument"}),
        ),
        verlet_agent::manifest_schema::EffectClass::Idempotent
    );
    for command in [
        "FOO=1 safe",
        "safe; destructive",
        "safe ; destructive",
        "safe && destructive",
        "safe || destructive",
        "safe | destructive",
        "safe\ndestructive",
        "safe > /tmp/output",
        "safe $(destructive)",
        "safe `destructive`",
        "\"safe\" argument",
    ] {
        assert_eq!(
            crate::adapters::agent_loop::effect_class_from_bind_receipt(
                &receipt,
                verlet_vbash::BASH_TOOL,
                &serde_json::json!({"command": command}),
            ),
            verlet_agent::manifest_schema::EffectClass::AtMostOnce,
            "compound or shell-evaluated command {command:?} must fail closed"
        );
    }
}

async fn append_recovery_request(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    arguments: serde_json::Value,
) -> verlet_history::EventRecord {
    let fingerprint =
        crate::agent::tool_universe::args_fingerprint("recovery_tool", &arguments).unwrap();
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::ToolCallRequested,
                serde_json::to_value(crate::kernel::control_decision::ToolCallRequestedPayload {
                    subject: crate::kernel::control_decision::ToolCallSubject {
                        turn_id: "turn-recovery".to_string(),
                        call_id: "call-recovery".to_string(),
                    },
                    snapshot_id: "snapshot-recovery".to_string(),
                    tool_name: "recovery_tool".to_string(),
                    arguments,
                    attach_event_id: None,
                    args_fingerprint: Some(fingerprint),
                    holds: Vec::new(),
                })
                .unwrap(),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn recovery_after_store_reopen(
    effect_class: verlet_agent::manifest_schema::EffectClass,
    prior_arguments: serde_json::Value,
    recorded_result: Option<&str>,
    current_arguments: serde_json::Value,
) -> (u64, verlet_history::CanonicalMessage) {
    let path = temp_db_path("verlet-tool-recovery");
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "recovery");
    {
        let store = std::sync::Arc::new(
            verlet_history_sqlite::SqliteSessionStore::open(&path)
                .await
                .unwrap(),
        );
        append_recovery_bind_receipt(store.as_ref(), &coordinates, effect_class).await;
        let request = append_recovery_request(store.as_ref(), &coordinates, prior_arguments).await;
        if let Some(recorded_result) = recorded_result {
            let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
                store,
                crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
            );
            services
                .append_agent_loop_session_entry(
                    &coordinates,
                    None,
                    verlet_history::SessionEntryKind::Message {
                        message: verlet_history::CanonicalMessage::tool_result(
                            "call-recovery",
                            "recovery_tool",
                            recorded_result,
                            false,
                        ),
                    },
                    vec![request.id],
                )
                .await
                .unwrap();
            let request_payload = serde_json::from_value::<
                crate::kernel::control_decision::ToolCallRequestedPayload,
            >(request.payload)
            .unwrap();
            crate::adapters::agent_loop::append_tool_completion_event(
                &services,
                &coordinates,
                "turn-recovery".to_string(),
                "call-recovery".to_string(),
                "snapshot-recovery".to_string(),
                "recovery_tool".to_string(),
                request_payload.args_fingerprint,
                true,
                Some(1),
                Some(0),
                None,
            )
            .await
            .unwrap();
        }
    }

    let store = std::sync::Arc::new(
        verlet_history_sqlite::SqliteSessionStore::open(&path)
            .await
            .unwrap(),
    );
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store,
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let current_request = append_recovery_request(
        services.runtime_store().as_ref(),
        &coordinates,
        current_arguments.clone(),
    )
    .await;
    let current_payload = serde_json::from_value::<
        crate::kernel::control_decision::ToolCallRequestedPayload,
    >(current_request.payload)
    .unwrap();
    let mut calls = vec![crate::adapters::agent_loop::WitnessedToolCall {
        tool_call: crate::adapters::agent_loop::ProviderToolCall {
            id: "call-recovery".to_string(),
            name: "recovery_tool".to_string(),
            arguments: current_arguments,
        },
        snapshot_id: "snapshot-recovery".to_string(),
        args_fingerprint: current_payload.args_fingerprint.clone(),
        request_event_id: current_request.id,
        holds: Vec::new(),
        recovery_action: crate::adapters::agent_loop::ToolRecoveryAction::Reexecute,
        recovery_source_event_id: None,
        recovery_fingerprint_mismatch: false,
    }];
    crate::adapters::agent_loop::apply_tool_recovery_actions(
        &services,
        &coordinates,
        "turn-recovery",
        &mut calls,
    )
    .await
    .unwrap();

    let provider = std::sync::Arc::new(RecoveryCountingToolProvider::default());
    let kernel_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = provider.clone();
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(kernel_provider),
    );
    let interceptor = crate::agent::tool_interceptor::ToolExecutionInterceptor::new(router);
    let turn_context = crate::kernel::runtime_host::turn::TurnContext::new(
        verlet_runtime_contracts::ThreadContext::root(coordinates),
        "turn-recovery",
        &crate::kernel::runtime_host::turn::TurnInput::text(""),
        tokio_util::sync::CancellationToken::new(),
    );
    let (events, _) = tokio::sync::broadcast::channel(8);
    let prepared = crate::adapters::agent_loop::prepare_tool_call(
        &interceptor,
        &services,
        &turn_context,
        &events,
        calls.pop().unwrap(),
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        crate::agent::agent_tool_router::ToolInvocationCancellation::never(),
    )
    .await
    .unwrap();
    let crate::adapters::agent_loop::PreparedToolCallOutcome::Completed { ref outcome, .. } =
        prepared
    else {
        panic!("recovery should produce a completed outcome");
    };
    let result = outcome.result.clone();
    crate::adapters::agent_loop::append_detached_tool_call_outcome(
        &services,
        &turn_context,
        turn_context.coordinates().thread_id,
        &events,
        Ok(prepared),
    )
    .await
    .unwrap();
    assert!(
        crate::adapters::agent_loop::matching_tool_call_completed_exists(
            &services,
            turn_context.coordinates(),
            "turn-recovery",
            "call-recovery",
            "snapshot-recovery",
            current_payload.args_fingerprint.as_deref(),
        )
        .await
        .unwrap(),
        "recovery completion must echo the current fingerprint"
    );
    assert!(
        crate::adapters::agent_loop::existing_tool_result_message(
            &services,
            turn_context.coordinates(),
            current_request.id,
            "call-recovery",
            "snapshot-recovery",
            current_payload.args_fingerprint.as_deref(),
        )
        .await
        .unwrap()
        .is_some(),
        "recovery outcome must be witnessed from the current request"
    );
    let invocations = provider
        .invocations
        .load(std::sync::atomic::Ordering::SeqCst);
    drop(services);
    let _ = std::fs::remove_file(path);
    (invocations, result)
}

#[tokio::test]
async fn crash_cut_idempotent_dangling_request_reexecutes_after_store_reopen() {
    let (invocations, result) = recovery_after_store_reopen(
        verlet_agent::manifest_schema::EffectClass::Idempotent,
        serde_json::json!({"input":"same"}),
        None,
        serde_json::json!({"input":"same"}),
    )
    .await;
    assert_eq!(invocations, 1);
    assert_eq!(
        crate::adapters::agent_loop::text_from_message(&result),
        "executed:same"
    );
}

#[tokio::test]
async fn crash_cut_at_most_once_dangling_request_records_conservative_failure() {
    let (invocations, result) = recovery_after_store_reopen(
        verlet_agent::manifest_schema::EffectClass::AtMostOnce,
        serde_json::json!({"input":"same"}),
        None,
        serde_json::json!({"input":"same"}),
    )
    .await;
    assert_eq!(invocations, 0);
    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult { is_error: true, .. }
    ));
    let text = crate::adapters::agent_loop::text_from_message(&result);
    assert!(text.contains("interrupted"), "{text}");
    assert!(text.contains("effect class at-most-once"), "{text}");
}

#[tokio::test]
async fn completed_matching_fingerprint_reuses_and_mismatch_never_reuses() {
    let (matching_invocations, matching_result) = recovery_after_store_reopen(
        verlet_agent::manifest_schema::EffectClass::Idempotent,
        serde_json::json!({"input":"same"}),
        Some("recorded:same"),
        serde_json::json!({"input":"same"}),
    )
    .await;
    assert_eq!(matching_invocations, 0);
    assert_eq!(
        crate::adapters::agent_loop::text_from_message(&matching_result),
        "recorded:same"
    );

    let (mismatch_invocations, mismatch_result) = recovery_after_store_reopen(
        verlet_agent::manifest_schema::EffectClass::Idempotent,
        serde_json::json!({"input":"old"}),
        Some("recorded:old"),
        serde_json::json!({"input":"new"}),
    )
    .await;
    assert_eq!(mismatch_invocations, 1);
    assert_eq!(
        crate::adapters::agent_loop::text_from_message(&mismatch_result),
        "executed:new"
    );

    let (at_most_once_invocations, at_most_once_result) = recovery_after_store_reopen(
        verlet_agent::manifest_schema::EffectClass::AtMostOnce,
        serde_json::json!({"input":"old"}),
        Some("recorded:old"),
        serde_json::json!({"input":"new"}),
    )
    .await;
    assert_eq!(at_most_once_invocations, 0);
    let text = crate::adapters::agent_loop::text_from_message(&at_most_once_result);
    assert!(text.contains("fingerprint mismatch"), "{text}");
    assert!(text.contains("effect class at-most-once"), "{text}");
}

#[tokio::test]
async fn completion_without_canonical_result_degrades_by_effect_class() {
    for (label, effect_class, expected) in [
        (
            "idempotent",
            verlet_agent::manifest_schema::EffectClass::Idempotent,
            crate::adapters::agent_loop::ToolRecoveryAction::Reexecute,
        ),
        (
            "at-most-once",
            verlet_agent::manifest_schema::EffectClass::AtMostOnce,
            crate::adapters::agent_loop::ToolRecoveryAction::ConservativeFailure,
        ),
    ] {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
            store.clone(),
            crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
        );
        let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
            "tenant_a",
            "user_1",
            format!("completion-only-{label}"),
        );
        append_recovery_bind_receipt(store.as_ref(), &coordinates, effect_class).await;
        let arguments = serde_json::json!({"input":"same"});
        let prior = append_recovery_request(store.as_ref(), &coordinates, arguments.clone()).await;
        let prior_payload = serde_json::from_value::<
            crate::kernel::control_decision::ToolCallRequestedPayload,
        >(prior.payload)
        .unwrap();
        crate::adapters::agent_loop::append_tool_completion_event(
            &services,
            &coordinates,
            "turn-recovery".to_string(),
            "call-recovery".to_string(),
            "snapshot-recovery".to_string(),
            "recovery_tool".to_string(),
            prior_payload.args_fingerprint,
            true,
            Some(1),
            Some(0),
            None,
        )
        .await
        .unwrap();
        let current =
            append_recovery_request(store.as_ref(), &coordinates, arguments.clone()).await;
        let current_payload = serde_json::from_value::<
            crate::kernel::control_decision::ToolCallRequestedPayload,
        >(current.payload)
        .unwrap();
        let mut calls = vec![crate::adapters::agent_loop::WitnessedToolCall {
            tool_call: crate::adapters::agent_loop::ProviderToolCall {
                id: "call-recovery".to_string(),
                name: "recovery_tool".to_string(),
                arguments,
            },
            snapshot_id: "snapshot-recovery".to_string(),
            args_fingerprint: current_payload.args_fingerprint,
            request_event_id: current.id,
            holds: Vec::new(),
            recovery_action: crate::adapters::agent_loop::ToolRecoveryAction::Reexecute,
            recovery_source_event_id: None,
            recovery_fingerprint_mismatch: false,
        }];

        crate::adapters::agent_loop::apply_tool_recovery_actions(
            &services,
            &coordinates,
            "turn-recovery",
            &mut calls,
        )
        .await
        .unwrap();

        assert_eq!(calls[0].recovery_action, expected, "{label}");
        assert_eq!(calls[0].recovery_source_event_id, None, "{label}");
    }
}

#[tokio::test]
async fn fingerprintless_request_and_completion_do_not_match_current_fingerprint() {
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
        "tenant_a",
        "user_1",
        "legacy-tool-recovery",
    );
    append_recovery_bind_receipt(
        store.as_ref(),
        &coordinates,
        verlet_agent::manifest_schema::EffectClass::Idempotent,
    )
    .await;
    let prior = store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::ToolCallRequested,
                serde_json::json!({
                    "subject": {"turn_id":"turn-recovery", "call_id":"call-recovery"},
                    "snapshot_id": "snapshot-recovery",
                    "tool_name": "recovery_tool",
                    "arguments": {"input":"same"}
                }),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    services
        .append_agent_loop_session_entry(
            &coordinates,
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::tool_result(
                    "call-recovery",
                    "recovery_tool",
                    "legacy result",
                    false,
                ),
            },
            vec![prior.id],
        )
        .await
        .unwrap();
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::ToolCallCompleted,
                serde_json::json!({
                    "subject": {"turn_id":"turn-recovery", "call_id":"call-recovery"},
                    "snapshot_id": "snapshot-recovery",
                    "tool_name": "recovery_tool",
                    "success": true
                }),
            )],
        )
        .await
        .unwrap();
    let current = append_recovery_request(
        store.as_ref(),
        &coordinates,
        serde_json::json!({"input":"same"}),
    )
    .await;
    let current_payload = serde_json::from_value::<
        crate::kernel::control_decision::ToolCallRequestedPayload,
    >(current.payload)
    .unwrap();
    let mut calls = vec![crate::adapters::agent_loop::WitnessedToolCall {
        tool_call: crate::adapters::agent_loop::ProviderToolCall {
            id: "call-recovery".to_string(),
            name: "recovery_tool".to_string(),
            arguments: serde_json::json!({"input":"same"}),
        },
        snapshot_id: "snapshot-recovery".to_string(),
        args_fingerprint: current_payload.args_fingerprint,
        request_event_id: current.id,
        holds: Vec::new(),
        recovery_action: crate::adapters::agent_loop::ToolRecoveryAction::Reexecute,
        recovery_source_event_id: None,
        recovery_fingerprint_mismatch: false,
    }];

    crate::adapters::agent_loop::apply_tool_recovery_actions(
        &services,
        &coordinates,
        "turn-recovery",
        &mut calls,
    )
    .await
    .unwrap();

    assert_eq!(
        calls[0].recovery_action,
        crate::adapters::agent_loop::ToolRecoveryAction::Reexecute
    );
    assert_eq!(calls[0].recovery_source_event_id, None);
    assert!(calls[0].recovery_fingerprint_mismatch);
}

#[tokio::test]
async fn recovered_bash_rewrite_does_not_inherit_the_original_commands_lax_class() {
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
        "tenant_a",
        "user_1",
        "rewritten-bash-recovery",
    );
    let mut receipt = recovery_bind_receipt(verlet_agent::manifest_schema::EffectClass::Idempotent);
    receipt.operation_bindings[0].operations = vec!["safe".to_string()];
    receipt.operation_bindings[0].direct_tools.clear();
    append_current_bind_receipt(store.as_ref(), &coordinates, receipt).await;
    let arguments = serde_json::json!({"command":"safe input"});
    let request = |arguments: serde_json::Value| {
        let fingerprint =
            crate::agent::tool_universe::args_fingerprint(verlet_vbash::BASH_TOOL, &arguments)
                .unwrap();
        verlet_history::NewEventRecord::witnessed(
            coordinates.clone(),
            verlet_history::EventKind::ToolCallRequested,
            serde_json::to_value(crate::kernel::control_decision::ToolCallRequestedPayload {
                subject: crate::kernel::control_decision::ToolCallSubject {
                    turn_id: "turn-recovery".to_string(),
                    call_id: "call-recovery".to_string(),
                },
                snapshot_id: "snapshot-recovery".to_string(),
                tool_name: verlet_vbash::BASH_TOOL.to_string(),
                arguments,
                attach_event_id: None,
                args_fingerprint: Some(fingerprint),
                holds: Vec::new(),
            })
            .unwrap(),
        )
    };
    let prior = store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![request(arguments.clone())],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    store
        .append_events(
            &crate::kernel::control_decision::control_stream_id(&coordinates),
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ToolCallDecision,
                serde_json::to_value(crate::kernel::control_decision::ToolCallDecisionPayload {
                    subject: crate::kernel::control_decision::ToolCallSubject {
                        turn_id: "turn-recovery".to_string(),
                        call_id: "call-recovery".to_string(),
                    },
                    snapshot_id: "snapshot-recovery".to_string(),
                    outcome:
                        crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Rewrite {
                            arguments: serde_json::json!({"command":"destructive input"}),
                        },
                    admissible: None,
                })
                .unwrap(),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::for_thread(&coordinates)],
                    source_event_ids: vec![prior.id],
                    discharged_by: Some("test:rewrite".to_string()),
                    function: Some("rewrite/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    let current = store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![request(arguments.clone())],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let current_payload = serde_json::from_value::<
        crate::kernel::control_decision::ToolCallRequestedPayload,
    >(current.payload)
    .unwrap();
    let mut calls = vec![crate::adapters::agent_loop::WitnessedToolCall {
        tool_call: crate::adapters::agent_loop::ProviderToolCall {
            id: "call-recovery".to_string(),
            name: verlet_vbash::BASH_TOOL.to_string(),
            arguments,
        },
        snapshot_id: "snapshot-recovery".to_string(),
        args_fingerprint: current_payload.args_fingerprint,
        request_event_id: current.id,
        holds: Vec::new(),
        recovery_action: crate::adapters::agent_loop::ToolRecoveryAction::Reexecute,
        recovery_source_event_id: None,
        recovery_fingerprint_mismatch: false,
    }];

    crate::adapters::agent_loop::apply_tool_recovery_actions(
        &services,
        &coordinates,
        "turn-recovery",
        &mut calls,
    )
    .await
    .unwrap();

    assert_eq!(
        calls[0].recovery_action,
        crate::adapters::agent_loop::ToolRecoveryAction::ConservativeFailure
    );
}

#[tokio::test]
async fn turn_rerun_replays_witnessed_assistant_batch_without_redecode_mismatch() {
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
        "tenant_a",
        "user_1",
        "witnessed-turn-rerun",
    );
    append_recovery_bind_receipt(
        store.as_ref(),
        &coordinates,
        verlet_agent::manifest_schema::EffectClass::Idempotent,
    )
    .await;
    let input = crate::kernel::runtime_host::turn::TurnInput::text("recover the witnessed batch");
    let user_entry = services
        .append_user_turn_input(&coordinates, "turn-recovery", &input)
        .await
        .unwrap();
    let submitted = crate::adapters::agent_loop::append_turn_submitted_event(
        &services,
        &coordinates,
        "turn-recovery",
        &user_entry,
    )
    .await
    .unwrap();
    let arguments = serde_json::json!({"input":"same"});
    let assistant = verlet_history::CanonicalMessage::assistant(
        "openai",
        verlet_history::ProviderApi::OpenAIResponses,
        "gpt-test",
        vec![verlet_history::CanonicalContent::tool_call(
            "call-recovery",
            "recovery_tool",
            arguments.clone(),
        )],
        verlet_history::CanonicalStopReason::ToolUse,
    );
    let assistant_entry = services
        .append_agent_loop_session_entry(
            &coordinates,
            None,
            verlet_history::SessionEntryKind::Message { message: assistant },
            vec![submitted.id],
        )
        .await
        .unwrap();
    let turn_context = crate::kernel::runtime_host::turn::TurnContext::new(
        verlet_runtime_contracts::ThreadContext::root(coordinates.clone()),
        "turn-recovery",
        &input,
        tokio_util::sync::CancellationToken::new(),
    );
    crate::adapters::agent_loop::append_tool_call_requested_events(
        &services,
        &turn_context,
        &[(
            crate::adapters::agent_loop::ProviderToolCall {
                id: "call-recovery".to_string(),
                name: "recovery_tool".to_string(),
                arguments: arguments.clone(),
            },
            "snapshot-recovery".to_string(),
            None,
        )],
        assistant_entry.entry_id,
    )
    .await
    .unwrap();
    drop(services);

    let provider = std::sync::Arc::new(RecoveryCountingToolProvider::default());
    let kernel_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = provider.clone();
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(kernel_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
        "recovered final reply",
    )]));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                client.clone(),
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .load_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        coordinates.thread_id,
        "turn-recovery",
        "recover the witnessed batch",
    )
    .await
    .unwrap();
    assert_output(&mut events, "recovered final reply").await;

    assert_eq!(
        provider
            .invocations
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        client.requests().len(),
        1,
        "the model is called only after replay"
    );
    let requests = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.kind == verlet_history::EventKind::ToolCallRequested
                && event.payload["subject"]["call_id"] == "call-recovery"
        })
        .map(|event| {
            serde_json::from_value::<crate::kernel::control_decision::ToolCallRequestedPayload>(
                event.payload,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].arguments, requests[1].arguments);
    assert_eq!(requests[0].args_fingerprint, requests[1].args_fingerprint);
    assert!(
        thread
            .session_context()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|message| {
                matches!(
                    message,
                    verlet_history::CanonicalMessage::ToolResult {
                        tool_call_id,
                        is_error: false,
                        ..
                    } if tool_call_id == "call-recovery"
                )
            })
    );
    host.shutdown_all().await.unwrap();
}

#[derive(Default)]
struct AppendPause {
    entered: std::sync::atomic::AtomicBool,
    entered_notify: tokio::sync::Notify,
    released: std::sync::atomic::AtomicBool,
    release_notify: tokio::sync::Notify,
}

impl AppendPause {
    async fn arrive_and_wait(&self) {
        self.entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.entered_notify.notify_waiters();
        while !self.released.load(std::sync::atomic::Ordering::SeqCst) {
            self.release_notify.notified().await;
        }
    }

    async fn wait_until_entered(&self) {
        while !self.entered.load(std::sync::atomic::Ordering::SeqCst) {
            self.entered_notify.notified().await;
        }
    }

    fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

#[derive(Clone)]
struct PausingRuntimeStore {
    inner: verlet_history::InMemorySessionStore,
    pause_kind: verlet_history::EventKind,
    pause_once: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pause: std::sync::Arc<AppendPause>,
}

impl PausingRuntimeStore {
    fn after_first_append_of(pause_kind: verlet_history::EventKind) -> Self {
        Self {
            inner: verlet_history::InMemorySessionStore::new(),
            pause_kind,
            pause_once: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            pause: std::sync::Arc::new(AppendPause::default()),
        }
    }
}

#[async_trait::async_trait]
impl verlet_history::SessionStore for PausingRuntimeStore {
    async fn append(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        self.inner.append(coordinates, parent_entry_id, kind).await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
        provenance: verlet_history::EventProvenance,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        self.inner
            .append_with_provenance(coordinates, parent_entry_id, kind, provenance)
            .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: &str,
        kind: verlet_history::SessionEntryKind,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        self.inner
            .append_turn_input(coordinates, turn_id, kind)
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<Option<verlet_history::SessionEntryId>> {
        self.inner.active_leaf(coordinates).await
    }

    async fn select_branch(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        leaf_entry_id: Option<verlet_history::SessionEntryId>,
    ) -> verlet_history::HistoryResult<()> {
        self.inner.select_branch(coordinates, leaf_entry_id).await
    }

    async fn build_context(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<verlet_history::SessionContext> {
        self.inner.build_context(coordinates).await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        source_leaf: Option<verlet_history::SessionEntryId>,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<Option<verlet_history::SessionEntryId>> {
        self.inner
            .clone_branch(source_coordinates, source_leaf, target_coordinates)
            .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        base: verlet_history::ThreadBaseRef,
    ) -> verlet_history::HistoryResult<()> {
        self.inner
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
    }
}

#[async_trait::async_trait]
impl verlet_history::EventStore for PausingRuntimeStore {
    async fn append_events(
        &self,
        stream_id: &verlet_history::EventStreamId,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let should_pause = records.iter().any(|record| record.kind == self.pause_kind)
            && self
                .pause_once
                .swap(false, std::sync::atomic::Ordering::SeqCst);
        let appended = self.inner.append_events(stream_id, records).await?;
        if should_pause {
            self.pause.arrive_and_wait().await;
        }
        Ok(appended)
    }

    async fn append_events_fenced(
        &self,
        stream_id: &verlet_history::EventStreamId,
        expected_next_sequence: verlet_history::EventSequence,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let should_pause = records.iter().any(|record| record.kind == self.pause_kind)
            && self
                .pause_once
                .swap(false, std::sync::atomic::Ordering::SeqCst);
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
        stream_id: &verlet_history::EventStreamId,
        from_sequence: Option<verlet_history::EventSequence>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        self.inner.read_events(stream_id, from_sequence).await
    }
}

#[async_trait::async_trait]
// lexicon-allow: observation_store - deterministic test store implements the existing history trait.
impl verlet_history::ObservationStore for PausingRuntimeStore {
    async fn append_observation(
        &self,
        record: verlet_history::NewObservationRecord,
    ) -> verlet_history::HistoryResult<verlet_history::ObservationRecord> {
        self.inner.append_observation(record).await
    }

    async fn list_observations(
        &self,
        scope: &verlet_runtime_contracts::ThreadCoordinates,
        kind: Option<&str>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::ObservationRecord>> {
        self.inner.list_observations(scope, kind).await
    }
}

struct StaticThreadSpawnAgentResolver;

const CHILD_AGENT_REF: &str = "agent://worker@latest";
const CHILD_MANIFEST_HASH: &str = "sha256:child-manifest";

#[async_trait::async_trait]
impl crate::agent::agent_process::KernelThreadSpawnAgentResolver
    for StaticThreadSpawnAgentResolver
{
    fn default_agent_ref(
        &self,
        _caller: &verlet_runtime_contracts::ThreadContext,
    ) -> Option<String> {
        Some(CHILD_AGENT_REF.to_string())
    }

    async fn resolve_agent_ref(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        agent_ref: &str,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::agent::agent_process::KernelThreadSpawnAgentBinding,
    > {
        assert_eq!(agent_ref, CHILD_AGENT_REF);
        Ok(crate::agent::agent_process::KernelThreadSpawnAgentBinding {
            metadata: std::collections::BTreeMap::from([(
                "cooldis.agent.manifest_hash".to_string(),
                CHILD_MANIFEST_HASH.to_string(),
            )]),
            compile_receipt: serde_json::json!({
                "ref_uri": CHILD_AGENT_REF,
                "manifest_hash": CHILD_MANIFEST_HASH,
                "source_hash": "sha256:child-source"
            }),
            bind_receipt: serde_json::to_value(
                crate::agent::manifest_bind::AgentManifestBindReceipt {
                    ref_uri: CHILD_AGENT_REF.to_string(),
                    manifest_hash: CHILD_MANIFEST_HASH.to_string(),
                    model_profile_id: "default".to_string(),
                    model_profile_origin: None,
                    provider_id: "test".to_string(),
                    model_id: "model".to_string(),
                    tool_ids: Vec::new(),
                    operation_bindings: Vec::new(),
                    tool_universes: Vec::new(),
                    couplings: Vec::new(),
                    skill_packages: Vec::new(),
                    skill_discovery: None,
                    static_context_segments: Vec::new(),
                    effective_runtime:
                        verlet_agent::manifest_schema::AgentManifestRuntimeDefaults::default(),
                    overridden_keys: Vec::new(),
                    placement: None,
                    placement_origin: None,
                    workspace: None,
                    workspace_origin: None,
                },
            )
            .unwrap(),
            principal_id: caller.coordinates.user_id.clone(),
        })
    }
}

struct StaticHookHandler {
    spec: crate::agent::hooks::HookHandlerSpec,
    output: crate::agent::hooks::HookHandlerOutput,
    requests: std::sync::Mutex<Vec<crate::agent::hooks::HookRequest>>,
}

impl StaticHookHandler {
    fn new(
        id: impl Into<String>,
        event_name: crate::agent::hooks::HookEventName,
        matcher: Option<&str>,
        output: crate::agent::hooks::HookHandlerOutput,
    ) -> Self {
        Self {
            spec: crate::agent::hooks::HookHandlerSpec {
                id: id.into(),
                event_name,
                matcher: matcher.map(str::to_string),
            },
            output,
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<crate::agent::hooks::HookRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl TurnContextRecordingKernelToolProvider {
    fn new() -> Self {
        Self {
            snapshots: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn snapshots(&self) -> Vec<Option<crate::kernel::runtime_host::turn::TurnContextSnapshot>> {
        self.snapshots.lock().unwrap().clone()
    }
}

impl WitnessCheckingEchoProvider {
    fn seen_arguments(&self) -> Vec<serde_json::Value> {
        self.seen_arguments.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::agent::hooks::HookHandler for StaticHookHandler {
    fn spec(&self) -> crate::agent::hooks::HookHandlerSpec {
        self.spec.clone()
    }

    async fn run(
        &self,
        request: crate::agent::hooks::HookRequest,
    ) -> crate::kernel::runtime_host::VerletResult<crate::agent::hooks::HookHandlerOutput> {
        self.requests.lock().unwrap().push(request);
        Ok(self.output.clone())
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for WitnessCheckingEchoProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "echo_search",
            "Echo input after checking hook witnesses.",
            serde_json::json!({"type":"object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
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
                verlet_agent::contracts::sha256_hex(
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
                verlet_agent::contracts::sha256_hex(
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
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
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
            requests: std::sync::Mutex::new(Vec::new()),
            responses: std::sync::Mutex::new(responses.into()),
        }
    }

    fn requests(&self) -> Vec<verlet_provider::ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl StreamingClient {
    fn new(events: Vec<Vec<verlet_provider::ProviderStreamEvent>>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            events: std::sync::Mutex::new(events.into()),
        }
    }

    fn requests(&self) -> Vec<verlet_provider::ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for ScriptedClient {
    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let response = self.responses.lock().unwrap().pop_front().ok_or_else(|| {
            verlet_provider::ProviderError::Decode("no test response queued".to_string())
        })?;
        match response {
            ScriptedResponse::Pending => std::future::pending().await,
            ScriptedResponse::Error(error) => Err(error),
            ScriptedResponse::Response(response) => Ok(response),
        }
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for RecordingClient {
    fn capabilities(&self) -> Option<verlet_provider::ProviderCapabilityRecord> {
        self.capabilities.clone()
    }

    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        self.responses.lock().unwrap().pop().ok_or_else(|| {
            verlet_provider::ProviderError::Decode("no test response queued".to_string())
        })
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for GatedRecordingClient {
    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        let request_index = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            requests.len() - 1
        };
        if request_index == 0 {
            self.first_request_started.notify_one();
            self.release_first_request.notified().await;
        }
        self.responses.lock().unwrap().pop_front().ok_or_else(|| {
            verlet_provider::ProviderError::Decode("no test response queued".to_string())
        })
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for BashMandateListClient {
    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        if request
            .messages
            .iter()
            .any(|message| matches!(message, verlet_history::CanonicalMessage::ToolResult { .. }))
        {
            Ok(response_text("listed mandates"))
        } else {
            self.barrier.wait().await;
            Ok(response_tool_call_named(
                verlet_vbash::BASH_TOOL,
                serde_json::json!({
                    "command": format!(
                        "printf '{{}}' | verlet run {} {}",
                        crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE,
                        crate::operations::kernel_packages::MANDATE_LIST_OPERATION,
                    ),
                }),
            ))
        }
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for StreamingClient {
    async fn complete(
        &self,
        _request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        Err(verlet_provider::ProviderError::Decode(
            "streaming test client requires stream()".to_string(),
        ))
    }

    async fn stream(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<Vec<verlet_provider::ProviderStreamEvent>> {
        self.requests.lock().unwrap().push(request.clone());
        self.events.lock().unwrap().pop_front().ok_or_else(|| {
            verlet_provider::ProviderError::Decode("no test stream queued".to_string())
        })
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider
    for TurnContextRecordingKernelToolProvider
{
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "record_turn_context",
            "Record the current Verlet turn context.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        self.snapshots
            .lock()
            .unwrap()
            .push(call.turn_context.clone());
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "turn context recorded",
            false,
        )))
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for FinishSecondFirstToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "thread_submit",
            "Deterministic hold-scheduler test tool.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        match call.arguments["slot"].as_str() {
            Some("first") => self.second_finished.notified().await,
            Some("second") => self.second_finished.notify_one(),
            other => panic!("unexpected finish-order slot: {other:?}"),
        }
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            call.arguments["slot"].as_str().unwrap(),
            false,
        )))
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for SerialBlockingToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            self.tool_name,
            "Deterministic serialization test tool.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        let slot = call.arguments["slot"].as_str().unwrap().to_string();
        self.started.send(slot.clone()).unwrap();
        if slot == "first" {
            self.release_first.notified().await;
        }
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            slot,
            false,
        )))
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider
    for CancellationAcknowledgingThreadToolProvider
{
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "thread_submit",
            "Cancellation-aware interruption test tool.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        _call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        panic!("the interruption test must use the cancellable provider surface")
    }

    async fn invoke_tool_call_cancellable(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
        cancellation: crate::agent::agent_tool_router::ToolInvocationCancellation,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::agent::agent_tool_router::AgentKernelToolOutcome,
    > {
        self.started.send(call.call_id.clone()).unwrap();
        cancellation.token().cancelled().await;
        self.acknowledged.send(call.call_id.clone()).unwrap();
        Ok(
            crate::agent::agent_tool_router::AgentKernelToolOutcome::Completed(Some(
                verlet_history::CanonicalMessage::tool_result(
                    call.call_id,
                    call.tool_name,
                    "interrupt acknowledged",
                    true,
                ),
            )),
        )
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for NonObservingThreadToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        ["thread_status", "thread_wait"]
            .into_iter()
            .map(|name| {
                verlet_provider::ToolDefinition::new(
                    name,
                    "Default-implementation interruption test tool.",
                    serde_json::json!({"type": "object"}),
                )
            })
            .collect()
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        if call.tool_name == "thread_wait" {
            self.never_launched
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        self.started.send(call.call_id.clone()).unwrap();
        while !self.released.load(std::sync::atomic::Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "finished without observing cancellation",
            false,
        )))
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for PanickingAfterGraceToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "thread_status",
            "Panics after the cancellation monitor abandons it.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        _call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        self.started.send(()).unwrap();
        self.release.notified().await;
        panic!("panic after grace")
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for ImmediateThreadToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        ["thread_submit", "thread_status"]
            .into_iter()
            .map(|name| {
                verlet_provider::ToolDefinition::new(
                    name,
                    "Immediate suspension-batch test tool.",
                    serde_json::json!({"type": "object"}),
                )
            })
            .collect()
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name.clone(),
            format!("{} completed", call.tool_name),
            false,
        )))
    }
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for IsolatedFailureToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "thread_submit",
            "Per-call failure isolation test tool.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        if call.arguments["fail"].as_bool() == Some(true) {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                "expected call failure".to_string(),
            ));
        }
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "sibling completed",
            false,
        )))
    }
}

fn response_text(text: &str) -> verlet_provider::ProviderResponse {
    verlet_provider::ProviderResponse {
        content: vec![verlet_history::CanonicalContent::text(text)],
        usage: verlet_history::CanonicalUsage {
            input_tokens: 1,
            output_tokens: 2,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        },
        stop_reason: verlet_history::CanonicalStopReason::EndTurn,
    }
}

fn response_tool_call() -> verlet_provider::ProviderResponse {
    response_tool_call_named("bash", serde_json::json!({"command":"pwd"}))
}

fn response_tool_call_named(
    name: &str,
    arguments: serde_json::Value,
) -> verlet_provider::ProviderResponse {
    response_tool_call_named_with_id("call_1|fc_1", name, arguments)
}

fn response_tool_call_named_with_id(
    call_id: &str,
    name: &str,
    arguments: serde_json::Value,
) -> verlet_provider::ProviderResponse {
    verlet_provider::ProviderResponse {
        content: vec![verlet_history::CanonicalContent::tool_call(
            call_id, name, arguments,
        )],
        usage: verlet_history::CanonicalUsage::default(),
        stop_reason: verlet_history::CanonicalStopReason::ToolUse,
    }
}

fn response_tool_calls(
    calls: Vec<(&str, &str, serde_json::Value)>,
) -> verlet_provider::ProviderResponse {
    verlet_provider::ProviderResponse {
        content: calls
            .into_iter()
            .map(|(call_id, name, arguments)| {
                verlet_history::CanonicalContent::tool_call(call_id, name, arguments)
            })
            .collect(),
        usage: verlet_history::CanonicalUsage::default(),
        stop_reason: verlet_history::CanonicalStopReason::ToolUse,
    }
}

fn tool_round_responses(rounds: usize) -> Vec<verlet_provider::ProviderResponse> {
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

fn runtime_factory(
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
) -> std::sync::Arc<crate::adapters::agent_loop::AgentLoopFactory> {
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    std::sync::Arc::new(crate::adapters::agent_loop::AgentLoopFactory::new(
        config, client,
    ))
}

fn runtime_factory_with_registry(
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    registry: std::sync::Arc<verlet_operations::operation_registry::OperationRegistry>,
) -> std::sync::Arc<crate::adapters::agent_loop::AgentLoopFactory> {
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, client)
            .with_operation_registry(registry),
    )
}

fn streaming_runtime_factory(
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
) -> std::sync::Arc<crate::adapters::agent_loop::AgentLoopFactory> {
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    config.stream = true;
    std::sync::Arc::new(crate::adapters::agent_loop::AgentLoopFactory::new(
        config, client,
    ))
}

fn factory(
    client: std::sync::Arc<RecordingClient>,
) -> std::sync::Arc<crate::adapters::agent_loop::AgentLoopFactory> {
    runtime_factory(client)
}

struct RootProviderChildEchoFactory {
    root: std::sync::Arc<crate::adapters::agent_loop::AgentLoopFactory>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory
    for RootProviderChildEchoFactory
{
    async fn build(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<
        Box<dyn crate::kernel::runtime_host::runtime_api::AgentRuntime>,
    > {
        if context.parent_thread_id.is_some() {
            return Ok(Box::new(ChildEchoRuntime));
        }
        self.root.build(context).await
    }
}

struct ChildEchoRuntime;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntime for ChildEchoRuntime {
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
        let thread_id = context.coordinates.thread_id;
        let thread_context = context.clone();
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
        let _ =
            events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Started { context });
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                    let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Running);
                            let _ = services.append_user_turn_input(&coordinates, &turn_id, &input).await;
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Output {
                                thread_id,
                                text: format!("child:{}", input.text_projection()),
                            });
                            if let Ok(completed) = services
                                .append_thread_event(
                                    &coordinates,
                                    verlet_history::NewEventRecord::discharged(
                                        coordinates.clone(),
                                        verlet_history::EventKind::TurnCompleted,
                                        serde_json::json!({
                                            "turn_id": turn_id,
                                        }),
                                        verlet_history::EventProvenance {
                                            source_streams: vec![verlet_history::EventStreamId::for_thread(&coordinates)],
                                            discharged_by: Some("runtime:child-echo".to_string()),
                                            function: Some("turn_complete/v1".to_string()),
                                            ..verlet_history::EventProvenance::default()
                                        },
                                    ),
                                )
                                .await
                            {
                                let _ = services
                                    .append_thread_joined_event_if_spawned(
                                        &thread_context,
                                        verlet_history::ThreadTerminalState::Completed,
                                        None,
                                        Some(completed.id),
                                    )
                                    .await;
                            }
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Cancel { reason }) => {
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::CancelTurn { .. }) => {}
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Compact { .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown) | None => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

fn text_messages(messages: &[verlet_history::CanonicalMessage]) -> Vec<String> {
    messages
        .iter()
        .map(|message| match message {
            verlet_history::CanonicalMessage::User { content, .. }
            | verlet_history::CanonicalMessage::Assistant { content, .. }
            | verlet_history::CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    verlet_history::CanonicalContent::Text { text, .. } => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default(),
        })
        .collect()
}

fn text_from_content(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

#[tokio::test]
async fn runtime_builds_each_turn_from_canonical_session_history() {
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_text("first reply"),
        response_text("second reply"),
    ]));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory(std::sync::Arc::clone(&client)),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
async fn turn_endpoint_router_snapshots_wire_and_record_coordinates_per_turn() {
    let launch_client = std::sync::Arc::new(RecordingClient::default());
    let first_client = std::sync::Arc::new(GatedRecordingClient::new(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input":"routed"})),
        response_text("first routed reply"),
    ]));
    let second_client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
        "second routed reply",
    )]));
    let first_endpoint = crate::adapters::agent_loop::ResolvedTurnEndpoint {
        config: crate::adapters::agent_loop::AgentLoopConfig::new(
            verlet_history::ProviderApi::OpenAIResponses,
            "provider-a",
            "model-a",
        ),
        client: first_client.clone(),
    };
    let second_endpoint = crate::adapters::agent_loop::ResolvedTurnEndpoint {
        config: crate::adapters::agent_loop::AgentLoopConfig::new(
            verlet_history::ProviderApi::AnthropicMessages,
            "provider-b",
            "model-b",
        ),
        client: second_client.clone(),
    };
    let router = std::sync::Arc::new(MutableTurnEndpointRouter::new(first_endpoint));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
                    "launch-provider",
                    "launch-model",
                ),
                launch_client.clone(),
            )
            .with_operation_registry(echo_registry("echo").await)
            .with_turn_endpoint_router(router.clone()),
        ),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "session_routed",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit_turn(
        thread.context().coordinates.thread_id,
        "turn-1",
        crate::kernel::runtime_host::turn::TurnInput::text("first")
            .with_provider("ignored-turn-provider")
            .with_model("ignored-turn-model"),
    )
    .await
    .unwrap();
    tokio::time::timeout(
        tokio::time::Duration::from_secs(30),
        first_client.first_request_started.notified(),
    )
    .await
    .expect("first routed request did not start");
    router.set(second_endpoint);
    first_client.release_first_request.notify_one();
    assert_output(&mut events, "first routed reply").await;

    host.submit(thread.context().coordinates.thread_id, "turn-2", "second")
        .await
        .unwrap();
    assert_output(&mut events, "second routed reply").await;

    let first_requests = first_client.requests();
    assert_eq!(first_requests.len(), 2);
    assert!(first_requests.iter().all(|request| {
        request.provider == "provider-a"
            && request.model == "model-a"
            && request.api == verlet_history::ProviderApi::OpenAIResponses
    }));
    let second_requests = second_client.requests();
    assert_eq!(second_requests.len(), 1);
    assert_eq!(second_requests[0].provider, "provider-b");
    assert_eq!(second_requests[0].model, "model-b");
    assert_eq!(
        second_requests[0].api,
        verlet_history::ProviderApi::AnthropicMessages
    );
    assert!(launch_client.requests().is_empty());

    let session = thread.session_context().await.unwrap();
    let assistant_coordinates = session
        .messages
        .iter()
        .filter_map(|message| match message {
            verlet_history::CanonicalMessage::Assistant {
                api,
                provider,
                model,
                ..
            } => Some((api.clone(), provider.clone(), model.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        assistant_coordinates,
        vec![
            (
                verlet_history::ProviderApi::OpenAIResponses,
                "provider-a".to_string(),
                "model-a".to_string(),
            ),
            (
                verlet_history::ProviderApi::OpenAIResponses,
                "provider-a".to_string(),
                "model-a".to_string(),
            ),
            (
                verlet_history::ProviderApi::AnthropicMessages,
                "provider-b".to_string(),
                "model-b".to_string(),
            ),
        ]
    );
}

#[tokio::test]
async fn turn_endpoint_router_uses_selected_stream_mode_and_token_limit() {
    let launch_client = std::sync::Arc::new(RecordingClient::default());
    let selected_client = std::sync::Arc::new(StreamingClient::new(vec![vec![
        verlet_provider::ProviderStreamEvent::TextDelta {
            text: "selected stream reply".to_string(),
        },
        verlet_provider::ProviderStreamEvent::Done {
            stop_reason: verlet_history::CanonicalStopReason::EndTurn,
        },
    ]]));
    let mut selected_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIChatCompletions,
        "selected-provider",
        "selected-model",
    );
    selected_config.max_tokens = 321;
    selected_config.stream = true;
    let router = std::sync::Arc::new(MutableTurnEndpointRouter::new(
        crate::adapters::agent_loop::ResolvedTurnEndpoint {
            config: selected_config,
            client: selected_client.clone(),
        },
    ));
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(
            crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::OpenAIResponses,
                "launch-provider",
                "launch-model",
            ),
            launch_client.clone(),
        )
        .with_turn_endpoint_router(router),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "session_routed_stream",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "use selected stream",
    )
    .await
    .unwrap();
    assert_output(&mut events, "selected stream reply").await;

    let requests = selected_client.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].max_tokens, 321);
    assert!(launch_client.requests().is_empty());
}

#[tokio::test]
async fn runtime_applies_provider_context_policy_before_request() {
    let mut capabilities = verlet_provider::ProviderCapabilityRecord::for_api(
        verlet_history::ProviderApi::OpenAIResponses,
    );
    capabilities.context_policy = verlet_provider::ProviderContextPolicy {
        max_messages: Some(2),
        max_text_bytes: Some(5),
    };
    let client = std::sync::Arc::new(
        RecordingClient::with_responses(vec![
            response_text("first reply"),
            response_text("second reply"),
        ])
        .with_capabilities(capabilities),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory(std::sync::Arc::clone(&client)),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_text("first reply"),
        response_text("second reply"),
    ]));
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let factory = std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, client.clone())
            .with_context_compile_policy(
                crate::kernel::context_compiler::AgentContextCompilePolicy {
                    max_messages: Some(1),
                    max_text_bytes: None,
                },
            ),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory,
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
        "memory-aware reply",
    )]));
    let config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    let factory = std::sync::Arc::new(crate::adapters::agent_loop::AgentLoopFactory::new(
        config,
        client.clone(),
    ));
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(factory, store.clone());
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let coordinates = &thread.context().coordinates;
    let thread_stream = verlet_history::EventStreamId::for_thread(coordinates);
    let memory_stream =
        verlet_history::EventStreamId::new(format!("derived:memory:{}", coordinates.thread_id));
    let memory = store
        .append_events(
            &memory_stream,
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ContextSummaryCompleted,
                serde_json::json!({
                    "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
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
                verlet_history::EventProvenance {
                    source_streams: vec![thread_stream],
                    discharged_by: Some("coupling:std::memory.extract".to_string()),
                    function: Some("op://std-memory-extract/run@sha256:test".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    let derived_context_stream =
        verlet_history::EventStreamId::new(format!("derived:context:{}", coordinates.thread_id));
    store
        .append_events(
            &derived_context_stream,
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ContextReadPlanSet,
                serde_json::json!({
                    "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
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
                verlet_history::EventProvenance {
                    source_streams: vec![memory_stream],
                    source_event_ids: vec![memory[0].id],
                    discharged_by: Some("coupling:std::memory.recall".to_string()),
                    function: Some("op://std-memory-recall/run@sha256:test".to_string()),
                    ..verlet_history::EventProvenance::default()
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
        "instruction-aware reply",
    )]));
    let config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    let factory = std::sync::Arc::new(crate::adapters::agent_loop::AgentLoopFactory::new(
        config,
        client.clone(),
    ));
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(factory, store.clone());
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let coordinates = &thread.context().coordinates;
    let thread_stream = verlet_history::EventStreamId::for_thread(coordinates);
    let derived_context_stream =
        verlet_history::EventStreamId::new(format!("derived:context:{}", coordinates.thread_id));
    let instruction = store
        .append_events(
            &derived_context_stream,
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ContextSummaryCompleted,
                serde_json::json!({
                    "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
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
                verlet_history::EventProvenance {
                    source_streams: vec![thread_stream],
                    discharged_by: Some("coupling:std::prompt.dynamic_instructions".to_string()),
                    function: Some(
                        "op://std-prompt-dynamic-instructions/run@sha256:test".to_string(),
                    ),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    store
        .append_events(
            &derived_context_stream,
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ContextReadPlanSet,
                serde_json::json!({
                    "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
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
                verlet_history::EventProvenance {
                    source_streams: vec![derived_context_stream.clone()],
                    source_event_ids: vec![instruction[0].id],
                    discharged_by: Some("coupling:std::prompt.dynamic_instructions".to_string()),
                    function: Some(
                        "op://std-prompt-dynamic-instructions/run@sha256:test".to_string(),
                    ),
                    ..verlet_history::EventProvenance::default()
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
    let mut capabilities = verlet_provider::ProviderCapabilityRecord::for_api(
        verlet_history::ProviderApi::OpenAIResponses,
    );
    capabilities.context_policy = verlet_provider::ProviderContextPolicy {
        max_messages: Some(1),
        max_text_bytes: Some(4),
    };
    let client = std::sync::Arc::new(
        RecordingClient::with_responses(vec![
            response_text("first reply"),
            response_text("second reply"),
        ])
        .with_capabilities(capabilities),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory(std::sync::Arc::clone(&client)),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ContextCompiled {
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestStarted {
                turn_id,
                provider,
                api,
                model,
                mode: verlet_runtime_contracts::RuntimeModelRequestMode::Complete,
                purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose::Turn,
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestCompleted {
                turn_id,
                usage,
                stop_reason: verlet_history::CanonicalStopReason::EndTurn,
                ..
            } if turn_id == "turn-2"
                && usage.input_tokens == 1
                && usage.output_tokens == 2
        )
    }));
}

#[tokio::test]
async fn runtime_emits_model_request_failed_on_provider_error() {
    let client = std::sync::Arc::new(RecordingClient::with_responses(Vec::new()));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory(std::sync::Arc::clone(&client)),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestStarted {
                turn_id,
                purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose::Turn,
                ..
            } if turn_id == "turn-1"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFailed {
                turn_id,
                error,
                ..
            } if turn_id == "turn-1" && error.contains("no test response queued")
        )
    }));
}

#[tokio::test]
async fn terminal_provider_http_error_journals_one_body_free_turn_failed() {
    let client = std::sync::Arc::new(ScriptedClient::new(vec![ScriptedResponse::Error(
        verlet_provider::ProviderError::HttpStatus {
            status: reqwest::StatusCode::BAD_REQUEST,
            body: "raw-response-body-must-not-be-journaled".to_string(),
        },
    )]));
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory(client));
    let store = host.runtime_store();
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    assert_failed_with_runtime_events(&mut events, "raw-response-body-must-not-be-journaled").await;

    let failed = turn_failed_events(store.as_ref(), &thread.context().coordinates).await;
    assert_eq!(failed.len(), 1);
    let payload: verlet_history::TurnFailedPayload =
        serde_json::from_value(failed[0].payload.clone()).unwrap();
    assert_eq!(payload.turn_id, "turn-1");
    assert_eq!(
        payload.error_class,
        verlet_history::TurnFailureErrorClass::ProviderHttp
    );
    assert_eq!(payload.provider_id.as_deref(), Some("openai"));
    assert_eq!(payload.http_status, Some(400));
    assert_eq!(payload.message, "provider HTTP status 400");
    assert_eq!(payload.retries_attempted, 0);
    assert!(
        !serde_json::to_string(&failed[0].payload)
            .unwrap()
            .contains("raw-response-body-must-not-be-journaled")
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn model_request_retries_retryable_provider_error() {
    let inner = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
        "retry reply",
    )]));
    let client = std::sync::Arc::new(
        crate::support::fault::FaultingProviderClient::new(inner.clone())
            .fail_nth_http("complete", 1, "temporary outage")
            .delay_nth("complete", 2, tokio::time::Duration::from_millis(25)),
    );
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let factory = std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, client.clone())
            .with_model_request_retry_policy(
                crate::adapters::agent_loop::ModelRequestRetryPolicy::fixed(2, 50),
            ),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory,
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let store = host.runtime_store();
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    let runtime_events = assert_output_with_runtime_events(&mut events, "retry reply").await;
    assert_completed_terminal(&mut events).await;

    assert_eq!(client.call_count("complete"), 2);
    assert_eq!(inner.requests().len(), 1);
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFailed {
                error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable,
                error,
                ..
            } if error.contains("temporary outage")
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestRetryScheduled {
                attempt: 1,
                next_attempt: 2,
                delay_ms: 50,
                error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable,
                ..
            }
        )
    }));
    assert!(
        turn_failed_events(store.as_ref(), &thread.context().coordinates)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn retry_exhaustion_journals_exactly_one_turn_failed() {
    let client = std::sync::Arc::new(ScriptedClient::new(vec![
        ScriptedResponse::Error(verlet_provider::ProviderError::Http("outage-1".to_string())),
        ScriptedResponse::Error(verlet_provider::ProviderError::Http("outage-2".to_string())),
        ScriptedResponse::Error(verlet_provider::ProviderError::Http("outage-3".to_string())),
    ]));
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let factory = std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, client.clone())
            .with_model_request_retry_policy(
                crate::adapters::agent_loop::ModelRequestRetryPolicy::fixed(3, 0),
            ),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory);
    let store = host.runtime_store();
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    assert_failed_with_runtime_events(&mut events, "outage-3").await;

    assert_eq!(client.requests().len(), 3);
    let failed = turn_failed_events(store.as_ref(), &thread.context().coordinates).await;
    assert_eq!(failed.len(), 1);
    let payload: verlet_history::TurnFailedPayload =
        serde_json::from_value(failed[0].payload.clone()).unwrap();
    assert_eq!(
        payload.error_class,
        verlet_history::TurnFailureErrorClass::ProviderTransport
    );
    assert_eq!(payload.provider_id.as_deref(), Some("openai"));
    assert_eq!(payload.http_status, None);
    assert!(payload.message.contains("outage-3"));
    assert_eq!(payload.retries_attempted, 2);
}

#[tokio::test]
async fn terminal_provider_message_is_journaled_at_most_1024_bytes() {
    let client = std::sync::Arc::new(ScriptedClient::new(vec![ScriptedResponse::Error(
        verlet_provider::ProviderError::Http("x".repeat(2_048)),
    )]));
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory(client));
    let store = host.runtime_store();
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    assert_failed_with_runtime_events(&mut events, "provider HTTP request failed").await;

    let failed = turn_failed_events(store.as_ref(), &thread.context().coordinates).await;
    assert_eq!(failed.len(), 1);
    let payload: verlet_history::TurnFailedPayload =
        serde_json::from_value(failed[0].payload.clone()).unwrap();
    assert_eq!(payload.message.as_bytes().len(), 1024);
    assert!(payload.message.is_char_boundary(payload.message.len()));
    assert!(
        payload
            .message
            .starts_with("provider HTTP request failed: ")
    );
}

#[tokio::test]
async fn model_request_does_not_retry_fatal_provider_error() {
    let client = std::sync::Arc::new(ScriptedClient::new(vec![
        ScriptedResponse::Error(verlet_provider::ProviderError::Decode(
            "bad json".to_string(),
        )),
        ScriptedResponse::Response(response_text("unused reply")),
    ]));
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let factory = std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, client.clone())
            .with_model_request_retry_policy(
                crate::adapters::agent_loop::ModelRequestRetryPolicy::fixed(2, 0),
            ),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory,
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFailed {
                error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::Fatal,
                error,
                ..
            } if error.contains("bad json")
        )
    }));
    assert!(!runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestRetryScheduled { .. }
        )
    }));
}

#[tokio::test]
async fn model_request_falls_back_after_retry_exhaustion() {
    let primary_client = std::sync::Arc::new(ScriptedClient::new(vec![ScriptedResponse::Error(
        verlet_provider::ProviderError::HttpStatus {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            body: "provider down".to_string(),
        },
    )]));
    let fallback_client =
        std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
            "fallback reply",
        )]));
    let mut primary_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    primary_config.max_tokens = 128;
    let fallback_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "fallback",
        "gpt-fallback",
    );
    let factory = std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(primary_config, primary_client.clone())
            .with_model_request_fallback(fallback_config, fallback_client.clone()),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory,
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFallbackSelected {
                from_provider,
                from_model,
                to_provider,
                to_model,
                error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable,
                ..
            } if from_provider == "openai"
                && from_model == "gpt-test"
                && to_provider == "fallback"
                && to_model == "gpt-fallback"
        )
    }));
    assert!(matches!(
        assistant,
        verlet_history::CanonicalMessage::Assistant {
            provider,
            api: verlet_history::ProviderApi::OpenAIResponses,
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
    let client = std::sync::Arc::new(StreamingClient::new(vec![vec![
        verlet_provider::ProviderStreamEvent::TextDelta {
            text: "partial".to_string(),
        },
    ]]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(streaming_runtime_factory(provider_client));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFailed {
                error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::StreamAssembly,
                error,
                ..
            } if error.contains("provider stream ended before done event")
        )
    }));
}

#[test]
fn stream_assembly_merges_item_id_deltas_into_combined_completed_tool_call() {
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let (events, mut runtime_events) = tokio::sync::broadcast::channel(8);

    let response = super::response_from_stream_events(
        &coordinates,
        vec![
            verlet_provider::ProviderStreamEvent::ToolCallDelta {
                id: "fc_1".to_string(),
                name: None,
                arguments_delta: "{\"path\":\"notes.txt\",".to_string(),
            },
            verlet_provider::ProviderStreamEvent::ToolCallDelta {
                id: "fc_1".to_string(),
                name: None,
                arguments_delta: "\"content\":\"hello\"}".to_string(),
            },
            verlet_provider::ProviderStreamEvent::Content {
                content: verlet_history::CanonicalContent::tool_call(
                    "call_1|fc_1",
                    "write",
                    serde_json::json!({"path": "notes.txt", "content": "hello"}),
                ),
            },
            verlet_provider::ProviderStreamEvent::Done {
                stop_reason: verlet_history::CanonicalStopReason::ToolUse,
            },
        ],
        &events,
    )
    .unwrap();

    assert_eq!(
        response.content,
        vec![verlet_history::CanonicalContent::tool_call(
            "call_1|fc_1",
            "write",
            serde_json::json!({"path": "notes.txt", "content": "hello"}),
        )]
    );
    let started = std::iter::from_fn(|| runtime_events.try_recv().ok())
        .filter_map(|event| {
            match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                match event.kind {
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
                        call_id,
                        name,
                        ..
                    } => Some((call_id, name)),
                    _ => None,
                }
            }
            _ => None,
        }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        started,
        vec![("call_1|fc_1".to_string(), "write".to_string())]
    );
}

#[test]
fn stream_assembly_rejects_nameless_pending_tool_call() {
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let (events, _runtime_events) = tokio::sync::broadcast::channel(8);

    let error = super::response_from_stream_events(
        &coordinates,
        vec![
            verlet_provider::ProviderStreamEvent::ToolCallDelta {
                id: "fc_1".to_string(),
                name: None,
                arguments_delta: "{\"path\":\"notes.txt\"}".to_string(),
            },
            verlet_provider::ProviderStreamEvent::Done {
                stop_reason: verlet_history::CanonicalStopReason::ToolUse,
            },
        ],
        &events,
    )
    .unwrap_err();

    assert_eq!(
        error.class,
        verlet_runtime_contracts::RuntimeModelRequestErrorClass::StreamAssembly
    );
    assert_eq!(error.message, "streamed tool call fc_1 is missing a name");
}

#[test]
fn stream_assembly_ignores_item_id_deltas_after_completed_tool_call() {
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let (events, mut runtime_events) = tokio::sync::broadcast::channel(8);

    let completed = verlet_history::CanonicalContent::tool_call(
        "call_1|fc_1",
        "write",
        serde_json::json!({"path": "notes.txt"}),
    );
    let response = super::response_from_stream_events(
        &coordinates,
        vec![
            verlet_provider::ProviderStreamEvent::Content {
                content: completed.clone(),
            },
            verlet_provider::ProviderStreamEvent::ToolCallDelta {
                id: "fc_1".to_string(),
                name: None,
                arguments_delta: "{\"path\":\"notes.txt\"}".to_string(),
            },
            verlet_provider::ProviderStreamEvent::Done {
                stop_reason: verlet_history::CanonicalStopReason::ToolUse,
            },
        ],
        &events,
    )
    .unwrap();

    assert_eq!(response.content, vec![completed]);
    assert_eq!(
        std::iter::from_fn(|| runtime_events.try_recv().ok())
            .filter(|event| matches!(
                event,
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime {
                    event: crate::kernel::runtime_host::runtime_events::RuntimeEvent {
                        kind: crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted { .. },
                        ..
                    },
                    ..
                }
            ))
            .count(),
        1
    );
}

#[test]
fn stream_assembly_emits_one_started_event_for_delta_and_completed_call() {
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let (events, mut runtime_events) = tokio::sync::broadcast::channel(8);

    let response = super::response_from_stream_events(
        &coordinates,
        vec![
            verlet_provider::ProviderStreamEvent::ToolCallDelta {
                id: "call_1".to_string(),
                name: Some("bash".to_string()),
                arguments_delta: "{\"command\":\"pwd\"}".to_string(),
            },
            verlet_provider::ProviderStreamEvent::Content {
                content: verlet_history::CanonicalContent::tool_call(
                    "call_1",
                    "bash",
                    serde_json::json!({"command": "pwd"}),
                ),
            },
            verlet_provider::ProviderStreamEvent::Done {
                stop_reason: verlet_history::CanonicalStopReason::ToolUse,
            },
        ],
        &events,
    )
    .unwrap();

    assert_eq!(response.content.len(), 1);
    let started = std::iter::from_fn(|| runtime_events.try_recv().ok())
        .filter_map(|event| {
            match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                match event.kind {
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
                        call_id,
                        name,
                        input,
                    } => Some((call_id, name, input)),
                    _ => None,
                }
            }
            _ => None,
        }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        started,
        vec![(
            "call_1".to_string(),
            "bash".to_string(),
            serde_json::json!({"command": "pwd"}),
        )]
    );
}

#[test]
fn stream_assembly_preserves_named_call_that_collides_with_completed_id_component() {
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let (events, mut runtime_events) = tokio::sync::broadcast::channel(8);

    let response = super::response_from_stream_events(
        &coordinates,
        vec![
            verlet_provider::ProviderStreamEvent::ToolCallDelta {
                id: "call_1".to_string(),
                name: Some("read".to_string()),
                arguments_delta: "{\"path\":\"input.txt\"}".to_string(),
            },
            verlet_provider::ProviderStreamEvent::Content {
                content: verlet_history::CanonicalContent::tool_call(
                    "call_1|fc_1",
                    "write",
                    serde_json::json!({"path": "output.txt"}),
                ),
            },
            verlet_provider::ProviderStreamEvent::Done {
                stop_reason: verlet_history::CanonicalStopReason::ToolUse,
            },
        ],
        &events,
    )
    .unwrap();

    assert_eq!(response.content.len(), 2);
    assert!(response.content.iter().any(|content| matches!(
        content,
        verlet_history::CanonicalContent::ToolCall { id, name, arguments }
            if id == "call_1"
                && name == "read"
                && arguments == &serde_json::json!({"path": "input.txt"})
    )));
    assert!(response.content.iter().any(|content| matches!(
        content,
        verlet_history::CanonicalContent::ToolCall { id, name, arguments }
            if id == "call_1|fc_1"
                && name == "write"
                && arguments == &serde_json::json!({"path": "output.txt"})
    )));
    let started_ids = std::iter::from_fn(|| runtime_events.try_recv().ok())
        .filter_map(|event| {
            match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                match event.kind {
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
                        call_id,
                        ..
                    } => Some(call_id),
                    _ => None,
                }
            }
            _ => None,
        }
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        started_ids,
        std::collections::BTreeSet::from(["call_1".to_string(), "call_1|fc_1".to_string(),])
    );
}

#[test]
fn tool_call_id_aliases_split_only_once_and_omit_empty_components() {
    assert_eq!(super::tool_call_id_aliases("call_1"), vec!["call_1"]);
    assert_eq!(
        super::tool_call_id_aliases("call_1|fc_1"),
        vec!["call_1|fc_1", "call_1", "fc_1"]
    );
    assert_eq!(
        super::tool_call_id_aliases("call_1|namespace|fc_1"),
        vec!["call_1|namespace|fc_1", "call_1", "namespace|fc_1"]
    );
    assert_eq!(super::tool_call_id_aliases("|fc_1"), vec!["|fc_1", "fc_1"]);
    assert_eq!(
        super::tool_call_id_aliases("call_1|"),
        vec!["call_1|", "call_1"]
    );
}

#[tokio::test]
async fn stream_and_complete_preserve_equivalent_final_history() {
    let complete_client =
        std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
            "same reply",
        )]));
    let complete_host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory(std::sync::Arc::clone(&complete_client)),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let complete_thread = complete_host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "session_complete",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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

    let streaming_usage = verlet_history::CanonicalUsage {
        input_tokens: 1,
        output_tokens: 2,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    let stream_client = std::sync::Arc::new(StreamingClient::new(vec![vec![
        verlet_provider::ProviderStreamEvent::TextDelta {
            text: "same reply".to_string(),
        },
        verlet_provider::ProviderStreamEvent::Usage {
            usage: streaming_usage,
        },
        verlet_provider::ProviderStreamEvent::Done {
            stop_reason: verlet_history::CanonicalStopReason::EndTurn,
        },
    ]]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = stream_client;
    let stream_host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        streaming_runtime_factory(provider_client),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let stream_thread = stream_host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "session_stream",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            verlet_history::CanonicalMessage::Assistant {
                content: complete_content,
                usage: complete_usage,
                stop_reason: complete_stop_reason,
                ..
            },
            verlet_history::CanonicalMessage::Assistant {
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_text("first reply"),
        response_text("summary from model"),
    ]));
    let pre_hook = std::sync::Arc::new(StaticHookHandler::new(
        "pre-compact",
        crate::agent::hooks::HookEventName::PreCompact,
        Some("manual"),
        crate::agent::hooks::HookHandlerOutput::default(),
    ));
    let post_hook = std::sync::Arc::new(StaticHookHandler::new(
        "post-compact",
        crate::agent::hooks::HookEventName::PostCompact,
        Some("manual"),
        crate::agent::hooks::HookHandlerOutput::default(),
    ));
    let pre_handler: std::sync::Arc<dyn crate::agent::hooks::HookHandler> = pre_hook.clone();
    let post_handler: std::sync::Arc<dyn crate::agent::hooks::HookHandler> = post_hook.clone();
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let factory = std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, client.clone())
            .with_hook_pipeline(std::sync::Arc::new(
                crate::agent::hooks::HookPipeline::new()
                    .with_handler(pre_handler)
                    .with_handler(post_handler),
            )),
    );
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(factory, store.clone());
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    assert_compaction(
        &mut events,
        crate::kernel::compaction::CompactionTrigger::Manual,
        "summary from model",
    )
    .await;

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[1].messages),
        vec!["hello", "first reply"]
    );
    assert!(matches!(
        pre_hook.requests().as_slice(),
        [crate::agent::hooks::HookRequest::PreCompact(request)] if request.trigger == crate::kernel::compaction::CompactionTrigger::Manual
    ));
    assert!(matches!(
        post_hook.requests().as_slice(),
        [crate::agent::hooks::HookRequest::PostCompact(request)]
            if request.trigger == crate::kernel::compaction::CompactionTrigger::Manual
                && request.summary == "summary from model"
    ));
    assert_eq!(
        text_messages(&thread.session_context().await.unwrap().messages),
        vec!["Compacted conversation summary:\nsummary from model"]
    );

    let stream_id = verlet_history::EventStreamId::for_thread(&thread.context().coordinates);
    let persisted_events = store.read_events(&stream_id, None).await.unwrap();
    let summary_event = persisted_events
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ContextSummaryCompleted)
        .expect("compaction should persist a context.summary.completed event");
    assert_eq!(
        summary_event.origin,
        verlet_history::EventOrigin::Discharged
    );
    assert_eq!(
        summary_event.payload["schema"],
        "cooldis.event.context.summary.completed/1"
    );
    assert_eq!(summary_event.payload["text"], "summary from model");
    let expected_summary_hash = format!(
        "sha256:{}",
        verlet_agent::contracts::sha256_hex("summary from model".as_bytes())
    );
    assert_eq!(
        summary_event.payload["content"]["sha256"].as_str(),
        Some(expected_summary_hash.as_str())
    );

    let read_plan_event = persisted_events
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ContextReadPlanSet)
        .expect("compaction should persist a context.read_plan.set event");
    assert_eq!(
        read_plan_event.origin,
        verlet_history::EventOrigin::Discharged
    );
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
async fn routed_compaction_uses_the_resolved_endpoints_token_limit() {
    let launch_client = std::sync::Arc::new(RecordingClient::default());
    let selected_client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_text("selected reply"),
        response_text("selected summary"),
    ]));
    let mut selected_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIChatCompletions,
        "selected-provider",
        "selected-model",
    );
    selected_config.max_tokens = 333;
    let router = std::sync::Arc::new(MutableTurnEndpointRouter::new(
        crate::adapters::agent_loop::ResolvedTurnEndpoint {
            config: selected_config,
            client: selected_client.clone(),
        },
    ));
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(
            crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::OpenAIResponses,
                "launch-provider",
                "launch-model",
            ),
            launch_client.clone(),
        )
        .with_turn_endpoint_router(router),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "session_routed_compaction",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    assert_output(&mut events, "selected reply").await;
    host.compact_thread(thread.context().coordinates.thread_id, "compact-1", None)
        .await
        .unwrap();
    assert_compaction(
        &mut events,
        crate::kernel::compaction::CompactionTrigger::Manual,
        "selected summary",
    )
    .await;

    let requests = selected_client.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].max_tokens, 333);
    assert_eq!(requests[1].max_tokens, 333);
    assert!(launch_client.requests().is_empty());
}

#[tokio::test]
async fn compaction_reattaches_a_late_tool_result_before_the_replacement_user() {
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
        "summary after late result",
    )]));
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
        "tenant_a",
        "user_1",
        "compact-late-result",
    );
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let first_user = store
        .append_turn_input(
            &coordinates,
            "turn-old",
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("first turn"),
            },
        )
        .await
        .unwrap();
    let assistant = store
        .append(
            &coordinates,
            Some(first_user.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::assistant(
                    "openai",
                    verlet_history::ProviderApi::OpenAIResponses,
                    "gpt-test",
                    vec![verlet_history::CanonicalContent::tool_call(
                        "call-late",
                        "lookup",
                        serde_json::json!({"q": "slow"}),
                    )],
                    verlet_history::CanonicalStopReason::ToolUse,
                ),
            },
        )
        .await
        .unwrap();
    let replacement_user = store
        .append_turn_input(
            &coordinates,
            "turn-new",
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("replacement turn"),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &coordinates,
            Some(replacement_user.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::tool_result(
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
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(crate::adapters::agent_loop::AgentLoopFactory::new(
            crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::OpenAIResponses,
                "openai",
                "gpt-test",
            ),
            client.clone(),
        )),
        store,
    );
    let thread = host
        .start_thread(
            coordinates,
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.compact_thread(thread.context().coordinates.thread_id, "compact-1", None)
        .await
        .unwrap();
    assert_compaction(
        &mut events,
        crate::kernel::compaction::CompactionTrigger::Manual,
        "summary after late result",
    )
    .await;

    let request = client.requests().pop().unwrap();
    assert!(matches!(
        request.messages.as_slice(),
        [
            verlet_history::CanonicalMessage::User { .. },
            verlet_history::CanonicalMessage::Assistant { .. },
            verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. },
            verlet_history::CanonicalMessage::User { .. },
        ] if tool_call_id == "call-late"
    ));
}

#[tokio::test]
async fn auto_compaction_triggers_before_next_submit_when_budget_is_exceeded() {
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_text("first reply"),
        response_text("auto summary"),
        response_text("second reply"),
    ]));
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let factory = std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, client.clone())
            .with_compaction_policy(
                crate::kernel::compaction::CompactionPolicy::auto_at_text_bytes(5),
            ),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory,
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    assert_compaction(
        &mut events,
        crate::kernel::compaction::CompactionTrigger::Auto,
        "auto summary",
    )
    .await;
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_text("root reply"),
        response_text("resumed reply"),
        response_text("fork reply"),
    ]));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory(std::sync::Arc::clone(&client)),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    assert_compaction(
        &mut events,
        crate::kernel::compaction::CompactionTrigger::Manual,
        "root summary",
    )
    .await;
    let checkpoint = host
        .create_checkpoint(
            thread.context().coordinates.thread_id,
            None,
            Some("after-compact".to_string()),
            std::collections::BTreeMap::new(),
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_text("reply a"),
        response_text("reply b"),
    ]));
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(factory(std::sync::Arc::clone(&client)));
    let a = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let b = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_b", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_tool_call()]));
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory(client));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use bash")
        .await
        .unwrap();
    let assistant = assert_assistant_mirror(&mut events).await;

    match assistant {
        verlet_history::CanonicalMessage::Assistant {
            provider,
            api,
            model,
            content,
            stop_reason,
            ..
        } => {
            assert_eq!(provider, "openai");
            assert_eq!(api, verlet_history::ProviderApi::OpenAIResponses);
            assert_eq!(model, "gpt-test");
            assert_eq!(stop_reason, verlet_history::CanonicalStopReason::ToolUse);
            assert!(matches!(
                content.first(),
                Some(verlet_history::CanonicalContent::ToolCall { id, name, .. })
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "verlet"})),
        response_text("final reply"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory_with_registry(
        provider_client,
        registry,
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                call_id,
                output,
                success: true,
                duration_ms: Some(_),
            } if call_id == "call_1|fc_1" && output == "echo:verlet"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PermissionDecision {
                call_id,
                tool_name,
                decision: verlet_runtime_contracts::RuntimePermissionDecision::Allow,
                reason: None,
            } if call_id == "call_1|fc_1" && tool_name == "echo_search"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolLog {
                call_id,
                tool_name,
                level: verlet_runtime_contracts::RuntimeToolLogLevel::Info,
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
        verlet_history::CanonicalMessage::ToolResult {
            tool_call_id,
            tool_name,
            content,
            is_error: false,
            ..
        } if tool_call_id == "call_1|fc_1"
            && tool_name == "echo_search"
            && text_from_content(content) == "echo:verlet"
    ));

    let session = thread.session_context().await.unwrap();
    assert_eq!(
        text_messages(&session.messages),
        vec!["use echo", "", "echo:verlet", "final reply"]
    );
    assert!(session.entries.iter().all(is_canonical_message_entry));
}

#[tokio::test]
async fn default_tool_round_budget_still_fails_after_eight_completed_batches() {
    let registry = echo_registry("echo").await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(tool_round_responses(9)));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory_with_registry(
        provider_client,
        registry,
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "round-default"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(tool_round_responses(9)));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory_with_registry(
        provider_client,
        registry,
    ));
    let thread = host
        .start_thread_with_topology_and_metadata(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "round-64"),
            verlet_runtime_contracts::ThreadTopology::root(),
            std::collections::BTreeMap::from([(
                crate::adapters::agent_loop::THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA
                    .to_string(),
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(tool_round_responses(12)));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory_with_registry(
        provider_client,
        registry,
    ));
    let thread = host
        .start_thread_with_topology_and_metadata(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "round-unlimited",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
            std::collections::BTreeMap::from([(
                crate::adapters::agent_loop::THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA
                    .to_string(),
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
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "round-provenance");
    let malformed_request = || {
        verlet_history::NewEventRecord::discharged(
            coordinates.clone(),
            verlet_history::EventKind::ToolCallRequested,
            serde_json::to_value(crate::kernel::control_decision::ToolCallRequestedPayload {
                subject: crate::kernel::control_decision::ToolCallSubject {
                    turn_id: "turn-1".to_string(),
                    call_id: "call-1".to_string(),
                },
                snapshot_id: "unbound".to_string(),
                tool_name: "thread_status".to_string(),
                arguments: serde_json::json!({"task_name": "worker-a"}),
                attach_event_id: None,
                args_fingerprint: None,
                holds: Vec::new(),
            })
            .unwrap(),
            verlet_history::EventProvenance {
                source_streams: vec![verlet_history::EventStreamId::for_thread(&coordinates)],
                discharged_by: Some("test:malformed-round".to_string()),
                function: Some("tool_request/v1".to_string()),
                ..verlet_history::EventProvenance::default()
            },
        )
    };
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![malformed_request()],
        )
        .await
        .unwrap();
    let turn_submitted = store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": "turn-1"}),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

    assert_eq!(
        crate::adapters::agent_loop::persisted_tool_rounds_for_turn(
            &services,
            &coordinates,
            "turn-1",
            turn_submitted.sequence,
        )
        .await
        .unwrap(),
        0,
        "malformed events before the active turn bound must not affect accounting"
    );
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![malformed_request()],
        )
        .await
        .unwrap();

    let err = crate::adapters::agent_loop::persisted_tool_rounds_for_turn(
        &services,
        &coordinates,
        "turn-1",
        turn_submitted.sequence,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("has no assistant source event"));
}

#[tokio::test]
async fn persisted_round_accounting_rejects_a_cross_turn_assistant_source() {
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "round-cross-turn");
    let old_assistant = store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::SessionEntryAppended,
                serde_json::json!({
                    "entry_id": verlet_history::SessionEntryId::new().to_string(),
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
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": "turn-1"}),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ToolCallRequested,
                serde_json::to_value(crate::kernel::control_decision::ToolCallRequestedPayload {
                    subject: crate::kernel::control_decision::ToolCallSubject {
                        turn_id: "turn-1".to_string(),
                        call_id: "call-1".to_string(),
                    },
                    snapshot_id: "unbound".to_string(),
                    tool_name: "thread_status".to_string(),
                    arguments: serde_json::json!({"task_name": "worker-a"}),
                    attach_event_id: None,
                    args_fingerprint: None,
                    holds: Vec::new(),
                })
                .unwrap(),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::for_thread(&coordinates)],
                    source_event_ids: vec![old_assistant.id],
                    discharged_by: Some("test:cross-turn-round".to_string()),
                    function: Some("tool_request/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();

    let err = crate::adapters::agent_loop::persisted_tool_rounds_for_turn(
        &services,
        &coordinates,
        "turn-1",
        turn_submitted.sequence,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("outside the active turn"));
}

#[tokio::test]
async fn independent_thread_holds_overlap_results_append_in_call_order_and_finish_is_witnessed() {
    let tool_provider = std::sync::Arc::new(FinishSecondFirstToolProvider {
        second_finished: tokio::sync::Notify::new(),
    });
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client;
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
                .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "hold-overlap"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. } => {
                Some(tool_call_id.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(result_ids, vec!["call-first", "call-second"]);

    let records = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let requests = records
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
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
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[0].payload["subject"]["call_id"], "call-first");
    assert_eq!(completed[0].payload["finish_order"], 1);
    assert_eq!(completed[1].payload["subject"]["call_id"], "call-second");
    assert_eq!(completed[1].payload["finish_order"], 0);
}

#[tokio::test]
async fn duplicate_model_tool_call_ids_fail_before_the_batch_is_witnessed() {
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = std::sync::Arc::new(ImmediateThreadToolProvider);
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_tool_calls(
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
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "duplicate-tool-call-id",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        records
            .iter()
            .all(|event| event.kind != verlet_history::EventKind::ToolCallRequested),
        "an ambiguous batch must fail before request ids become durable"
    );
}

#[tokio::test]
async fn cancellation_waits_for_buffered_call_order_commit_to_finish() {
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = std::sync::Arc::new(ImmediateThreadToolProvider);
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_tool_calls(
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
    let store = std::sync::Arc::new(PausingRuntimeStore::after_first_append_of(
        verlet_history::EventKind::ToolCallCompleted,
    ));
    let pause = std::sync::Arc::clone(&store.pause);
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "cancel-during-tool-commit",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    tokio::time::timeout(
        tokio::time::Duration::from_secs(30),
        pause.wait_until_entered(),
    )
    .await
    .expect("first completion append did not reach the pause");

    host.cancel(
        thread.context().coordinates.thread_id,
        "cancel during commit",
    )
    .await
    .unwrap();
    assert!(
        // tight-timeout: cancellation must remain absent until the buffered commit is released
        tokio::time::timeout(tokio::time::Duration::from_millis(100), async {
            loop {
                if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { .. } =
                    events.recv().await.unwrap()
                {
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let completed = records
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .map(|event| event.payload["subject"]["call_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(completed, vec!["call-first", "call-second"]);
}

#[tokio::test]
async fn cancellation_racing_suspended_turn_commit_observes_the_full_boundary() {
    let registry = echo_registry("echo").await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "verlet"})),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client;
    let store = std::sync::Arc::new(PausingRuntimeStore::after_first_append_of(
        verlet_history::EventKind::TurnWaiting,
    ));
    let pause = std::sync::Arc::clone(&store.pause);
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "cancel-during-tool-wait",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    tokio::time::timeout(
        tokio::time::Duration::from_secs(30),
        pause.wait_until_entered(),
    )
    .await
    .expect("turn.waiting append did not reach the pause");
    host.cancel(
        thread.context().coordinates.thread_id,
        "cancel during suspended commit",
    )
    .await
    .unwrap();
    assert!(
        // tight-timeout: cancellation must remain absent until the suspended commit is released
        tokio::time::timeout(tokio::time::Duration::from_millis(100), async {
            loop {
                if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { .. } =
                    events.recv().await.unwrap()
                {
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
            &verlet_history::EventStreamId::new(format!(
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
            .filter(|event| event.kind == verlet_history::EventKind::TurnWaiting)
            .count(),
        1
    );
    let thread_records = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        thread_records
            .iter()
            .all(|event| event.kind != verlet_history::EventKind::ToolCallCompleted)
    );
}

#[tokio::test]
async fn cancellation_during_atomic_request_append_leaves_all_or_no_batch_witnesses() {
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = std::sync::Arc::new(ImmediateThreadToolProvider);
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_tool_calls(
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
    let store = std::sync::Arc::new(PausingRuntimeStore::after_first_append_of(
        verlet_history::EventKind::ToolCallRequested,
    ));
    let pause = std::sync::Arc::clone(&store.pause);
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "cancel-during-request-append",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    tokio::time::timeout(
        tokio::time::Duration::from_secs(30),
        pause.wait_until_entered(),
    )
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
            .count(),
        2
    );
    let completed = records
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .map(|event| {
            serde_json::from_value::<crate::kernel::control_decision::ToolCallCompletedPayload>(
                event.payload.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    assert!(completed.iter().all(|payload| {
        !payload.success
            && payload.cancellation
                == Some(
                    crate::kernel::control_decision::ToolCallCancellation::CancelledAcknowledged,
                )
    }));
}

#[tokio::test]
async fn conflicting_thread_holds_serialize_in_model_call_order() {
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let tool_provider = std::sync::Arc::new(SerialBlockingToolProvider {
        tool_name: "thread_submit",
        started: started_tx,
        release_first: tokio::sync::Notify::new(),
    });
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider.clone()),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client;
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
            .with_tool_router(router),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "hold-serialize",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        tokio::time::timeout(tokio::time::Duration::from_secs(30), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "first"
    );
    assert!(started_rx.try_recv().is_err());
    tool_provider.release_first.notify_one();
    assert_eq!(
        tokio::time::timeout(tokio::time::Duration::from_secs(30), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "second"
    );
    assert_output(&mut events, "final reply").await;
}

#[tokio::test]
async fn bash_family_holds_prevent_interleaving_before_the_harness_mutex() {
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let tool_provider = std::sync::Arc::new(SerialBlockingToolProvider {
        tool_name: "bash",
        started: started_tx,
        release_first: tokio::sync::Notify::new(),
    });
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider.clone()),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client;
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
            .with_tool_router(router),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "bash-hold-serialize",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        tokio::time::timeout(tokio::time::Duration::from_secs(30), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "first"
    );
    assert!(started_rx.try_recv().is_err());
    tool_provider.release_first.notify_one();
    assert_eq!(
        tokio::time::timeout(tokio::time::Duration::from_secs(30), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "second"
    );
    assert_output(&mut events, "final reply").await;
}

#[tokio::test]
async fn suspended_batch_finishes_and_appends_other_members_before_turn_waits() {
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = std::sync::Arc::new(ImmediateThreadToolProvider);
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_tool_calls(
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
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
                .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "hold-suspension",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        verlet_history::EventKind::TurnWaiting,
    )
    .await;
    wait_for_status(&mut status, verlet_runtime_contracts::ThreadStatus::Idle).await;

    assert_eq!(client.requests().len(), 1);
    let session = thread.session_context().await.unwrap();
    assert!(session.messages.iter().any(|message| {
        matches!(
            message,
            verlet_history::CanonicalMessage::ToolResult {
                tool_call_id,
                is_error: false,
                ..
            } if tool_call_id == "call-finish"
        )
    }));
    assert!(session.messages.iter().all(|message| {
        !matches!(
            message,
            verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. } if tool_call_id == "call-wait"
        )
    }));
}

#[tokio::test]
async fn provider_waits_for_every_suspended_batch_member_before_continuing() {
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = std::sync::Arc::new(ImmediateThreadToolProvider);
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "all-tools-suspended",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        verlet_history::EventKind::TurnWaiting,
    )
    .await;
    wait_for_status(&mut status, verlet_runtime_contracts::ThreadStatus::Idle).await;
    for call_id in ["call-first", "call-second"] {
        append_witnessed_tool_decision(
            &store,
            &thread.context().coordinates,
            "snapshot-controller",
            "turn-1",
            call_id,
            crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
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
    wait_for_status(&mut status, verlet_runtime_contracts::ThreadStatus::Idle).await;
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
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = std::sync::Arc::new(IsolatedFailureToolProvider);
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client;
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
            .with_tool_router(router),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "hold-failure-isolation",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            verlet_history::CanonicalMessage::ToolResult {
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
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = std::sync::Arc::new(IsolatedFailureToolProvider);
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "hold-error-release",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            verlet_history::CanonicalMessage::ToolResult {
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
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (acknowledged_tx, mut acknowledged_rx) = tokio::sync::mpsc::unbounded_channel();
    let acknowledging_provider = std::sync::Arc::new(CancellationAcknowledgingThreadToolProvider {
        started: started_tx.clone(),
        acknowledged: acknowledged_tx,
    });
    let non_observing_provider = std::sync::Arc::new(NonObservingThreadToolProvider {
        started: started_tx,
        released: std::sync::atomic::AtomicBool::new(false),
        release: tokio::sync::Notify::new(),
        never_launched: std::sync::atomic::AtomicBool::new(true),
    });
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(acknowledging_provider)
        .with_kernel_tool_provider(non_observing_provider.clone()),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client;
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
                .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread_with_topology_and_metadata(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "interrupt-tool-batch",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
            std::collections::BTreeMap::new(),
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
    assert!(
        non_observing_provider
            .never_launched
            .load(std::sync::atomic::Ordering::SeqCst)
    );

    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-replacement",
        "replacement",
        verlet_runtime_contracts::TurnSubmissionMode::Interrupt,
    )
    .await
    .unwrap();
    assert_eq!(
        acknowledged_rx.recv().await.as_deref(),
        Some("call-acknowledged")
    );
    tokio::task::yield_now().await;

    tokio::time::advance(tokio::time::Duration::from_millis(99)).await;
    tokio::task::yield_now().await;
    assert!(
        !drain_has_cancelled(&mut events),
        "the turn terminal must remain blocked until the configured grace"
    );

    tokio::time::advance(tokio::time::Duration::from_millis(1)).await;
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let requests = before_detached_settlement
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|event| !event.payload["holds"].is_null())
    );
    let completed_before_release = before_detached_settlement
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let exceeded = records
        .iter()
        .find(|event| {
            event.kind == verlet_history::EventKind::ToolCallCompleted
                && event.payload["subject"]["call_id"] == "call-exceeded"
        })
        .expect("detached invocation did not settle its own completion");
    assert_eq!(
        exceeded.payload["cancellation"],
        serde_json::json!("cancelled_exceeded_grace")
    );
    assert_eq!(exceeded.payload["success"], true);
    assert!(
        non_observing_provider
            .never_launched
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invocation_panic_after_grace_still_self_settles_exactly_once() {
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let tool_provider = std::sync::Arc::new(PanickingAfterGraceToolProvider {
        started: started_tx,
        release: tokio::sync::Notify::new(),
    });
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider.clone()),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("thread_status", serde_json::json!({"task_name": "worker"})),
    ]));
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "panic-after-grace",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    tokio::time::advance(tokio::time::Duration::from_millis(100)).await;
    assert_cancelled(&mut events, "cancel panicking tool").await;

    tool_provider.release.notify_waiters();
    wait_for_tool_completion_count(&store, &thread.context().coordinates, 1).await;
    let completions = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .map(|event| {
            serde_json::from_value::<crate::kernel::control_decision::ToolCallCompletedPayload>(
                event.payload,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(completions.len(), 1);
    assert!(!completions[0].success);
    assert_eq!(
        completions[0].cancellation,
        Some(crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace)
    );
}

#[tokio::test]
async fn invocation_panic_before_cancellation_is_a_failed_completion() {
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let tool_provider = std::sync::Arc::new(PanickingAfterGraceToolProvider {
        started: started_tx,
        release: tokio::sync::Notify::new(),
    });
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider.clone()),
    );
    let client: std::sync::Arc<dyn verlet_provider::ProviderClient> =
        std::sync::Arc::new(RecordingClient::with_responses(vec![
            response_tool_call_named("thread_status", serde_json::json!({"task_name": "worker"})),
        ]));
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "panic-before-cancel",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .map(|event| {
            serde_json::from_value::<crate::kernel::control_decision::ToolCallCompletedPayload>(
                event.payload,
            )
            .unwrap()
        })
        .unwrap();
    assert!(!completion.success);
    assert_eq!(completion.cancellation, None);
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn monitor_panic_after_settlement_recovers_one_completion() {
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (acknowledged_tx, mut acknowledged_rx) = tokio::sync::mpsc::unbounded_channel();
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = std::sync::Arc::new(CancellationAcknowledgingThreadToolProvider {
        started: started_tx,
        acknowledged: acknowledged_tx,
    });
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("thread_submit", serde_json::json!({"task_name": "worker"})),
    ]));
    let inner = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let store = std::sync::Arc::new(crate::support::fault::FaultingRuntimeStore::new(
        inner.clone(),
    ));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "monitor-panic"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    tokio::time::timeout(tokio::time::Duration::from_secs(30), started_rx.recv())
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
    tokio::time::timeout(tokio::time::Duration::from_secs(30), acknowledged_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_cancelled(&mut events, "cancel monitor panic").await;

    let records = inner
        .read_events(
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|event| {
                event.kind == verlet_history::EventKind::ToolCallCompleted
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
                verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. }
                    if tool_call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn detached_completion_retry_is_idempotent_before_and_after_a_store_failure() {
    for fail_after_append in [false, true] {
        let inner = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
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
                &verlet_history::EventStreamId::for_thread(&coordinates),
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ToolCallRequested,
                    serde_json::to_value(
                        crate::kernel::control_decision::ToolCallRequestedPayload {
                            subject: crate::kernel::control_decision::ToolCallSubject {
                                turn_id: "turn-1".to_string(),
                                call_id: "call-1".to_string(),
                            },
                            snapshot_id: "snapshot-1".to_string(),
                            tool_name: "thread_status".to_string(),
                            arguments: serde_json::json!({}),
                            attach_event_id: None,
                            args_fingerprint: None,
                            holds: Vec::new(),
                        },
                    )
                    .unwrap(),
                    verlet_history::EventProvenance {
                        source_streams: vec![verlet_history::EventStreamId::for_thread(
                            &coordinates,
                        )],
                        discharged_by: Some("test:detached-retry".to_string()),
                        function: Some("tool_request/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        inner
            .append_events(
                &verlet_history::EventStreamId::for_thread(&coordinates),
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::ToolCallCompleted,
                    serde_json::json!({
                        "subject": {"turn_id": "unrelated-turn"},
                        "malformed": true
                    }),
                )],
            )
            .await
            .unwrap();
        let faulting = crate::support::fault::FaultingRuntimeStore::new(inner.clone());
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
        let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
            std::sync::Arc::new(faulting),
            crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
        );
        let turn_context = crate::kernel::runtime_host::turn::TurnContext::new(
            verlet_runtime_contracts::ThreadContext::root(coordinates.clone()),
            "turn-1",
            &crate::kernel::runtime_host::turn::TurnInput::text(""),
            tokio_util::sync::CancellationToken::new(),
        );
        let (events, mut event_rx) = tokio::sync::broadcast::channel(16);
        let append = tokio::spawn({
            let services = services.clone();
            let turn_context = turn_context.clone();
            async move {
                crate::adapters::agent_loop::append_detached_tool_call_outcome_until_recorded(
                    &services,
                    &turn_context,
                    coordinates.thread_id,
                    &events,
                    Ok(
                        crate::adapters::agent_loop::PreparedToolCallOutcome::Completed {
                            call_id: "call-1".to_string(),
                            tool_name: "thread_status".to_string(),
                            snapshot_id: "snapshot-1".to_string(),
                            args_fingerprint: None,
                            source_event_id: request.id,
                            finish_order: 0,
                            cancellation: Some(crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace),
                            outcome: Box::new(crate::agent::tool_interceptor::ToolExecutionOutcome {
                                result: verlet_history::CanonicalMessage::tool_result(
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
                        },
                    ),
                )
                .await;
            }
        });
        loop {
            if matches!(
                event_rx.recv().await.unwrap(),
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime {
                    event: crate::kernel::runtime_host::runtime_events::RuntimeEvent {
                        kind: crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery { ref action, .. },
                        ..
                    },
                    ..
                } if action == "retry_detached_tool_completion"
            ) {
                break;
            }
        }
        tokio::time::advance(crate::adapters::agent_loop::DETACHED_COMPLETION_RETRY_DELAY).await;
        append.await.unwrap();

        let completions = inner
            .read_events(
                &verlet_history::EventStreamId::for_thread(&coordinates),
                None,
            )
            .await
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.kind == verlet_history::EventKind::ToolCallCompleted
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
                    verlet_history::SessionEntryKind::Message {
                        message: verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. }
                    } if tool_call_id == "call-1"
                )
            })
            .count();
        assert_eq!(completions, 1);
        assert_eq!(results, 1);
    }
}

#[tokio::test]
async fn result_append_commits_before_its_completion_event() {
    let inner = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
        "tenant_a",
        "user_1",
        "result-before-completion",
    );
    let request = append_recovery_request(
        inner.as_ref(),
        &coordinates,
        serde_json::json!({"input":"same"}),
    )
    .await;
    let payload =
        serde_json::from_value::<crate::kernel::control_decision::ToolCallRequestedPayload>(
            request.payload.clone(),
        )
        .unwrap();
    let faulting = std::sync::Arc::new(
        crate::support::fault::FaultingRuntimeStore::new(inner.clone()).fail_nth(
            "append_events_fenced",
            1,
            "completion append failed before commit",
        ),
    );
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        faulting,
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let (events, _) = tokio::sync::broadcast::channel(8);

    let err = crate::adapters::agent_loop::append_tool_result_message(
        &services,
        &coordinates,
        coordinates.thread_id,
        &events,
        "call-recovery".to_string(),
        "recovery_tool".to_string(),
        "turn-recovery".to_string(),
        "snapshot-recovery".to_string(),
        payload.args_fingerprint,
        verlet_history::CanonicalMessage::tool_result(
            "call-recovery",
            "recovery_tool",
            "persisted first",
            false,
        ),
        Some(1),
        Some(0),
        None,
        request.id,
        false,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("completion append failed"),
        "{err}"
    );

    let records = inner
        .read_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        records
            .iter()
            .all(|event| event.kind != verlet_history::EventKind::ToolCallCompleted)
    );
    assert!(
        inner
            .build_context(&coordinates)
            .await
            .unwrap()
            .messages
            .iter()
            .any(|message| matches!(
                message,
                verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. }
                    if tool_call_id == "call-recovery"
            ))
    );
}

#[tokio::test]
async fn legacy_completion_does_not_swallow_a_new_fingerprinted_completion() {
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
        "tenant_a",
        "user_1",
        "legacy-completion-collision",
    );
    let legacy_request = store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::ToolCallRequested,
                serde_json::to_value(crate::kernel::control_decision::ToolCallRequestedPayload {
                    subject: crate::kernel::control_decision::ToolCallSubject {
                        turn_id: "turn-recovery".to_string(),
                        call_id: "call-recovery".to_string(),
                    },
                    snapshot_id: "snapshot-recovery".to_string(),
                    tool_name: "recovery_tool".to_string(),
                    arguments: serde_json::json!({"input":"legacy"}),
                    attach_event_id: None,
                    args_fingerprint: None,
                    holds: Vec::new(),
                })
                .unwrap(),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    crate::adapters::agent_loop::append_tool_completion_event(
        &services,
        &coordinates,
        "turn-recovery".to_string(),
        "call-recovery".to_string(),
        "snapshot-recovery".to_string(),
        "recovery_tool".to_string(),
        None,
        true,
        Some(1),
        Some(0),
        None,
    )
    .await
    .unwrap();
    let current = append_recovery_request(
        store.as_ref(),
        &coordinates,
        serde_json::json!({"input":"current"}),
    )
    .await;
    let current_payload = serde_json::from_value::<
        crate::kernel::control_decision::ToolCallRequestedPayload,
    >(current.payload.clone())
    .unwrap();
    let current_fingerprint = current_payload.args_fingerprint.clone().unwrap();
    let (events, _) = tokio::sync::broadcast::channel(8);

    assert!(
        !crate::adapters::agent_loop::matching_tool_call_completed_exists(
            &services,
            &coordinates,
            "turn-recovery",
            "call-recovery",
            "snapshot-recovery",
            Some(&current_fingerprint),
        )
        .await
        .unwrap(),
        "a legacy completion must not terminate a new fingerprinted generation"
    );

    crate::adapters::agent_loop::append_tool_result_message(
        &services,
        &coordinates,
        coordinates.thread_id,
        &events,
        "call-recovery".to_string(),
        "recovery_tool".to_string(),
        "turn-recovery".to_string(),
        "snapshot-recovery".to_string(),
        current_payload.args_fingerprint,
        verlet_history::CanonicalMessage::tool_result(
            "call-recovery",
            "recovery_tool",
            "current result",
            false,
        ),
        Some(1),
        Some(1),
        None,
        current.id,
        false,
    )
    .await
    .unwrap();

    let completions = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .map(|event| {
            serde_json::from_value::<crate::kernel::control_decision::ToolCallCompletedPayload>(
                event.payload,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(completions.len(), 2);
    assert!(
        completions
            .iter()
            .any(|completion| completion.args_fingerprint.as_deref() == Some(&current_fingerprint))
    );
    assert!(
        crate::adapters::agent_loop::matching_tool_call_completed_exists(
            &services,
            &coordinates,
            "turn-recovery",
            "call-recovery",
            "snapshot-recovery",
            Some(&current_fingerprint),
        )
        .await
        .unwrap(),
        "the exact completion must terminate the current generation"
    );
    assert!(
        crate::adapters::agent_loop::existing_tool_result_message(
            &services,
            &coordinates,
            current.id,
            "call-recovery",
            "snapshot-recovery",
            Some(&current_fingerprint),
        )
        .await
        .unwrap()
        .is_some()
    );
    let _ = legacy_request;
}

#[tokio::test]
async fn completion_append_is_subject_idempotent_under_concurrency() {
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "completion-race");
    let append = || {
        crate::adapters::agent_loop::append_tool_completion_event(
            &services,
            &coordinates,
            "turn-1".to_string(),
            "call-1".to_string(),
            "snapshot-1".to_string(),
            "thread_status".to_string(),
            Some(format!("sha256:{}", "a".repeat(64))),
            false,
            Some(0),
            Some(0),
            Some(crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace),
        )
    };

    let (left, right) = tokio::join!(append(), append());
    left.unwrap();
    right.unwrap();

    let completions = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .count();
    assert_eq!(completions, 1);
}

#[tokio::test]
async fn resume_sweep_settles_only_dangling_calls_from_the_full_cancelled_turn_window() {
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let parent_coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
        "tenant_a",
        "user_1",
        "cancel-sweep-parent",
    );
    let child_coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
        "tenant_a",
        "user_1",
        "cancel-sweep-child",
    );
    let turn_submitted = store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&child_coordinates),
            vec![verlet_history::NewEventRecord::witnessed(
                child_coordinates.clone(),
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": "turn-cancelled"}),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let request = |call_id: &str, arguments: serde_json::Value| {
        let fingerprint =
            crate::agent::tool_universe::args_fingerprint("thread_status", &arguments).unwrap();
        verlet_history::NewEventRecord::discharged(
            child_coordinates.clone(),
            verlet_history::EventKind::ToolCallRequested,
            serde_json::to_value(crate::kernel::control_decision::ToolCallRequestedPayload {
                subject: crate::kernel::control_decision::ToolCallSubject {
                    turn_id: "turn-cancelled".to_string(),
                    call_id: call_id.to_string(),
                },
                snapshot_id: "snapshot-cancelled".to_string(),
                tool_name: "thread_status".to_string(),
                arguments,
                attach_event_id: None,
                args_fingerprint: Some(fingerprint),
                holds: Vec::new(),
            })
            .unwrap(),
            verlet_history::EventProvenance {
                source_streams: vec![verlet_history::EventStreamId::for_thread(
                    &child_coordinates,
                )],
                source_event_ids: vec![turn_submitted.id],
                discharged_by: Some("test:cancel-sweep".to_string()),
                function: Some("tool_request/v1".to_string()),
                ..verlet_history::EventProvenance::default()
            },
        )
    };
    let requests = store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&child_coordinates),
            vec![
                request("call-dangling", serde_json::json!({"task_name": "old"})),
                request("call-dangling", serde_json::json!({"task_name": "new"})),
                request(
                    "call-already-completed",
                    serde_json::json!({"task_name": "completed"}),
                ),
            ],
        )
        .await
        .unwrap();
    store
        .append_with_provenance(
            &child_coordinates,
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::tool_result(
                    "call-dangling",
                    "thread_status",
                    "result persisted before the completion fact",
                    false,
                ),
            },
            verlet_history::EventProvenance {
                source_streams: vec![verlet_history::EventStreamId::for_thread(
                    &child_coordinates,
                )],
                source_event_ids: vec![requests[0].id],
                discharged_by: Some("test:partial-detached-append".to_string()),
                function: Some("session_entry_append/v1".to_string()),
                ..verlet_history::EventProvenance::default()
            },
        )
        .await
        .unwrap();
    store
        .append_events(
            &verlet_history::EventStreamId::new(format!(
                "control:{}",
                parent_coordinates.thread_id
            )),
            vec![
                verlet_history::NewEventRecord::witnessed(
                    parent_coordinates.clone(),
                    verlet_history::EventKind::ThreadJoined,
                    serde_json::json!({"malformed": "unrelated legacy join"}),
                ),
                verlet_history::NewEventRecord::discharged(
                    parent_coordinates.clone(),
                    verlet_history::EventKind::ThreadJoined,
                    serde_json::json!({
                        "child_thread_id": child_coordinates.thread_id,
                        "terminal_state": "cancelled"
                    }),
                    verlet_history::EventProvenance {
                        source_streams: vec![verlet_history::EventStreamId::for_thread(
                            &child_coordinates,
                        )],
                        source_event_ids: vec![turn_submitted.id],
                        discharged_by: Some("test:interrupt".to_string()),
                        function: Some("thread_join/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                ),
            ],
        )
        .await
        .unwrap();
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&child_coordinates),
            vec![verlet_history::NewEventRecord::discharged(
                child_coordinates.clone(),
                verlet_history::EventKind::ToolCallCompleted,
                serde_json::to_value(crate::kernel::control_decision::ToolCallCompletedPayload {
                    subject: crate::kernel::control_decision::ToolCallSubject {
                        turn_id: "turn-cancelled".to_string(),
                        call_id: "call-already-completed".to_string(),
                    },
                    snapshot_id: "snapshot-cancelled".to_string(),
                    tool_name: "thread_status".to_string(),
                    success: true,
                    args_fingerprint: serde_json::from_value::<crate::kernel::control_decision::ToolCallRequestedPayload>(
                        requests[2].payload.clone(),
                    )
                    .unwrap()
                    .args_fingerprint,
                    duration_ms: Some(7),
                    finish_order: Some(4),
                    cancellation: Some(crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace),
                })
                .unwrap(),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::for_thread(&child_coordinates)],
                    source_event_ids: vec![requests[2].id],
                    discharged_by: Some("test:late-detached-completion".to_string()),
                    function: Some("tool_result/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&child_coordinates),
            vec![
                verlet_history::NewEventRecord::witnessed(
                    child_coordinates.clone(),
                    verlet_history::EventKind::ToolCallRequested,
                    serde_json::json!({
                        "subject": {"turn_id": "unrelated-turn"},
                        "malformed": true
                    }),
                ),
                verlet_history::NewEventRecord::witnessed(
                    child_coordinates.clone(),
                    verlet_history::EventKind::ToolCallCompleted,
                    serde_json::json!({
                        "subject": {"turn_id": "unrelated-turn"},
                        "malformed": true
                    }),
                ),
            ],
        )
        .await
        .unwrap();

    let client: std::sync::Arc<dyn verlet_provider::ProviderClient> =
        std::sync::Arc::new(RecordingClient::default());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(crate::adapters::agent_loop::AgentLoopFactory::new(
            crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::OpenAIResponses,
                "openai",
                "gpt-test",
            ),
            client,
        )),
        store.clone(),
    );
    let child = host
        .load_thread_with_topology_and_metadata(
            child_coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::spawned_from(parent_coordinates.thread_id),
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
    wait_for_tool_completion_count(&store, &child_coordinates, 3).await;

    let completed = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&child_coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .filter_map(|event| {
            serde_json::from_value::<crate::kernel::control_decision::ToolCallCompletedPayload>(
                event.payload,
            )
            .ok()
        })
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
    assert!(!recovered.success);
    let latest_request = serde_json::from_value::<
        crate::kernel::control_decision::ToolCallRequestedPayload,
    >(requests[1].payload.clone())
    .unwrap();
    assert_eq!(recovered.args_fingerprint, latest_request.args_fingerprint);
    assert_eq!(
        recovered.cancellation,
        Some(crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace)
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
                verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. }
                    if tool_call_id == "call-dangling"
            ))
            .count(),
        2,
        "the stale result remains, while recovery settles the latest unfinished generation"
    );
    let _ = child;
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn runtime_persists_tool_request_and_completion_facts() {
    let registry = echo_registry("echo").await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "verlet"})),
        response_text("final reply"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let submitted = records
        .iter()
        .find(|event| {
            event.kind == verlet_history::EventKind::TurnSubmitted
                && event.origin == verlet_history::EventOrigin::Witnessed
                && event.payload["turn_id"].as_str() == Some("turn-1")
        })
        .expect("turn submission should be durable");
    let assistant_session_entry = records
        .iter()
        .find(|event| {
            event.kind == verlet_history::EventKind::SessionEntryAppended
                && event.origin == verlet_history::EventOrigin::Discharged
                && event.provenance.source_event_ids == vec![submitted.id]
        })
        .expect("assistant session entry should cite the submitted turn");
    assert_ne!(assistant_session_entry.id, submitted.id);
    let request = records
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
        .expect("tool call request should be durable");
    assert_eq!(request.origin, verlet_history::EventOrigin::Discharged);
    assert_eq!(request.payload["tool_name"].as_str(), Some("echo_search"));
    assert_eq!(request.payload["tool"].as_str(), Some("echo_search"));
    assert_eq!(
        request.payload["args_fingerprint"],
        serde_json::json!(
            crate::agent::tool_universe::args_fingerprint(
                "echo_search",
                &serde_json::json!({"input": "verlet"})
            )
            .unwrap()
        )
    );
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
        event.kind == verlet_history::EventKind::SessionEntryAppended
            && event.origin == verlet_history::EventOrigin::Discharged
            && event.provenance.source_event_ids == vec![request.id]
    }));
    let completed = records
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .expect("tool completion should be durable");
    assert_eq!(completed.origin, verlet_history::EventOrigin::Witnessed);
    assert_eq!(completed.payload["tool_name"].as_str(), Some("echo_search"));
    assert_eq!(completed.payload["success"].as_bool(), Some(true));
    assert_eq!(
        completed.payload["args_fingerprint"],
        request.payload["args_fingerprint"]
    );
    assert!(records.iter().any(|event| {
        event.kind == verlet_history::EventKind::TurnCompleted
            && event.origin == verlet_history::EventOrigin::Discharged
            && event.payload["turn_id"].as_str() == Some("turn-1")
            && !event.provenance.source_event_ids.is_empty()
    }));
}

#[tokio::test]
async fn bound_tool_controller_without_terminal_fact_denies_fail_closed() {
    let registry = echo_registry("echo").await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "verlet"})),
        response_text("handled denial"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        verlet_history::CanonicalMessage::ToolResult {
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let completed = records
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .expect("denial should still write a terminal tool result fact");
    assert_eq!(completed.payload["success"].as_bool(), Some(false));
    assert!(
        records
            .iter()
            .any(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
    );
    assert!(
        !text_messages(&thread.session_context().await.unwrap().messages)
            .iter()
            .any(|text| text == "echo:verlet"),
        "the operation should not run when a matching controller fails to decide"
    );
}

#[tokio::test]
async fn witnessed_tool_suspension_pauses_turn_without_invoking_tool() {
    let registry = echo_registry("echo").await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "verlet"})),
        response_text("should not be requested"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        verlet_history::EventKind::TurnWaiting,
    )
    .await;
    wait_for_status(&mut status, verlet_runtime_contracts::ThreadStatus::Idle).await;

    let requests = client.requests();
    assert_eq!(
        requests.len(),
        1,
        "the provider should not receive a continuation request while the tool is suspended"
    );
    let records = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        records
            .iter()
            .any(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
    );
    assert!(
        records
            .iter()
            .all(|event| event.kind != verlet_history::EventKind::ToolCallCompleted)
    );
    assert!(
        records
            .iter()
            .all(|event| event.kind != verlet_history::EventKind::TurnCompleted)
    );
    let pending = crate::kernel::control_decision::list_pending_tool_call_suspensions(
        store.as_ref(),
        &thread.context().coordinates,
    )
    .await
    .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].approval_id.as_deref(), Some("approval-1"));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn resume_tool_call_consumes_decision_and_invokes_once() {
    let registry = echo_registry("echo").await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "verlet"})),
        response_text("resumed final"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        verlet_history::EventKind::TurnWaiting,
    )
    .await;
    append_witnessed_tool_decision(
        &store,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call_1|fc_1",
        crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
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
        verlet_history::CanonicalMessage::ToolResult {
            tool_call_id,
            tool_name,
            content,
            is_error: false,
            ..
        } if tool_call_id == "call_1|fc_1"
            && tool_name == "echo_search"
            && text_from_content(content) == "echo:verlet"
    ));
    let records = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let request = records
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
        .expect("resumed call request");
    assert_eq!(
        request.payload["holds"],
        serde_json::json!([{"key": {"kind": "global"}, "access": "exclusive"}])
    );
    let completion = records
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
        .expect("resumed call completion");
    assert_eq!(completion.payload["finish_order"], 0);
    let request_payload = serde_json::from_value::<
        crate::kernel::control_decision::ToolCallRequestedPayload,
    >(request.payload.clone())
    .unwrap();
    let services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
    );
    assert!(
        crate::adapters::agent_loop::existing_tool_result_message(
            &services,
            &thread.context().coordinates,
            request.id,
            "call_1|fc_1",
            &request_payload.snapshot_id,
            request_payload.args_fingerprint.as_deref(),
        )
        .await
        .unwrap()
        .is_some(),
        "a resumed result remains reusable through its original request witness"
    );
    assert!(
        records
            .iter()
            .any(|event| event.kind == verlet_history::EventKind::TurnCompleted)
    );
    let control_records = store
        .read_events(
            &verlet_history::EventStreamId::new(format!(
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
            .any(|event| event.kind == verlet_history::EventKind::TurnResumed)
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn resumed_tool_call_keeps_the_endpoint_snapshotted_by_the_original_turn() {
    let registry = echo_registry("echo").await;
    let launch_client = std::sync::Arc::new(RecordingClient::default());
    let original_client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "verlet"})),
        response_text("original endpoint resumed"),
    ]));
    let replacement_client =
        std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
            "replacement endpoint leaked",
        )]));
    let original_endpoint = crate::adapters::agent_loop::ResolvedTurnEndpoint {
        config: crate::adapters::agent_loop::AgentLoopConfig::new(
            verlet_history::ProviderApi::OpenAIResponses,
            "original-provider",
            "original-model",
        ),
        client: original_client.clone(),
    };
    let replacement_endpoint = crate::adapters::agent_loop::ResolvedTurnEndpoint {
        config: crate::adapters::agent_loop::AgentLoopConfig::new(
            verlet_history::ProviderApi::OpenAIChatCompletions,
            "replacement-provider",
            "replacement-model",
        ),
        client: replacement_client.clone(),
    };
    let router = std::sync::Arc::new(MutableTurnEndpointRouter::new(original_endpoint));
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
                    "launch-provider",
                    "launch-model",
                ),
                launch_client.clone(),
            )
            .with_operation_registry(registry)
            .with_turn_endpoint_router(router.clone()),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "session_sticky_resume",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        verlet_history::EventKind::TurnWaiting,
    )
    .await;
    router.set(replacement_endpoint);
    append_witnessed_tool_decision(
        &store,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call_1|fc_1",
        crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
    )
    .await;
    host.resume_tool_call(
        thread.context().coordinates.thread_id,
        "turn-1",
        "call_1|fc_1",
    )
    .await
    .unwrap();
    assert_output(&mut events, "original endpoint resumed").await;

    let requests = original_client.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| {
        request.provider == "original-provider" && request.model == "original-model"
    }));
    assert!(replacement_client.requests().is_empty());
    assert!(launch_client.requests().is_empty());
}

#[tokio::test]
async fn suspended_batch_counts_as_one_round_when_the_turn_resumes() {
    let registry = echo_registry("echo").await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread_with_topology_and_metadata(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "resume-round-budget",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
            std::collections::BTreeMap::from([(
                crate::adapters::agent_loop::THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA
                    .to_string(),
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
        verlet_history::EventKind::TurnWaiting,
    )
    .await;
    append_witnessed_tool_decision(
        &store,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call-wait",
        crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
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
            &verlet_history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
            .count(),
        1
    );
}

#[tokio::test]
async fn runtime_bash_tool_advertises_and_executes_operation_shell_commands() {
    let registry = named_echo_registry("search", "search").await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named(
            "bash",
            serde_json::json!({
                "command": "command -v search && printf verlet | search"
            }),
        ),
        response_text("final reply"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let bash_config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_capability_grants(
            crate::operations::kernel_packages::verlet_threads_kernel_package().capability_grants,
        );
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(
            crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::OpenAIResponses,
                "openai",
                "gpt-test",
            ),
            provider_client,
        )
        .with_bash_tool(bash_config),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                call_id,
                output,
                success: true,
                ..
            } if call_id == "call_1|fc_1"
                && output.contains(r#""exit_code":0"#)
                && output.contains("search\\n")
                && output.contains("echo:verlet")
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
        verlet_history::CanonicalMessage::ToolResult {
            tool_call_id,
            tool_name,
            content,
            is_error: false,
            ..
        } if tool_call_id == "call_1|fc_1"
            && tool_name == "bash"
            && text_from_content(content).contains("echo:verlet")
    ));
}

#[tokio::test]
async fn runtime_bash_tool_executes_kernel_thread_operation_commands_without_agent_builtin() {
    let registry = kernel_thread_registry().await;
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named(
            "bash",
            serde_json::json!({
                "command": "if command -v agent >/dev/null 2>&1; then echo agent-present; exit 9; fi; printf '{\"task_name\":\"worker\",\"message\":\"echo child-through-bash\"}' | thread_spawn"
            }),
        ),
        response_text("spawned child from bash"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let bash_config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_capability_grants(
            crate::operations::kernel_packages::verlet_threads_kernel_package().capability_grants,
        );
    let root_factory = crate::adapters::agent_loop::AgentLoopFactory::new(
        crate::adapters::agent_loop::AgentLoopConfig::new(
            verlet_history::ProviderApi::OpenAIResponses,
            "openai",
            "gpt-test",
        ),
        provider_client,
    )
    .with_bash_tool(bash_config);
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        RootProviderChildEchoFactory {
            root: std::sync::Arc::new(root_factory),
        },
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
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
    assert!(
        bash_tool
            .description
            .contains(crate::operations::kernel_packages::THREAD_SPAWN_OPERATION)
    );
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
    let pre_hook = std::sync::Arc::new(StaticHookHandler::new(
        "pre-echo",
        crate::agent::hooks::HookEventName::PreToolUse,
        Some("echo_search"),
        crate::agent::hooks::HookHandlerOutput {
            updated_input: Some(serde_json::json!({"input": "rewritten"})),
            additional_context: Some("pre context".to_string()),
            ..crate::agent::hooks::HookHandlerOutput::default()
        },
    ));
    let post_hook = std::sync::Arc::new(StaticHookHandler::new(
        "post-echo",
        crate::agent::hooks::HookEventName::PostToolUse,
        Some("echo_search"),
        crate::agent::hooks::HookHandlerOutput {
            replacement_output: Some("hook replacement".to_string()),
            additional_context: Some("post context".to_string()),
            feedback: Some("feedback context".to_string()),
            ..crate::agent::hooks::HookHandlerOutput::default()
        },
    ));
    let pre_handler: std::sync::Arc<dyn crate::agent::hooks::HookHandler> = pre_hook.clone();
    let post_handler: std::sync::Arc<dyn crate::agent::hooks::HookHandler> = post_hook.clone();
    let hook_pipeline = std::sync::Arc::new(
        crate::agent::hooks::HookPipeline::new()
            .with_handler(pre_handler)
            .with_handler(post_handler),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "original"})),
        response_text("final reply"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
            .with_operation_registry(registry)
            .with_hook_pipeline(hook_pipeline),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::HookStarted {
                hook_id,
                event_name: crate::agent::hooks::HookEventName::PreToolUse,
                matcher: Some(matcher),
            } if hook_id == "pre-echo" && matcher == "echo_search"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::HookCompleted {
                hook_id,
                event_name: crate::agent::hooks::HookEventName::PostToolUse,
                status: crate::agent::hooks::HookRunStatus::Completed,
                ..
            } if hook_id == "post-echo"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
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
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted { call_id, .. } if call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );
    assert_eq!(
        runtime_events
            .iter()
            .filter(|event| matches!(
                event,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult { call_id, .. } if call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );

    let pre_requests = pre_hook.requests();
    assert!(matches!(
        &pre_requests[0],
        crate::agent::hooks::HookRequest::PreToolUse(request)
            if request.arguments == serde_json::json!({"input": "original"})
    ));
    let post_requests = post_hook.requests();
    assert!(matches!(
        &post_requests[0],
        crate::agent::hooks::HookRequest::PostToolUse(request)
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
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let pre_command = r#"cat >/dev/null; printf '%s' '{"updated_input":{"input":"rewritten","secret":"after-secret"}}'"#;
    let post_command = r#"cat >/dev/null; printf '%s' '{"replacement_output":"hook replacement after-secret-output"}'"#;
    let expected_pre_command_sha256 = verlet_agent::contracts::sha256_hex(pre_command.as_bytes());
    let expected_post_command_sha256 = verlet_agent::contracts::sha256_hex(post_command.as_bytes());
    let echo_provider = std::sync::Arc::new(WitnessCheckingEchoProvider {
        store: store.clone(),
        expected_command_sha256: expected_pre_command_sha256.clone(),
        seen_arguments: std::sync::Mutex::new(Vec::new()),
    });
    let kernel_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = echo_provider.clone();
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(kernel_provider),
    );
    let hook_pipeline = std::sync::Arc::new(
        crate::agent::hooks::HookPipeline::new()
            .with_command_handler(
                crate::agent::hooks::CommandHookHandler::new(
                    "pre-echo",
                    crate::agent::hooks::HookEventName::PreToolUse,
                    pre_command,
                )
                .with_matcher("echo_search"),
            )
            .with_command_handler(
                crate::agent::hooks::CommandHookHandler::new(
                    "post-echo",
                    crate::agent::hooks::HookEventName::PostToolUse,
                    post_command,
                )
                .with_matcher("echo_search"),
            ),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named(
            "echo_search",
            serde_json::json!({"input":"original","secret":"before-secret"}),
        ),
        response_text("final reply"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
                .with_tool_router(router)
                .with_hook_pipeline(hook_pipeline),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        Some(
            verlet_agent::contracts::sha256_hex("tool original before-secret-output".as_bytes())
                .as_str()
        )
    );
    assert_eq!(
        post_payload["tool_output"]["after_sha256"].as_str(),
        Some(
            verlet_agent::contracts::sha256_hex("hook replacement after-secret-output".as_bytes())
                .as_str()
        )
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
    let block_hook = std::sync::Arc::new(StaticHookHandler::new(
        "block-echo",
        crate::agent::hooks::HookEventName::PreToolUse,
        Some("echo_search"),
        crate::agent::hooks::HookHandlerOutput {
            should_block: true,
            block_reason: Some("blocked by hook".to_string()),
            additional_context: Some("block context".to_string()),
            ..crate::agent::hooks::HookHandlerOutput::default()
        },
    ));
    let hook_handler: std::sync::Arc<dyn crate::agent::hooks::HookHandler> = block_hook.clone();
    let hook_pipeline =
        std::sync::Arc::new(crate::agent::hooks::HookPipeline::new().with_handler(hook_handler));
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "original"})),
        response_text("final reply"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
            .with_operation_registry(registry)
            .with_hook_pipeline(hook_pipeline),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::HookCompleted {
                hook_id,
                event_name: crate::agent::hooks::HookEventName::PreToolUse,
                status: crate::agent::hooks::HookRunStatus::Blocked,
                message: Some(message),
                ..
            } if hook_id == "block-echo" && message == "blocked by hook"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
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
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted { call_id, .. } if call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );
    assert_eq!(
        runtime_events
            .iter()
            .filter(|event| matches!(
                event,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult { call_id, .. } if call_id == "call_1|fc_1"
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
    let block_store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let block_command = r#"cat >/dev/null; printf '%s' '{"should_block":true,"block_reason":"blocked by hook secret","additional_context":"block context secret"}'"#;
    let block_hook_pipeline = std::sync::Arc::new(
        crate::agent::hooks::HookPipeline::new().with_command_handler(
            crate::agent::hooks::CommandHookHandler::new(
                "block-echo",
                crate::agent::hooks::HookEventName::PreToolUse,
                block_command,
            )
            .with_matcher("echo_search"),
        ),
    );
    let block_client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "original"})),
        response_text("final reply"),
    ]));
    let block_provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> =
        block_client.clone();
    let mut block_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    block_config.max_tokens = 128;
    let block_host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(block_config, block_provider_client)
                .with_operation_registry(echo_registry("echo").await)
                .with_hook_pipeline(block_hook_pipeline),
        ),
        block_store.clone(),
    );
    let block_thread = block_host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_block"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        Some(verlet_agent::contracts::sha256_hex(block_command.as_bytes()).as_str())
    );
    assert_mutated_fields(block_payload, &["additional_contexts", "should_block"]);
    assert_payload_omits_values(
        block_payload,
        &["blocked by hook secret", "block context secret"],
    );

    let stop_store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let stop_command =
        r#"cat >/dev/null; printf '%s' '{"should_stop":true,"stop_reason":"stop secret"}'"#;
    let stop_hook_pipeline = std::sync::Arc::new(
        crate::agent::hooks::HookPipeline::new().with_command_handler(
            crate::agent::hooks::CommandHookHandler::new(
                "stop-turn",
                crate::agent::hooks::HookEventName::UserPromptSubmit,
                stop_command,
            ),
        ),
    );
    let stop_client = std::sync::Arc::new(RecordingClient::with_responses(vec![]));
    let stop_provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> =
        stop_client.clone();
    let stop_host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                crate::adapters::agent_loop::AgentLoopConfig::new(
                    verlet_history::ProviderApi::OpenAIResponses,
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
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_stop"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
        Some(verlet_agent::contracts::sha256_hex(stop_command.as_bytes()).as_str())
    );
    assert_mutated_fields(stop_payload, &["should_stop"]);
    assert_payload_omits_values(stop_payload, &["stop secret"]);

    let observe_store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let observe_hook_pipeline = std::sync::Arc::new(
        crate::agent::hooks::HookPipeline::new().with_command_handler(
            crate::agent::hooks::CommandHookHandler::new(
                "observe-echo",
                crate::agent::hooks::HookEventName::PreToolUse,
                "cat >/dev/null",
            )
            .with_matcher("echo_search"),
        ),
    );
    let observe_client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "observed"})),
        response_text("final reply"),
    ]));
    let observe_provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> =
        observe_client.clone();
    let mut observe_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    observe_config.max_tokens = 128;
    let observe_host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(
            crate::adapters::agent_loop::AgentLoopFactory::new(
                observe_config,
                observe_provider_client,
            )
            .with_operation_registry(echo_registry("echo").await)
            .with_hook_pipeline(observe_hook_pipeline),
        ),
        observe_store.clone(),
    );
    let observe_thread = observe_host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "session_observe",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    let kernel_provider = std::sync::Arc::new(TurnContextRecordingKernelToolProvider::new());
    let tool_provider: std::sync::Arc<
        dyn crate::agent::agent_tool_router::AgentKernelToolProvider,
    > = kernel_provider.clone();
    let router = std::sync::Arc::new(
        crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(tool_provider),
    );
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
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
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
            .with_tool_router(router),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit_turn(
        thread.context().coordinates.thread_id,
        "turn-context-1",
        crate::kernel::runtime_host::turn::TurnInput::text("record context")
            .with_cwd("/tmp/verlet-turn")
            .with_permission_profile("workspace-write")
            .with_metadata("source", "agent-loop-test"),
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
        Some("agent-loop-test")
    );
    assert_eq!(
        snapshot.budget.max_tool_rounds,
        Some(crate::adapters::agent_loop::MAX_TOOL_ROUTER_ROUNDS)
    );
    assert_eq!(snapshot.budget.max_output_tokens, Some(128));
    assert!(!snapshot.cancellation_requested);
}

#[tokio::test]
async fn runtime_routes_thread_spawn_operation_through_kernel_dispatch() {
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named(
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "message": "echo child-through-tool",
                "dispatch_id": "model-supplied-id-must-not-win",
            }),
        ),
        response_text("spawned child"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let root_factory = crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
        .with_tool_router(std::sync::Arc::new(kernel_thread_router().await))
        .with_thread_spawn_agent_resolver(std::sync::Arc::new(StaticThreadSpawnAgentResolver));
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        RootProviderChildEchoFactory {
            root: std::sync::Arc::new(root_factory),
        },
    ));
    let store = host.runtime_store();
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
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
            .any(|tool| tool.name == crate::operations::kernel_packages::THREAD_SPAWN_OPERATION)
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
        verlet_history::EventKind::ThreadSpawnRequested,
    )
    .await;
    let requested_payload: verlet_history::ThreadSpawnRequestedPayload =
        serde_json::from_value(requested.payload.clone()).unwrap();
    assert_eq!(requested_payload.correlation_id, "call_1|fc_1");
    assert_eq!(requested_payload.child_agent_ref, CHILD_AGENT_REF);
    let spawned = wait_for_control_event(
        store.as_ref(),
        &thread.context().coordinates,
        verlet_history::EventKind::ThreadSpawned,
    )
    .await;
    let spawned_payload: verlet_history::ThreadSpawnedPayload =
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
    assert!(spawned_payload.granted.is_empty());
    assert!(spawned_payload.inputs_hash.starts_with("sha256:"));

    let joined = wait_for_control_event(
        store.as_ref(),
        &thread.context().coordinates,
        verlet_history::EventKind::ThreadJoined,
    )
    .await;
    let joined_payload: verlet_history::ThreadJoinedPayload =
        serde_json::from_value(joined.payload.clone()).unwrap();
    assert_eq!(
        joined_payload.child_thread_id,
        spawned_payload.child_thread_id
    );
    assert_eq!(joined_payload.spawned_event_id, spawned.id);
    assert_eq!(
        joined_payload.terminal_state,
        verlet_history::ThreadTerminalState::Completed
    );

    let parent_session = thread.session_context().await.unwrap();
    assert!(
        text_messages(&parent_session.messages)
            .iter()
            .any(|text| text.contains("cooldis.thread_spawn"))
    );
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn concurrent_thread_mounts_isolate_bash_kernel_dispatch_on_shared_registry() {
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> =
        std::sync::Arc::new(BashMandateListClient {
            barrier: std::sync::Arc::new(tokio::sync::Barrier::new(2)),
        });
    let mut config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let registry = kernel_schedule_registry().await;
    let capability_grants =
        crate::operations::kernel_packages::verlet_schedule_kernel_package().capability_grants;
    let factory = crate::adapters::agent_loop::AgentLoopFactory::new(config, provider_client)
        .with_bash_tool(
            crate::capabilities::execution::VirtualBashRuntimeConfig::default()
                .with_operation_registry(registry)
                .with_capability_grants(capability_grants),
        );
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(factory));

    let (thread_a, thread_b) = tokio::join!(
        host.start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "overlay-a",),
            verlet_runtime_contracts::ThreadTopology::root(),
        ),
        host.start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "overlay-b",),
            verlet_runtime_contracts::ThreadTopology::root(),
        ),
    );
    let thread_a = thread_a.unwrap();
    let thread_b = thread_b.unwrap();
    let thread_a_id = thread_a.context().coordinates.thread_id.to_string();
    let thread_b_id = thread_b.context().coordinates.thread_id.to_string();
    let mut status_a = thread_a.subscribe_status();
    let mut status_b = thread_b.subscribe_status();
    tokio::join!(
        wait_for_status(&mut status_a, verlet_runtime_contracts::ThreadStatus::Idle),
        wait_for_status(&mut status_b, verlet_runtime_contracts::ThreadStatus::Idle),
    );
    let mut events_a = thread_a.subscribe_events();
    let mut events_b = thread_b.subscribe_events();

    let (submit_a, submit_b) = tokio::join!(
        host.submit(
            thread_a.context().coordinates.thread_id,
            "turn-a",
            "list mandates",
        ),
        host.submit(
            thread_b.context().coordinates.thread_id,
            "turn-b",
            "list mandates",
        ),
    );
    submit_a.unwrap();
    submit_b.unwrap();
    let (runtime_events_a, runtime_events_b) = tokio::join!(
        assert_output_with_runtime_events(&mut events_a, "listed mandates"),
        assert_output_with_runtime_events(&mut events_b, "listed mandates"),
    );

    let output_a = successful_tool_output(&runtime_events_a);
    let output_b = successful_tool_output(&runtime_events_b);
    assert!(output_a.contains(&thread_a_id));
    assert!(!output_a.contains(&thread_b_id));
    assert!(output_b.contains(&thread_b_id));
    assert!(!output_b.contains(&thread_a_id));

    host.shutdown_all().await.unwrap();
}

fn successful_tool_output(
    events: &[crate::kernel::runtime_host::runtime_events::RuntimeEventKind],
) -> &str {
    events
        .iter()
        .find_map(|event| match event {
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                output,
                success: true,
                ..
            } => Some(output.as_str()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("successful tool result in {events:#?}"))
}

async fn kernel_thread_registry()
-> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let package = crate::operations::kernel_packages::verlet_threads_kernel_package();
    let mut registration = verlet_operations::operation_registry::KernelOperationRegistration::new(
        crate::operations::kernel_packages::VERLET_THREADS_PACKAGE,
        package.manifest.clone(),
    )
    .with_capability_grants(package.capability_grants.clone());
    registration.metadata.insert(
        crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND.to_string(),
        serde_json::Value::String(
            crate::operations::kernel_packages::KERNEL_RUNTIME_KIND.to_string(),
        ),
    );
    registry.register_kernel(registration).await.unwrap();
    registry
}

async fn kernel_thread_router() -> crate::agent::agent_tool_router::AgentToolRouter {
    let registry = kernel_thread_registry().await;
    crate::agent::agent_tool_router::AgentToolRouter::new(registry).with_tool_aliases(vec![
        crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: crate::operations::kernel_packages::THREAD_SPAWN_OPERATION.to_string(),
            registered_name: crate::operations::kernel_packages::VERLET_THREADS_PACKAGE.to_string(),
            operation_name: crate::operations::kernel_packages::THREAD_SPAWN_OPERATION.to_string(),
            attach_event_id: None,
            surface: None,
        },
    ])
}

async fn kernel_schedule_registry()
-> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let package = crate::operations::kernel_packages::verlet_schedule_kernel_package();
    let mut registration = verlet_operations::operation_registry::KernelOperationRegistration::new(
        crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE,
        package.manifest.clone(),
    )
    .with_capability_grants(package.capability_grants.clone());
    registration.metadata.insert(
        crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND.to_string(),
        serde_json::Value::String(
            crate::operations::kernel_packages::KERNEL_RUNTIME_KIND.to_string(),
        ),
    );
    registry.register_kernel(registration).await.unwrap();
    registry
}

async fn append_tool_controller_bind_receipt(
    store: &verlet_history::InMemorySessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    tool_name: &str,
) {
    let receipt = crate::agent::manifest_bind::AgentManifestBindReceipt {
        ref_uri: "agent://test/controller".to_string(),
        manifest_hash: "snapshot-controller".to_string(),
        model_profile_id: "default".to_string(),
        model_profile_origin: None,
        provider_id: "test".to_string(),
        model_id: "model".to_string(),
        tool_ids: Vec::new(),
        operation_bindings: Vec::new(),
        skill_packages: Vec::new(),
        skill_discovery: None,
        static_context_segments: Vec::new(),
        tool_universes: Vec::new(),
        couplings: vec![crate::agent::manifest_bind::AgentManifestCouplingBinding {
            id: "tool_gate".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::ToolCallRequested.to_string(),
            trigger_match: std::collections::BTreeMap::from([(
                "tool".to_string(),
                serde_json::json!(tool_name),
            )]),
            source_streams: vec!["thread".to_string()],
            source_kinds: vec![verlet_history::EventKind::ToolCallRequested.to_string()],
            sink_stream: "control".to_string(),
            sink_kinds: vec![verlet_history::EventKind::ToolCallDecision.to_string()],
            function_ref: "op://policy/tool-gate@sha256:test".to_string(),
            artifact_hash: "test".to_string(),
            operation_name: Some("tool_gate".to_string()),
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget::default(),
            config_hash: "config".to_string(),
        }],
        effective_runtime: verlet_agent::manifest_schema::AgentManifestRuntimeDefaults::default(),
        overridden_keys: Vec::new(),
        placement: None,
        placement_origin: None,
        workspace: None,
        workspace_origin: None,
    };
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ManifestBindCompleted,
                serde_json::to_value(receipt).unwrap(),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::for_thread(coordinates)],
                    discharged_by: Some("binder:manifest".to_string()),
                    function: Some("bind/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
}

async fn append_manifest_runtime_grace(
    store: &verlet_history::InMemorySessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    cancellation_grace_ms: u64,
) {
    let receipt = crate::agent::manifest_bind::AgentManifestBindReceipt {
        ref_uri: "agent://test/interruption".to_string(),
        manifest_hash: "snapshot-interruption".to_string(),
        model_profile_id: "default".to_string(),
        model_profile_origin: None,
        provider_id: "test".to_string(),
        model_id: "model".to_string(),
        tool_ids: Vec::new(),
        operation_bindings: Vec::new(),
        skill_packages: Vec::new(),
        skill_discovery: None,
        static_context_segments: Vec::new(),
        tool_universes: Vec::new(),
        couplings: Vec::new(),
        effective_runtime: verlet_agent::manifest_schema::AgentManifestRuntimeDefaults {
            cancellation_grace_ms: Some(cancellation_grace_ms),
            ..verlet_agent::manifest_schema::AgentManifestRuntimeDefaults::default()
        },
        overridden_keys: vec!["cancellation_grace_ms".to_string()],
        placement: None,
        placement_origin: None,
        workspace: None,
        workspace_origin: None,
    };
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ManifestBindCompleted,
                serde_json::to_value(receipt).unwrap(),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::for_thread(coordinates)],
                    discharged_by: Some("binder:manifest".to_string()),
                    function: Some("bind/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
}

async fn append_witnessed_tool_suspension(
    store: &verlet_history::InMemorySessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    snapshot_id: &str,
    turn_id: &str,
    call_id: &str,
    approval_id: &str,
) {
    store
        .append_events(
            &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::ToolCallSuspended,
                serde_json::to_value(crate::kernel::control_decision::ToolCallSuspendedPayload {
                    subject: crate::kernel::control_decision::ToolCallSubject {
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
    store: &verlet_history::InMemorySessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    snapshot_id: &str,
    turn_id: &str,
    call_id: &str,
    outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload,
) {
    store
        .append_events(
            &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::ToolCallDecision,
                serde_json::to_value(crate::kernel::control_decision::ToolCallDecisionPayload {
                    subject: crate::kernel::control_decision::ToolCallSubject {
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
    store: &verlet_history::InMemorySessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    kind: verlet_history::EventKind,
) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        let mut records = store
            .read_events(
                &verlet_history::EventStreamId::for_thread(coordinates),
                None,
            )
            .await
            .unwrap();
        records.extend(
            store
                .read_events(
                    &verlet_history::EventStreamId::new(format!(
                        "control:{}",
                        coordinates.thread_id
                    )),
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
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

async fn wait_for_tool_call_completion(
    store: &verlet_history::InMemorySessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    call_id: &str,
) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        let records = store
            .read_events(
                &verlet_history::EventStreamId::for_thread(coordinates),
                None,
            )
            .await
            .unwrap();
        if records.iter().any(|event| {
            event.kind == verlet_history::EventKind::ToolCallCompleted
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

async fn wait_for_control_event<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    kind: verlet_history::EventKind,
) -> verlet_history::EventRecord {
    let stream_id =
        verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        let records = store.read_events(&stream_id, None).await.unwrap();
        if let Some(record) = records.into_iter().find(|event| event.kind == kind) {
            return record;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for control event kind {kind}"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

async fn wait_for_status(
    status: &mut tokio::sync::watch::Receiver<verlet_runtime_contracts::ThreadStatus>,
    expected: verlet_runtime_contracts::ThreadStatus,
) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        if *status.borrow() == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for status {expected:?}"
        );
        tokio::time::timeout(tokio::time::Duration::from_secs(30), status.changed())
            .await
            .ok();
    }
}

#[tokio::test]
async fn runtime_returns_error_tool_result_for_unknown_tool_and_continues() {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("missing_tool", serde_json::json!({})),
        response_text("handled missing tool"),
    ]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory_with_registry(
        provider_client,
        registry,
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
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
        verlet_history::CanonicalMessage::ToolResult {
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
    let client = std::sync::Arc::new(StreamingClient::new(vec![vec![
        verlet_provider::ProviderStreamEvent::TextDelta {
            text: "COOL".to_string(),
        },
        verlet_provider::ProviderStreamEvent::ToolCallDelta {
            id: "call_1".to_string(),
            name: Some("bash".to_string()),
            arguments_delta: "{\"command\"".to_string(),
        },
        verlet_provider::ProviderStreamEvent::ToolCallDelta {
            id: "call_1".to_string(),
            name: None,
            arguments_delta: ":\"pwd\"}".to_string(),
        },
        verlet_provider::ProviderStreamEvent::Usage {
            usage: verlet_history::CanonicalUsage {
                input_tokens: 5,
                output_tokens: 6,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 1,
            },
        },
        verlet_provider::ProviderStreamEvent::Done {
            stop_reason: verlet_history::CanonicalStopReason::ToolUse,
        },
    ]]));
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = client.clone();
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(streaming_runtime_factory(provider_client));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "stream")
        .await
        .unwrap();
    let (assistant, runtime_events) = assert_assistant_with_runtime_events(&mut events).await;

    assert!(runtime_events.iter().any(|event| {
        matches!(event, crate::kernel::runtime_host::runtime_events::RuntimeEventKind::TextDelta { text } if text == "COOL")
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
                call_id,
                name,
                ..
            } if call_id == "call_1" && name == "bash"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Usage { usage }
                if usage.input_tokens == 5
                    && usage.output_tokens == 6
                    && usage.cache_read_input_tokens == 1
        )
    }));

    match assistant {
        verlet_history::CanonicalMessage::Assistant {
            content,
            usage,
            stop_reason,
            ..
        } => {
            assert_eq!(stop_reason, verlet_history::CanonicalStopReason::ToolUse);
            assert_eq!(usage.input_tokens, 5);
            assert_eq!(usage.output_tokens, 6);
            assert!(matches!(
                &content[0],
                verlet_history::CanonicalContent::Text { text, .. } if text == "COOL"
            ));
            assert!(matches!(
                &content[1],
                verlet_history::CanonicalContent::ToolCall { id, name, arguments }
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
    let path = temp_db_path("verlet-provider-resume");
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let checkpoint = {
        let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
            "first reply",
        )]));
        let store = std::sync::Arc::new(
            verlet_history_sqlite::SqliteSessionStore::open(&path)
                .await
                .unwrap(),
        );
        let host =
            crate::kernel::runtime_host::RuntimeHost::with_session_store(factory(client), store);
        let thread = host
            .start_thread(
                coordinates.clone(),
                verlet_runtime_contracts::ThreadTopology::root(),
            )
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
                std::collections::BTreeMap::new(),
            )
            .await
            .unwrap();
        host.shutdown_thread(thread.context().coordinates.thread_id)
            .await
            .unwrap();
        checkpoint
    };

    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
        "second reply",
    )]));
    let store = std::sync::Arc::new(
        verlet_history_sqlite::SqliteSessionStore::open(&path)
            .await
            .unwrap(),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory(std::sync::Arc::clone(&client)),
        store,
    );
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
    let path = temp_db_path("verlet-provider-context-receipt");
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    {
        let client = std::sync::Arc::new(RecordingClient::with_responses(vec![response_text(
            "first reply",
        )]));
        let store = std::sync::Arc::new(
            verlet_history_sqlite::SqliteSessionStore::open(&path)
                .await
                .unwrap(),
        );
        let host =
            crate::kernel::runtime_host::RuntimeHost::with_session_store(factory(client), store);
        let thread = host
            .start_thread(
                coordinates.clone(),
                verlet_runtime_contracts::ThreadTopology::root(),
            )
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

    let reopened = verlet_history_sqlite::SqliteSessionStore::open(&path)
        .await
        .unwrap();
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let events = reopened.read_events(&stream_id, None).await.unwrap();
    let session_events = events
        .iter()
        .filter(|event| {
            event.kind == verlet_history::EventKind::SessionEntryAppended
                && event
                    .payload
                    .get("runtime_kind")
                    .and_then(serde_json::Value::as_str)
                    != Some("thread_started")
        })
        .collect::<Vec<_>>();
    let compile_events = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ContextCompileCompleted)
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
    let client = std::sync::Arc::new(RecordingClient::with_responses(vec![
        response_text("root reply"),
        response_text("parent reply"),
        response_text("fork reply"),
    ]));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory(std::sync::Arc::clone(&client)),
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
    );
    let parent = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
            std::collections::BTreeMap::new(),
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
    let client = std::sync::Arc::new(ScriptedClient::new(vec![
        ScriptedResponse::Pending,
        ScriptedResponse::Response(response_text("after reply")),
    ]));
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory(client));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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
    let client = std::sync::Arc::new(ScriptedClient::new(vec![
        ScriptedResponse::Pending,
        ScriptedResponse::Response(response_text("queued reply")),
    ]));
    let host = crate::kernel::runtime_host::RuntimeHost::new(runtime_factory(client));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            verlet_runtime_contracts::ThreadTopology::root(),
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

    let signal = assert_signal(
        &mut events,
        verlet_runtime_contracts::ThreadSignalKind::UserQueue,
    )
    .await;
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

async fn echo_registry(
    name: &str,
) -> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
    named_echo_registry(name, "search").await
}

async fn named_echo_registry(
    name: &str,
    operation_name: &str,
) -> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let wasm = wat::parse_str(echo_operation_guest("echo", operation_name))
        .expect("echo operation fixture should compile");
    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                name,
                verlet_wasm::WasmRuntimeArtifact::bytes(wasm),
            ),
        )
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
              (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__verlet_call_operation__")
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

fn is_canonical_message_entry(entry: &verlet_history::SessionEntry) -> bool {
    matches!(entry.kind, verlet_history::SessionEntryKind::Message { .. })
}

async fn turn_failed_events(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> Vec<verlet_history::EventRecord> {
    store
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::TurnFailed)
        .collect()
}

fn temp_db_path(prefix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}.sqlite3"))
}

async fn assert_output(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected: &str,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } = event {
            assert_eq!(text, expected);
            return;
        }
    }
}

async fn assert_stopped(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { .. } => return,
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                panic!("thread failed: {message}")
            }
            _ => {}
        }
    }
}

async fn assert_output_with_runtime_events(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected: &str,
) -> Vec<crate::kernel::runtime_host::runtime_events::RuntimeEventKind> {
    let mut runtime_events = Vec::new();
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                runtime_events.push(event.kind)
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
                assert_eq!(text, expected);
                return runtime_events;
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                panic!("thread failed: {message}")
            }
            _ => {}
        }
    }
}

async fn assert_completed_terminal(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. }
                if matches!(
                    event.kind,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                        state: verlet_runtime_contracts::RuntimeTerminalState::Completed,
                    }
                ) =>
            {
                return;
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                panic!("thread failed: {message}")
            }
            _ => {}
        }
    }
}

fn assert_mutated_fields(payload: &serde_json::Value, expected: &[&str]) {
    let fields = payload["mutated_fields"]
        .as_array()
        .expect("mutated_fields should be an array")
        .iter()
        .map(|field| field.as_str().expect("mutated field should be a string"))
        .collect::<Vec<_>>();
    assert_eq!(fields, expected);
}

fn assert_payload_omits_values(payload: &serde_json::Value, forbidden_values: &[&str]) {
    let encoded = serde_json::to_string(payload).unwrap();
    for forbidden in forbidden_values {
        assert!(
            !encoded.contains(forbidden),
            "witness payload leaked forbidden value {forbidden:?}: {encoded}"
        );
    }
}

async fn assert_failed_with_runtime_events(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected_message_fragment: &str,
) -> Vec<crate::kernel::runtime_host::runtime_events::RuntimeEventKind> {
    let mut runtime_events = Vec::new();
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                runtime_events.push(event.kind)
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
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
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected_trigger: crate::kernel::compaction::CompactionTrigger,
    expected_summary: &str,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime {
                event:
                    crate::kernel::runtime_host::runtime_events::RuntimeEvent {
                        kind:
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Compaction {
                                trigger,
                                summary,
                            },
                        ..
                    },
                ..
            } => {
                assert_eq!(trigger, expected_trigger);
                assert_eq!(summary, expected_summary);
                return;
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                panic!("thread failed: {message}")
            }
            _ => {}
        }
    }
}

async fn assert_user_mirror(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected: &str,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror {
            entry, ..
        } = event
            && let verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::User { content, .. },
            } = entry.kind
        {
            let text = content
                .iter()
                .find_map(|content| match content {
                    verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or_default();
            assert_eq!(text, expected);
            return;
        }
    }
}

async fn assert_assistant_mirror(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> verlet_history::CanonicalMessage {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror {
            entry,
            ..
        } = event
        {
            if let verlet_history::SessionEntryKind::Message { message } = entry.kind {
                if matches!(message, verlet_history::CanonicalMessage::Assistant { .. }) {
                    return message;
                }
            }
        }
    }
}

async fn assert_assistant_with_runtime_events(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> (
    verlet_history::CanonicalMessage,
    Vec<crate::kernel::runtime_host::runtime_events::RuntimeEventKind>,
) {
    let mut runtime_events = Vec::new();
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                runtime_events.push(event.kind)
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror {
                entry,
                ..
            } => {
                if let verlet_history::SessionEntryKind::Message { message } = entry.kind
                    && matches!(message, verlet_history::CanonicalMessage::Assistant { .. })
                {
                    return (message, runtime_events);
                }
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                panic!("thread failed: {message}")
            }
            _ => {}
        }
    }
}

async fn assert_cancelled(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected: &str,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { reason, .. } =
            event
        {
            assert_eq!(reason, expected);
            return;
        }
    }
}

fn drain_has_cancelled(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> bool {
    let mut cancelled = false;
    while let Ok(event) = events.try_recv() {
        cancelled |= matches!(
            event,
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { .. }
        );
    }
    cancelled
}

async fn wait_for_tool_completion_count(
    store: &verlet_history::InMemorySessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    expected: usize,
) {
    for _ in 0..100 {
        let count = store
            .read_events(
                &verlet_history::EventStreamId::for_thread(coordinates),
                None,
            )
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
            .count();
        if count == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("tool completion count did not reach {expected}");
}

async fn assert_signal(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected: verlet_runtime_contracts::ThreadSignalKind,
) -> verlet_runtime_contracts::ThreadSignal {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal { signal, .. } = event
            && signal.kind == expected
        {
            return signal;
        }
    }
}
