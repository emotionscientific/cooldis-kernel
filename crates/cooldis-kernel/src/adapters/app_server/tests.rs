use super::connection::*;
use super::subscriptions::*;
use super::threads::{
    AppServerTurnState, app_server_turns_from_session_entries, append_bound_agent_metadata,
    apply_manifest_operation_grants, apply_manifest_runtime_metadata, finalize_turn_payload,
    record_bound_agent_receipts, thread_manifest_operation_bindings,
    thread_manifest_skill_discovery, thread_manifest_skill_packages, thread_metadata_thinking,
    turn_input_from_values, user_input_preview,
};
use super::*;
use crate::{
    CHANNEL_EMIT_OPERATION, COOLDIS_NOTIFY_PACKAGE, COOLDIS_PROCESS_PACKAGE,
    COOLDIS_SCHEDULE_PACKAGE, COOLDIS_THREADS_PACKAGE, EffectClass, EventKind, EventOrigin,
    KERNEL_RUNTIME_KIND, LocalOperationRegistry, LocalSkillRegistry, MANDATE_LIST_OPERATION,
    MANDATE_REVOKE_OPERATION, MANDATE_START_OPERATION, NOTIFY_PREVIEW_OPERATION,
    OPERATION_METADATA_RUNTIME_KIND, PROCESS_EXEC_OPERATION, PROCESS_POLL_OPERATION,
    PROCESS_TERMINATE_OPERATION, PROCESS_WRITE_OPERATION, ProviderError,
    PublishSkillPackageRequest, PublishedOperationSource, SCHEDULE_MANAGE_CAPABILITY,
    SCHEDULE_READ_CAPABILITY, THREAD_CANCEL_OPERATION, THREAD_SPAWN_OPERATION,
    THREAD_STATUS_OPERATION, THREAD_SUBMIT_OPERATION, THREAD_WAIT_OPERATION,
    THREADS_CONTROL_CAPABILITY, THREADS_READ_CAPABILITY, THREADS_SPAWN_CAPABILITY, TOOL_CALL_TOOL,
    TOOL_DESCRIBE_TOOL, TOOL_SEARCH_TOOL, ThinkingConfig, ThinkingEffort,
};

#[test]
fn jsonrpc_decodes_initialize_without_jsonrpc_field() {
    let raw = r#"{"id":"initialize","method":"initialize","params":{"clientInfo":{"name":"codex","title":null,"version":"test"},"capabilities":{"experimentalApi":true,"requestAttestation":false}}}"#;
    let message: JsonRpcMessage = serde_json::from_str(raw).unwrap();
    match message {
        JsonRpcMessage::Request(request) => {
            assert_eq!(request.id, RequestId::String("initialize".to_string()));
            assert_eq!(request.method, "initialize");
            let params: InitializeParams = parse_params(request.params).unwrap();
            assert_eq!(params.client_info.name, "codex");
            assert!(params.capabilities.unwrap().experimental_api);
        }
        other => panic!("expected request, got {other:?}"),
    }
}

#[test]
fn jsonrpc_encodes_notification_shape() {
    let notification = JsonRpcMessage::Notification(JsonRpcNotification {
        method: "item/agentMessage/delta".to_string(),
        params: Some(json!({
            "threadId": "thread",
            "turnId": "turn",
            "itemId": "item",
            "delta": "hello",
        })),
    });
    let encoded = serde_json::to_value(notification).unwrap();
    assert_eq!(
        encoded,
        json!({
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "thread",
                "turnId": "turn",
                "itemId": "item",
                "delta": "hello",
            },
        })
    );
}

fn operation_record_by_name<'a>(records: &'a [Value], name: &str) -> &'a Value {
    records
        .iter()
        .find(|record| record["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("expected operation record {name}"))
}

fn manifest_operation_binding_by_name<'a>(payload: &'a Value, name: &str) -> &'a Value {
    payload["operation_bindings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|binding| binding["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("expected manifest operation binding {name}"))
}

fn event_by_kind(events: &[crate::EventRecord], kind: crate::EventKind) -> &crate::EventRecord {
    events
        .iter()
        .find(|event| event.kind == kind)
        .unwrap_or_else(|| panic!("expected {} event", kind.as_str()))
}

#[test]
fn thinking_params_parse_supported_shapes() {
    let effort: ThreadStartParams = parse_params(Some(json!({
        "thinking": { "type": "effort", "effort": "xhigh" },
    })))
    .unwrap();
    assert_eq!(
        effort.thinking,
        Some(ThinkingConfig::Effort {
            effort: ThinkingEffort::XHigh
        })
    );

    let budget: ThreadStartParams = parse_params(Some(json!({
        "thinking": { "type": "budget", "budgetTokens": 2048 },
    })))
    .unwrap();
    assert_eq!(
        budget.thinking,
        Some(ThinkingConfig::Budget {
            budget_tokens: 2048
        })
    );
    let zero_budget: ThreadStartParams = parse_params(Some(json!({
        "thinking": { "type": "budget", "budgetTokens": 0 },
    })))
    .unwrap();
    assert_eq!(
        zero_budget.thinking,
        Some(ThinkingConfig::Budget { budget_tokens: 0 })
    );

    let disabled: TurnStartParams = parse_params(Some(json!({
        "threadId": "thread-1",
        "input": [],
        "thinking": { "type": "disabled" },
    })))
    .unwrap();
    assert_eq!(disabled.thinking, Some(ThinkingConfig::Disabled));
}

#[test]
fn thinking_params_reject_malformed_shapes() {
    let malformed = [
        json!({ "type": "effort", "effort": "x_high" }),
        json!({ "type": "budget" }),
        json!({ "type": "budget", "budgetTokens": -1 }),
        json!({ "type": "budget", "budgetTokens": (u64::from(u32::MAX) + 1) }),
        json!({ "type": "budget", "budgetTokens": 1.5 }),
        json!({ "type": "mystery" }),
        json!("low"),
    ];

    for thinking in malformed {
        let err = parse_params::<ThreadStartParams>(Some(json!({ "thinking": thinking })))
            .expect_err("thread/start should reject malformed thinking");
        assert_eq!(err.code, -32602);
    }
}

#[tokio::test]
async fn app_server_turn_start_records_surface_admission_before_execution() {
    use crate::EventStore;

    let app = test_app().await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id.clone(),
                "input": [{ "type": "text", "text": "admission rpc", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    let turn_id = turn["turn"]["id"].as_str().unwrap().to_string();
    wait_for_turn_completed_notification(&mut outbound_rx, &thread_id, &turn_id).await;

    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(&thread_id).unwrap())
        .await
        .unwrap()
        .unwrap();
    let session_store = crate::SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let control_events = session_store
        .read_events(&crate::control_stream_id(&lifecycle.coordinates), None)
        .await
        .unwrap();
    let thread_events = session_store
        .read_events(
            &crate::EventStreamId::for_thread(&lifecycle.coordinates),
            None,
        )
        .await
        .unwrap();
    let admission = crate::kernel::admission::assert_admission_precedes_turn_records(
        &control_events,
        &thread_events,
    );
    assert_eq!(
        admission.payload["schema"],
        crate::EventKind::AdmissionDecided.payload_schema_id()
    );
    assert_eq!(admission.payload["route_id"], "surface:app-server-rpc");
    assert_eq!(admission.payload["decision"], "queue");
    assert_eq!(admission.payload["admissible"], json!(["queue"]));
    let source_ids = admission.payload["source_ingress_event_ids"]
        .as_array()
        .unwrap();
    assert_eq!(source_ids.len(), 1);
    let source_id = source_ids[0].as_str().unwrap();
    assert!(control_events.iter().any(|event| {
        event.kind == crate::EventKind::IoIngressReceived && event.id.to_string() == source_id
    }));
    assert_eq!(admission.origin, EventOrigin::Discharged);
    assert_eq!(
        admission.provenance.discharged_by.as_deref(),
        Some("policy:admission_surface:app-server-rpc")
    );
    assert_eq!(
        admission.provenance.function.as_deref(),
        Some("surface_admission/v1")
    );
    assert_eq!(
        admission.provenance.config_hash.as_deref(),
        admission.payload["policy_hash"].as_str()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_new_thread_turn_burst_surfaces_no_history_lock_errors() {
    const THREAD_STARTS: usize = 200;
    let root = unique_test_root("app-server-history-contention");
    let app = test_app_at_root(&root).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(THREAD_STARTS));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..THREAD_STARTS {
        let app = app.clone();
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            let (connection, _outbound_rx) = test_connection(app.clone());
            initialize_for_test(&connection).await;
            barrier.wait().await;
            let thread = app
                .dispatch_request(&connection, "thread/start", Some(json!({})))
                .await
                .map_err(|error| format!("thread/start: {}", error.message))?;
            let thread_id = thread["thread"]["id"]
                .as_str()
                .expect("thread/start response missing thread id")
                .to_string();
            app.dispatch_request(
                &connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id.clone(),
                    "input": [{
                        "type": "text",
                        "text": "history contention probe",
                        "text_elements": [],
                    }],
                })),
            )
            .await
            .map_err(|error| format!("turn/start: {}", error.message))?;
            Ok::<_, String>(())
        });
    }

    let burst = async {
        let mut failures = Vec::new();
        while let Some(task) = tasks.join_next().await {
            match task {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error),
                Err(error) => failures.push(format!("new-thread turn task failed: {error}")),
            }
        }
        failures
    };

    // Timeouts here are hang detectors, not performance assertions: shutdown of
    // 200 threads does O(n) serialized history writes and took >10s on a 2-core
    // CI runner, so bounds carry an order of magnitude of headroom over the
    // fast-path time.
    let burst_result = tokio::time::timeout(std::time::Duration::from_secs(120), burst).await;
    let task_shutdown_result =
        tokio::time::timeout(std::time::Duration::from_secs(60), tasks.shutdown()).await;
    let shutdown_result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        app.inner.supervisor.shutdown_all(),
    )
    .await;
    drop(app);
    let cleanup_result = std::fs::remove_dir_all(root);

    let failures = burst_result.expect("200 in-process new-thread turns exceeded the 120s bound");
    task_shutdown_result.expect("new-thread turn task shutdown exceeded the 60s bound");
    shutdown_result
        .expect("app-server shutdown exceeded the 60s bound")
        .unwrap();
    cleanup_result.unwrap();
    assert!(
        failures.is_empty(),
        "new-thread turn burst surfaced {} RPC errors: {failures:#?}",
        failures.len()
    );
}

#[test]
fn assistant_content_projection_concatenates_thinking_chunks_like_streaming() {
    let messages = vec![
        CanonicalMessage::user_text("question"),
        CanonicalMessage::assistant(
            "openai",
            ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![
                CanonicalContent::Thinking {
                    text: "plan ".to_string(),
                    provider: crate::ThinkingProvider::Other("unit".to_string()),
                    metadata: crate::ThinkingMetadata::None,
                },
                CanonicalContent::Thinking {
                    text: "check".to_string(),
                    provider: crate::ThinkingProvider::Other("unit".to_string()),
                    metadata: crate::ThinkingMetadata::None,
                },
                CanonicalContent::text("answer"),
            ],
            CanonicalStopReason::EndTurn,
        ),
    ];

    let content = assistant_content_after_latest_user(&messages, "question").unwrap();
    assert_eq!(content.text, "answer");
    assert_eq!(content.thinking, "plan check");
}

#[test]
fn restored_turn_projection_preserves_thinking_before_text_order() {
    let coordinates = crate::ThreadCoordinates::new("tenant", "user", "session");
    let user_entry = SessionEntry::new(
        coordinates.clone(),
        None,
        SessionEntryKind::Message {
            message: CanonicalMessage::user_text("question"),
        },
    );
    let assistant_entry = SessionEntry::new(
        coordinates,
        Some(user_entry.entry_id),
        SessionEntryKind::Message {
            message: CanonicalMessage::assistant(
                "anthropic_bedrock",
                ProviderApi::AnthropicMessages,
                "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
                vec![
                    CanonicalContent::Thinking {
                        text: "plan".to_string(),
                        provider: crate::ThinkingProvider::Anthropic,
                        metadata: crate::ThinkingMetadata::None,
                    },
                    CanonicalContent::text("answer"),
                ],
                CanonicalStopReason::EndTurn,
            ),
        },
    );

    let (_preview, turns) = app_server_turns_from_session_entries(&[user_entry, assistant_entry]);
    let turn = turns.values().next().unwrap();
    let item_types = turn
        .items
        .iter()
        .filter_map(|item| item.get("type").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        item_types,
        vec!["userMessage", "agentThinking", "agentMessage"]
    );
}

#[test]
fn reconcile_turn_assistant_text_starts_empty_item_then_delta() {
    let mut turn = AppServerTurnState::new(
        "turn-reconcile-start".to_string(),
        vec![json!({ "type": "text", "text": "hello", "text_elements": [] })],
    );
    let item_id = turn.assistant_item_id.clone();

    let (started_item, delta, delta_item_id) =
        reconcile_turn_assistant_text(&mut turn, "saved final text").unwrap();

    let started_item = started_item.unwrap();
    assert_eq!(delta_item_id, item_id);
    assert_eq!(delta, "saved final text");
    assert_eq!(started_item["id"].as_str(), Some(item_id.as_str()));
    assert_eq!(item_text(&started_item).as_deref(), Some(""));

    let (turn_json, completed_items) = finalize_turn_payload(&mut turn);
    assert_eq!(
        completed_items.first().and_then(item_text).as_deref(),
        Some("saved final text")
    );
    assert_eq!(
        completed_turn_agent_text(&json!({ "turn": turn_json })).as_deref(),
        Some("saved final text")
    );
}

#[test]
fn reconcile_turn_assistant_text_appends_only_missing_suffix() {
    let mut turn = AppServerTurnState::new(
        "turn-reconcile-suffix".to_string(),
        vec![json!({ "type": "text", "text": "hello", "text_elements": [] })],
    );

    let _ = reconcile_turn_assistant_text(&mut turn, "saved").unwrap();
    let (started_item, delta, _) =
        reconcile_turn_assistant_text(&mut turn, "saved final text").unwrap();

    assert!(started_item.is_none());
    assert_eq!(delta, " final text");
    let (turn_json, completed_items) = finalize_turn_payload(&mut turn);
    assert_eq!(
        completed_items.first().and_then(item_text).as_deref(),
        Some("saved final text")
    );
    assert_eq!(
        completed_turn_agent_text(&json!({ "turn": turn_json })).as_deref(),
        Some("saved final text")
    );
}

#[test]
fn reconcile_turn_assistant_text_does_not_replay_existing_delta() {
    let mut turn = AppServerTurnState::new(
        "turn-reconcile-existing".to_string(),
        vec![json!({ "type": "text", "text": "hello", "text_elements": [] })],
    );

    let _ = reconcile_turn_assistant_text(&mut turn, "saved final text").unwrap();

    assert!(reconcile_turn_assistant_text(&mut turn, "saved final text").is_none());
}

#[test]
fn reconcile_turn_assistant_text_keeps_prior_stream_when_saved_text_diverges() {
    let mut turn = AppServerTurnState::new(
        "turn-reconcile-diverged".to_string(),
        vec![json!({ "type": "text", "text": "hello", "text_elements": [] })],
    );

    let _ = reconcile_turn_assistant_text(&mut turn, "streamed text").unwrap();

    assert!(reconcile_turn_assistant_text(&mut turn, "saved final text").is_none());
    let (turn_json, completed_items) = finalize_turn_payload(&mut turn);
    assert_eq!(
        completed_items.first().and_then(item_text).as_deref(),
        Some("streamed text")
    );
    assert_eq!(
        completed_turn_agent_text(&json!({ "turn": turn_json })).as_deref(),
        Some("streamed text")
    );
}

#[tokio::test]
async fn thread_compact_start_dispatches_to_runtime() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();

    let compact_start = app
        .dispatch_request(
            &connection,
            "thread/compact/start",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    assert_eq!(compact_start, json!({}));
}

#[tokio::test]
async fn app_server_new_local_seeds_default_provider_store() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-provider-store-{}.sock", Uuid::now_v7())),
    );
    let root = std::env::temp_dir().join(format!("cooldis-provider-store-{}", Uuid::now_v7()));
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let metadata_path = config.metadata_store_path();

    let app = CooldisAppServer::new_local(config).await.unwrap();
    assert_eq!(app.model_provider(), APP_SERVER_LOCAL_PROVIDER);

    let store = crate::SqliteMetadataStore::open(&metadata_path)
        .await
        .unwrap();
    let openai_compatible = store
        .get_provider(crate::OPENAI_COMPATIBLE_PROVIDER_ID)
        .await
        .unwrap()
        .expect("app-server boot should seed OpenAI Compatible provider metadata");
    assert_eq!(
        openai_compatible.base_url,
        crate::OPENAI_COMPATIBLE_BASE_URL
    );
    assert_eq!(
        openai_compatible.models[0].model_id,
        crate::OPENAI_COMPATIBLE_DEFAULT_MODEL
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_provider_auth_methods_store_redacted_credentials() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-provider-auth-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-provider-auth");
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    let provider_id = "fixture-auth";
    let project_metadata_path = config.metadata_store_path();
    let user_metadata_path = config.user_metadata_store_path();
    let metadata_store = crate::SqliteMetadataStore::open(&project_metadata_path)
        .await
        .unwrap();
    metadata_store
        .upsert_provider(
            LlmProviderRecord::new(
                provider_id,
                ProviderApi::OpenAIChatCompletions,
                "https://example.invalid/v1",
            )
            .with_display_name("Fixture Auth")
            .with_auth_header(true),
        )
        .await
        .unwrap();
    drop(metadata_store);

    let app = CooldisAppServer::new_local(config.clone()).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let initial = app
        .dispatch_request(
            &connection,
            "modelProvider/auth/status",
            Some(json!({ "providerId": provider_id })),
        )
        .await
        .unwrap();
    assert_eq!(initial["auth"]["providerId"], provider_id);
    assert_eq!(initial["auth"]["configured"], false);

    let set = app
        .dispatch_request(
            &connection,
            "modelProvider/auth/set",
            Some(json!({
                "providerId": provider_id,
                "apiKey": "stored-openai_compatible-key",
            })),
        )
        .await
        .unwrap();
    assert_eq!(set["auth"]["configured"], true);
    assert_eq!(set["auth"]["source"], "stored");
    assert_eq!(set["auth"]["label"], "stored credential");
    assert!(
        !serde_json::to_string(&set)
            .unwrap()
            .contains("stored-openai_compatible-key")
    );

    let project_store = crate::SqliteMetadataStore::open(&project_metadata_path)
        .await
        .unwrap();
    let user_store = crate::SqliteMetadataStore::open(&user_metadata_path)
        .await
        .unwrap();
    let provider = project_store
        .get_provider(provider_id)
        .await
        .unwrap()
        .expect("default provider should be seeded");
    assert!(
        crate::resolve_llm_provider_auth(
            &project_store,
            &provider,
            &crate::LlmProviderAuthContext::new()
        )
        .await
        .unwrap()
        .is_none()
    );
    let resolved = crate::resolve_llm_provider_auth(
        &user_store,
        &provider,
        &crate::LlmProviderAuthContext::new(),
    )
    .await
    .unwrap()
    .expect("stored provider credential should resolve");
    assert_eq!(resolved.source, crate::LlmProviderAuthSourceKind::Stored);
    assert_eq!(resolved.api_key, "stored-openai_compatible-key");

    let deleted = app
        .dispatch_request(
            &connection,
            "modelProvider/auth/delete",
            Some(json!({ "providerId": provider_id })),
        )
        .await
        .unwrap();
    assert_eq!(deleted["auth"]["configured"], false);
    let user_store = crate::SqliteMetadataStore::open(&user_metadata_path)
        .await
        .unwrap();
    assert!(
        crate::resolve_llm_provider_auth(
            &user_store,
            &provider,
            &crate::LlmProviderAuthContext::new()
        )
        .await
        .unwrap()
        .is_none()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_provider_list_and_read_return_redacted_endpoint_records() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-provider-list-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-provider-list");
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    let provider_id = "fixture-list";
    let project_metadata_path = config.metadata_store_path();
    let user_metadata_path = config.user_metadata_store_path();
    let metadata_store = crate::SqliteMetadataStore::open(&project_metadata_path)
        .await
        .unwrap();
    metadata_store
        .upsert_provider(
            LlmProviderRecord::new(
                provider_id,
                ProviderApi::OpenAIChatCompletions,
                "https://example.invalid/v1",
            )
            .with_display_name("Fixture List")
            .with_auth(crate::LlmProviderAuthConfig::Env {
                name: "FIXTURE_API_KEY".to_string(),
            })
            .with_auth_header(true)
            .with_header(
                "x-fixture",
                crate::LlmProviderConfigValue::literal("secret-header"),
            )
            .with_model(
                crate::LlmProviderModelRecord::new("fixture-model")
                    .with_display_name("Fixture Model")
                    .with_context_window_tokens(4096),
            ),
        )
        .await
        .unwrap();
    drop(metadata_store);
    crate::SqliteMetadataStore::open(&user_metadata_path)
        .await
        .unwrap()
        .set_credential(
            provider_id,
            crate::LlmProviderCredential::ApiKey {
                key: "stored-list-key".to_string(),
            },
        )
        .await
        .unwrap();
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let list = app
        .dispatch_request(&connection, "modelProvider/list", None)
        .await
        .unwrap();
    let provider = list["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|provider| provider["providerId"].as_str() == Some(provider_id))
        .expect("expected fixture provider");
    assert_eq!(provider["api"], "open_ai_chat_completions");
    assert_eq!(provider["baseUrl"], "https://example.invalid/v1");
    assert_eq!(provider["auth"]["type"], "env");
    assert_eq!(provider["auth"]["name"], "FIXTURE_API_KEY");
    assert_eq!(provider["headers"][0]["name"], "x-fixture");
    assert_eq!(provider["headers"][0]["value"]["type"], "literal");
    assert_eq!(provider["headers"][0]["value"]["value"]["redacted"], true);
    assert_eq!(provider["models"][0]["modelId"], "fixture-model");
    assert_eq!(provider["models"][0]["contextWindowTokens"], 4096);
    assert_eq!(provider["configuredAuth"]["configured"], true);
    assert_eq!(provider["configuredAuth"]["source"], "stored");
    assert!(
        !serde_json::to_string(&list)
            .unwrap()
            .contains("secret-header")
    );
    assert!(
        !serde_json::to_string(&list)
            .unwrap()
            .contains("stored-list-key")
    );

    let read = app
        .dispatch_request(
            &connection,
            "modelProvider/read",
            Some(json!({ "providerId": provider_id })),
        )
        .await
        .unwrap();
    assert_eq!(read["provider"]["providerId"], provider_id);

    let unknown = app
        .dispatch_request(
            &connection,
            "modelProvider/read",
            Some(json!({ "providerId": "missing-provider" })),
        )
        .await
        .unwrap_err();
    assert_eq!(unknown.code, -32602);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_provider_upsert_creates_and_updates_endpoint_records() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-provider-upsert-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-provider-upsert");
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let metadata_path = config.metadata_store_path();
    let app = CooldisAppServer::new_local(config.clone()).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let created = app
        .dispatch_request(
            &connection,
            "modelProvider/upsert",
            Some(json!({
                "provider": {
                    "providerId": "fixture-upsert",
                    "api": "open_ai_chat_completions",
                    "baseUrl": "https://example.invalid/v1",
                    "displayName": "Fixture Upsert",
                    "auth": { "type": "env", "name": "FIXTURE_UPSERT_KEY" },
                    "authHeader": true,
                    "headers": {
                        "x-mode": { "type": "literal", "value": "secret-mode" },
                        "x-env": { "type": "env", "name": "FIXTURE_HEADER" }
                    },
                    "metadata": { "owner": "tests" },
                    "models": [{
                        "modelId": "fixture-small",
                        "displayName": "Fixture Small",
                        "api": "open_ai_chat_completions",
                        "baseUrl": "https://example.invalid/model-v1",
                        "contextWindowTokens": 8192,
                        "maxOutputTokens": 2048,
                        "inputModalities": ["text", "image"],
                        "headers": {
                            "x-model": { "type": "literal", "value": "secret-model" }
                        },
                        "metadata": { "tier": "small" }
                    }]
                }
            })),
        )
        .await
        .unwrap();
    assert_eq!(created["provider"]["providerId"], "fixture-upsert");
    assert_eq!(
        created["provider"]["models"][0]["baseUrl"],
        "https://example.invalid/model-v1"
    );
    assert_eq!(
        created["provider"]["models"][0]["inputModalities"],
        json!(["text", "image"])
    );
    assert_eq!(created["provider"]["metadata"]["owner"], "tests");
    assert!(!created.to_string().contains("secret-mode"));
    assert!(!created.to_string().contains("secret-model"));

    let stored = crate::SqliteMetadataStore::open(&metadata_path)
        .await
        .unwrap()
        .get_provider("fixture-upsert")
        .await
        .unwrap()
        .expect("provider should be stored");
    assert_eq!(stored.display_name.as_deref(), Some("Fixture Upsert"));
    assert_eq!(stored.models[0].model_id, "fixture-small");
    assert_eq!(stored.models[0].context_window_tokens, Some(8192));

    let updated = app
        .dispatch_request(
            &connection,
            "modelProvider/upsert",
            Some(json!({
                "provider": {
                    "providerId": "fixture-upsert",
                    "api": "open_ai_chat_completions",
                    "baseUrl": "https://example.invalid/v2",
                    "displayName": "Fixture Updated",
                    "auth": { "type": "none" },
                    "models": [{ "modelId": "fixture-large" }]
                }
            })),
        )
        .await
        .unwrap();
    assert_eq!(updated["provider"]["baseUrl"], "https://example.invalid/v2");
    assert_eq!(updated["provider"]["displayName"], "Fixture Updated");
    assert_eq!(updated["provider"]["models"][0]["modelId"], "fixture-large");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_provider_upsert_rejects_inline_api_keys_and_command_values() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-provider-reject-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-provider-reject");
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let inline = app
        .dispatch_request(
            &connection,
            "modelProvider/upsert",
            Some(json!({
                "provider": {
                    "providerId": "fixture-inline",
                    "api": "open_ai_chat_completions",
                    "baseUrl": "https://example.invalid/v1",
                    "auth": { "type": "inline_api_key", "key": "secret" }
                }
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(inline.code, -32602);
    assert!(inline.message.contains("inline API keys"));

    let command_auth = app
        .dispatch_request(
            &connection,
            "modelProvider/upsert",
            Some(json!({
                "provider": {
                    "providerId": "fixture-command-auth",
                    "api": "open_ai_chat_completions",
                    "baseUrl": "https://example.invalid/v1",
                    "auth": { "type": "command", "command": "secret-helper" }
                }
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(command_auth.code, -32602);
    assert!(command_auth.message.contains("command-backed auth"));

    let command_header = app
        .dispatch_request(
            &connection,
            "modelProvider/upsert",
            Some(json!({
                "provider": {
                    "providerId": "fixture-command-header",
                    "api": "open_ai_chat_completions",
                    "baseUrl": "https://example.invalid/v1",
                    "headers": {
                        "x-command": { "type": "command", "command": "secret-helper" }
                    }
                }
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(command_header.code, -32602);
    assert!(command_header.message.contains("command-backed header"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_provider_delete_removes_record_and_stored_credential() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-provider-delete-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-provider-delete");
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    let metadata_path = config.metadata_store_path();
    let user_metadata_path = config.user_metadata_store_path();
    let metadata_store = crate::SqliteMetadataStore::open(&metadata_path)
        .await
        .unwrap();
    metadata_store
        .upsert_provider(LlmProviderRecord::new(
            "fixture-delete",
            ProviderApi::OpenAIChatCompletions,
            "https://example.invalid/v1",
        ))
        .await
        .unwrap();
    metadata_store
        .set_credential(
            "fixture-delete",
            crate::LlmProviderCredential::ApiKey {
                key: "stored-delete-key".to_string(),
            },
        )
        .await
        .unwrap();
    drop(metadata_store);
    crate::SqliteMetadataStore::open(&user_metadata_path)
        .await
        .unwrap()
        .set_credential(
            "fixture-delete",
            crate::LlmProviderCredential::ApiKey {
                key: "stored-user-delete-key".to_string(),
            },
        )
        .await
        .unwrap();
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let deleted = app
        .dispatch_request(
            &connection,
            "modelProvider/delete",
            Some(json!({ "providerId": "fixture-delete" })),
        )
        .await
        .unwrap();
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["providerId"], "fixture-delete");

    let store = crate::SqliteMetadataStore::open(&metadata_path)
        .await
        .unwrap();
    assert!(
        store
            .get_provider("fixture-delete")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_credential("fixture-delete")
            .await
            .unwrap()
            .is_none()
    );
    let user_store = crate::SqliteMetadataStore::open(&user_metadata_path)
        .await
        .unwrap();
    assert!(
        user_store
            .get_credential("fixture-delete")
            .await
            .unwrap()
            .is_none()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_model_provider_delete_finishes_all_credential_cleanup() {
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-provider-cancel-delete-{}.sock",
        Uuid::now_v7()
    )));
    let root = unique_test_root("app-server-provider-cancel-delete");
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    let metadata_path = config.metadata_store_path();
    let user_metadata_path = config.user_metadata_store_path();
    let metadata_store = crate::SqliteMetadataStore::open(&metadata_path)
        .await
        .unwrap();
    metadata_store
        .upsert_provider(LlmProviderRecord::new(
            "fixture-cancel-delete",
            ProviderApi::OpenAIChatCompletions,
            "https://example.invalid/v1",
        ))
        .await
        .unwrap();
    metadata_store
        .set_credential(
            "fixture-cancel-delete",
            crate::LlmProviderCredential::ApiKey {
                key: "stored-project-delete-key".to_string(),
            },
        )
        .await
        .unwrap();
    let user_store = crate::SqliteMetadataStore::open(&user_metadata_path)
        .await
        .unwrap();
    user_store
        .set_credential(
            "fixture-cancel-delete",
            crate::LlmProviderCredential::ApiKey {
                key: "stored-user-delete-key".to_string(),
            },
        )
        .await
        .unwrap();

    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let user_db =
        cooldis_sqlite::Db::open(&user_metadata_path, cooldis_sqlite::DbConfig::default())
            .await
            .unwrap();
    let mut user_connection = user_db.connect().await.unwrap();
    let blocker = user_connection
        .transaction_with_behavior(cooldis_sqlite::TransactionBehavior::Immediate)
        .await
        .unwrap();

    let delete_app = app.clone();
    let delete_task = tokio::spawn(async move {
        delete_app
            .dispatch_request(
                &connection,
                "modelProvider/delete",
                Some(json!({ "providerId": "fixture-cancel-delete" })),
            )
            .await
    });

    let mut provider_deleted = false;
    for _ in 0..10_000 {
        if metadata_store
            .get_provider("fixture-cancel-delete")
            .await
            .unwrap()
            .is_none()
        {
            provider_deleted = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        provider_deleted,
        "provider delete did not reach the blocked credential cleanup"
    );

    delete_task.abort();
    assert!(delete_task.await.unwrap_err().is_cancelled());
    blocker.rollback().await.unwrap();

    let mut cleanup_finished = false;
    for _ in 0..10_000 {
        let project_credential = metadata_store
            .get_credential("fixture-cancel-delete")
            .await
            .unwrap();
        let user_credential = user_store
            .get_credential("fixture-cancel-delete")
            .await
            .unwrap();
        if project_credential.is_none() && user_credential.is_none() {
            cleanup_finished = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        cleanup_finished,
        "cancelled provider delete left stored credentials behind"
    );

    drop(user_connection);
    drop(user_db);
    drop(user_store);
    drop(metadata_store);
    drop(app);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_mcp_status_lists_redacted_remote_sources() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-mcp-source-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-mcp-source");
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let metadata_path = config.metadata_store_path();
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let registry = SqliteMcpSourceRegistry::open_async(&metadata_path)
        .await
        .unwrap();
    registry
        .upsert_source_async(
            crate::McpRemoteServerConfig::new(
                "arcade",
                crate::McpRemoteTransport::StreamableHttp,
                "https://example.com/mcp",
            )
            .unwrap()
            .with_bearer_secret("arcade.api_key")
            .unwrap()
            .with_header("x-provider", "fixture-secret-like-value"),
        )
        .await
        .unwrap();

    let status = app.mcp_server_status_list().await.unwrap();

    assert_eq!(status["data"][0]["name"], "arcade");
    assert_eq!(status["data"][0]["auth"]["secret"], "arcade.api_key");
    assert_eq!(status["data"][0]["auth"]["value"]["redacted"], true);
    assert!(!status.to_string().contains("fixture-secret-like-value"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_mcp_source_methods_register_discover_test_and_delete_remote_source() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-mcp-rpc-source-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-mcp-rpc-source");
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    let metadata_path = config.metadata_store_path();
    let user_metadata_path = config.user_metadata_store_path();
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let (mcp_url, mcp_task) = spawn_app_mcp_http_fixture("string").await;

    let upsert = app
        .dispatch_request(
            &connection,
            "mcpSource/upsert",
            Some(json!({
                "name": "arcade",
                "transport": "mcp-http",
                "url": mcp_url,
                "bearerToken": "fixture-token",
                "headers": [{ "name": "x-provider", "value": "fixture-secret-like-value" }],
                "includeTools": ["cooldis_mcp_echo"],
                "timeoutMs": 3000,
                "maxOutputBytes": 4096,
            })),
        )
        .await
        .unwrap();
    assert_eq!(upsert["source"]["name"], "arcade");
    assert_eq!(upsert["source"]["transport"], "streamable_http");
    assert_eq!(upsert["source"]["auth"]["secret"], "mcp.arcade.bearer");
    assert_eq!(upsert["source"]["auth"]["value"]["redacted"], true);
    assert_eq!(upsert["source"]["headers"][0]["name"], "x-provider");
    assert_eq!(upsert["source"]["headers"][0]["value"]["redacted"], true);
    assert!(!upsert.to_string().contains("fixture-token"));
    assert!(!upsert.to_string().contains("fixture-secret-like-value"));

    assert!(
        SqliteSecretStore::open(&metadata_path)
            .await
            .unwrap()
            .resolve_secret("mcp.arcade.bearer")
            .await
            .unwrap()
            .is_none()
    );
    let secret = SqliteSecretStore::open(&user_metadata_path)
        .await
        .unwrap()
        .resolve_secret("mcp.arcade.bearer")
        .await
        .unwrap()
        .expect("upsert should persist pasted bearer token");
    assert_eq!(secret.value, "fixture-token");
    assert_eq!(secret.source_kind, SecretSourceKind::Local);

    let list = app
        .dispatch_request(&connection, "mcpSource/list", Some(json!({})))
        .await
        .unwrap();
    assert_eq!(list["data"][0]["name"], "arcade");

    let legacy_status = app
        .dispatch_request(&connection, "mcpServerStatus/list", Some(json!({})))
        .await
        .unwrap();
    assert_eq!(legacy_status["data"][0]["name"], "arcade");

    let discovered = app
        .dispatch_request(
            &connection,
            "mcpSource/discover",
            Some(json!({ "name": "arcade" })),
        )
        .await
        .unwrap();
    assert_eq!(
        discovered["source"]["discovered_tools"][0]["name"],
        "cooldis_mcp_echo"
    );

    let read = app
        .dispatch_request(
            &connection,
            "mcpSource/read",
            Some(json!({ "name": "arcade" })),
        )
        .await
        .unwrap();
    assert_eq!(
        read["source"]["discovered_tools"].as_array().unwrap().len(),
        1
    );

    let test_tool = app
        .dispatch_request(
            &connection,
            "mcpSource/testTool",
            Some(json!({
                "name": "arcade",
                "tool": "cooldis_mcp_echo",
                "arguments": { "message": "hello" },
            })),
        )
        .await
        .unwrap();
    assert_eq!(test_tool["toolName"], "cooldis_mcp_echo");
    assert_eq!(test_tool["isError"], false);
    assert!(
        test_tool["contentText"]
            .as_str()
            .unwrap()
            .contains("REMOTE_MCP_OK hello")
    );

    let delete = app
        .dispatch_request(
            &connection,
            "mcpSource/delete",
            Some(json!({ "name": "arcade" })),
        )
        .await
        .unwrap();
    assert_eq!(delete["deleted"], true);
    let empty = app
        .dispatch_request(&connection, "mcpSource/list", Some(json!({})))
        .await
        .unwrap();
    assert!(empty["data"].as_array().unwrap().is_empty());

    mcp_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_mcp_source_manifest_patch_previews_bare_protocol_import() {
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-mcp-manifest-patch-{}.sock",
        Uuid::now_v7()
    )));
    let root = unique_test_root("app-server-mcp-manifest-patch");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let existing_record = publish_agent_manifest(
        &root,
        &agent_registry_root,
        "mcp-existing",
        "MCP Existing",
        "Already imports arcade",
        &[r#"
[[tools]]
type = "protocol_tool_import"
id = "arcade"
protocol = "mcp"
server_ref = "mcp://arcade"
"#
        .to_string()],
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.clone();
    let metadata_path = config.metadata_store_path();
    let app = CooldisAppServer::new_local(config).await.unwrap();
    SqliteMcpSourceRegistry::open_async(&metadata_path)
        .await
        .unwrap()
        .upsert_source_async(
            crate::McpRemoteServerConfig::new(
                "arcade",
                crate::McpRemoteTransport::StreamableHttp,
                "https://example.com/mcp",
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let preview = app
        .dispatch_request(
            &connection,
            "mcpSource/manifestPatch",
            Some(json!({
                "name": "arcade",
                "agentRef": "agent://mcp-existing@latest",
            })),
        )
        .await
        .unwrap();

    assert_eq!(preview["source"]["name"], "arcade");
    assert_eq!(preview["serverRef"], "mcp://arcade");
    assert_eq!(preview["tool"]["type"], "protocol_tool_import");
    assert_eq!(preview["tool"]["id"], "arcade");
    assert_eq!(preview["tool"]["protocol"], "mcp");
    assert_eq!(preview["tool"]["server_ref"], "mcp://arcade");
    let toml = preview["toml"].as_str().unwrap();
    assert!(toml.contains("[[tools]]"));
    assert!(toml.contains("type = \"protocol_tool_import\""));
    assert!(toml.contains("id = \"arcade\""));
    assert!(toml.contains("server_ref = \"mcp://arcade\""));
    let diagnostic_codes = preview["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|diagnostic| diagnostic["code"].as_str())
        .collect::<BTreeSet<_>>();
    assert!(diagnostic_codes.contains("duplicate_tool_id"));
    assert!(diagnostic_codes.contains("source_already_imported"));
    let current_record = LocalAgentRegistry::new(&agent_registry_root)
        .load_ref("agent://mcp-existing@latest")
        .unwrap();
    assert_eq!(current_record.manifest_hash, existing_record.manifest_hash);
    assert_eq!(
        current_record.resolved_manifest["tools"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let unknown = app
        .dispatch_request(
            &connection,
            "mcpSource/manifestPatch",
            Some(json!({ "name": "missing" })),
        )
        .await
        .unwrap_err();
    assert_eq!(unknown.code, -32602);
    assert!(unknown.message.contains("not found"));

    let invalid_import_id = app
        .dispatch_request(
            &connection,
            "mcpSource/manifestPatch",
            Some(json!({ "name": "arcade", "importId": "bad id" })),
        )
        .await
        .unwrap_err();
    assert_eq!(invalid_import_id.code, -32602);
    assert!(invalid_import_id.message.contains("importId"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn agent_query_methods_project_local_registry_records() {
    let root = unique_test_root("app-server-agent-query");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let record = publish_agent_manifest(
        &root,
        &agent_registry_root,
        "local-runner",
        "Local Runner",
        "Runs local prompts",
        &[],
    );
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-agent-query-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.clone();
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let list = app
        .dispatch_request(&connection, "agent/list", None)
        .await
        .unwrap();
    assert_eq!(list["cursor"], Value::Null);
    let agents = list["data"].as_array().unwrap();
    assert_eq!(agents.len(), 2);
    assert!(
        agents
            .iter()
            .any(|agent| agent["name"].as_str() == Some("default"))
    );
    let local_agent = agents
        .iter()
        .find(|agent| agent["name"].as_str() == Some("local-runner"))
        .unwrap();
    assert_eq!(local_agent["version"].as_str(), Some("0.1.0"));
    assert_eq!(
        local_agent["refUri"].as_str(),
        Some(record.ref_uri.as_str())
    );
    assert_eq!(
        local_agent["manifestHash"].as_str(),
        Some(record.manifest_hash.as_str())
    );
    assert_eq!(local_agent["title"].as_str(), Some("Local Runner"));
    assert_eq!(local_agent["summary"].as_str(), Some("Runs local prompts"));
    assert_eq!(
        local_agent["defaultModelProfile"]["id"].as_str(),
        Some("default")
    );
    assert_eq!(
        local_agent["defaultModelProfile"]["providerRef"].as_str(),
        Some("provider://local_offline")
    );
    assert_eq!(
        local_agent["defaultModelProfile"]["modelRef"].as_str(),
        Some("model://local_offline/echo")
    );
    assert_eq!(local_agent["toolIds"].as_array().unwrap().len(), 0);
    assert_eq!(local_agent["aliases"][0]["alias"].as_str(), Some("latest"));
    assert_eq!(local_agent["aliases"][0]["version"].as_str(), Some("0.1.0"));
    assert!(local_agent.get("authored_source").is_none());
    assert!(local_agent.get("resolved_manifest").is_none());

    let read = app
        .dispatch_request(
            &connection,
            "agent/read",
            Some(json!({ "ref": "agent://local-runner@latest" })),
        )
        .await
        .unwrap();
    assert_eq!(read["name"].as_str(), Some("local-runner"));
    assert_eq!(
        read["aliasResolutionReceipt"]["alias"].as_str(),
        Some("latest")
    );
    assert_eq!(
        read["aliasResolutionReceipt"]["manifest_hash"].as_str(),
        Some(record.manifest_hash.as_str())
    );

    let err = app
        .dispatch_request(
            &connection,
            "agent/read",
            Some(json!({ "ref": "agent://missing@latest" })),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, -32602);
    assert!(err.message.contains("unknown agent ref"));

    let malformed = app
        .dispatch_request(
            &connection,
            "agent/read",
            Some(json!({ "ref": "agent://bad name@latest" })),
        )
        .await
        .unwrap_err();
    assert_eq!(malformed.code, -32602);
    assert!(malformed.message.contains("malformed agent ref"));

    std::fs::remove_file(
        LocalAgentRegistry::new(&agent_registry_root)
            .version_record_path("local-runner", "0.1.0")
            .unwrap(),
    )
    .unwrap();
    let stale_alias = app
        .dispatch_request(&connection, "agent/list", None)
        .await
        .unwrap_err();
    assert_eq!(stale_alias.code, -32000);
    assert!(
        stale_alias
            .message
            .contains("failed to read agent version record")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn agent_plan_validates_source_and_manifest_without_writes() {
    let root = unique_test_root("app-server-agent-plan");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let base = publish_agent_manifest(
        &root,
        &agent_registry_root,
        "planner",
        "Planner",
        "Plans without writes",
        &[],
    );
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-agent-plan-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.clone();
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let source = r#"
[agent]
name = "planner"
version = "0.1.1"
display_name = "Planner v2"
description = "Plans without writes, still"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = true
"#;
    let from_source = app
        .dispatch_request(
            &connection,
            "agent/plan",
            Some(json!({
                "source": source,
                "baseRef": base.ref_uri,
                "baseManifestHash": base.manifest_hash,
                "expectedLatestVersion": base.version,
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        from_source["plan"]["ref_uri"].as_str(),
        Some("agent://planner@0.1.1")
    );
    assert_eq!(
        from_source["manifest"]["identity"]["display_name"].as_str(),
        Some("Planner v2")
    );
    assert_eq!(from_source["diagnostics"].as_array().unwrap().len(), 0);
    assert_eq!(from_source["suggestedNextVersion"].as_str(), Some("0.1.1"));
    assert_eq!(from_source["base"]["latestVersion"].as_str(), Some("0.1.0"));
    assert!(
        !LocalAgentRegistry::new(&agent_registry_root)
            .version_record_path("planner", "0.1.1")
            .unwrap()
            .exists()
    );

    let mut manifest = from_source["manifest"].clone();
    manifest["identity"]["version"] = json!("0.1.2");
    let from_manifest = app
        .dispatch_request(
            &connection,
            "agent/plan",
            Some(json!({ "manifest": manifest })),
        )
        .await
        .unwrap();
    assert_eq!(
        from_manifest["plan"]["ref_uri"].as_str(),
        Some("agent://planner@0.1.2")
    );
    assert!(
        from_manifest["source"]
            .as_str()
            .unwrap()
            .contains("version = \"0.1.2\"")
    );
}

#[tokio::test]
async fn agent_publish_writes_new_version_and_rejects_stale_base() {
    let root = unique_test_root("app-server-agent-publish");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let base = publish_agent_manifest(
        &root,
        &agent_registry_root,
        "publisher",
        "Publisher",
        "Publishes immutable versions",
        &[],
    );
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-agent-publish-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.clone();
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let read = app
        .dispatch_request(
            &connection,
            "agent/read",
            Some(json!({ "ref": "agent://publisher@latest" })),
        )
        .await
        .unwrap();
    let mut manifest = read["resolved_manifest"].clone();
    manifest["identity"]["version"] = json!("0.1.1");
    manifest["identity"]["display_name"] = json!("Publisher v2");

    let publish = app
        .dispatch_request(
            &connection,
            "agent/publish",
            Some(json!({
                "manifest": manifest,
                "baseRef": "agent://publisher@latest",
                "baseManifestHash": base.manifest_hash,
                "expectedLatestVersion": base.version,
            })),
        )
        .await
        .unwrap();
    assert_eq!(publish["record"]["version"].as_str(), Some("0.1.1"));
    assert_eq!(
        publish["record"]["resolved_manifest"]["identity"]["display_name"].as_str(),
        Some("Publisher v2")
    );
    assert_eq!(publish["latestAlias"]["version"].as_str(), Some("0.1.1"));
    let published_record = LocalAgentRegistry::new(&agent_registry_root)
        .load_version_record("publisher", "0.1.1")
        .unwrap();
    assert_eq!(
        published_record.authored_source.as_deref(),
        publish["source"].as_str()
    );

    let stale = app
        .dispatch_request(
            &connection,
            "agent/publish",
            Some(json!({
                "source": r#"
[agent]
name = "publisher"
version = "0.1.2"
display_name = "Stale Publisher"
description = "Should fail stale base"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = true
"#,
                "baseRef": "agent://publisher@latest",
                "baseManifestHash": base.manifest_hash,
                "expectedLatestVersion": base.version,
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code, -32000);
    assert!(stale.message.contains("stale agent manifest draft"));
}

#[tokio::test]
async fn operation_list_projects_published_registry_records() {
    let root = unique_test_root("app-server-operation-query");
    let registry_root = root.join("operations");
    let record = publish_echo_operation(&registry_root, "search", "search_web", "result").await;
    let app = test_app_with_provider_and_capsule_bindings(
        Arc::new(InspectingCapsuleClient::default()),
        CapsuleBindingsConfig::default().with_registry_root(&registry_root),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let list = app
        .dispatch_request(&connection, "operation/list", None)
        .await
        .unwrap();
    assert_eq!(list["cursor"], Value::Null);
    let operations = list["data"].as_array().unwrap();
    assert!(operations.len() >= 2);
    assert!(
        operations
            .iter()
            .any(|operation| operation["name"].as_str() == Some(COOLDIS_THREADS_PACKAGE))
    );
    let search = operation_record_by_name(operations, "search");
    assert_eq!(
        search["activeArtifactHash"].as_str(),
        Some(record.active_artifact_hash.as_str())
    );
    assert_eq!(
        search["projections"]["operations"][0]["mcp"]["tool_name"].as_str(),
        Some("search_search_web")
    );
    assert_eq!(
        search["manifest"]["operations"][0]["name"].as_str(),
        Some("search_web")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn registry_roots_resolve_against_configured_cwd() {
    let root = unique_test_root("app-server-registry-cwd");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = workspace.join(".cooldis/agents");
    publish_agent_manifest(
        &root,
        &agent_registry_root,
        "cwd-runner",
        "CWD Runner",
        "Loaded from configured cwd",
        &[],
    );
    let operation_registry_root = workspace.join(".cooldis/operations");
    publish_echo_operation(&operation_registry_root, "cwdop", "lookup", "cwd").await;

    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-registry-cwd-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace).with_capsule_bindings(
        CapsuleBindingsConfig::default().with_registry_root(".cooldis/operations"),
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let agents = app
        .dispatch_request(&connection, "agent/list", None)
        .await
        .unwrap();
    assert_eq!(agents["data"][0]["name"].as_str(), Some("cwd-runner"));

    let operations = app
        .dispatch_request(&connection, "operation/list", None)
        .await
        .unwrap();
    let operation_records = operations["data"].as_array().unwrap();
    assert_eq!(
        operation_record_by_name(operation_records, "cwdop")["name"].as_str(),
        Some("cwdop")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_list_projects_catalog_provider_models() {
    let root = unique_test_root("app-server-model-query");
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-model-query-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, root.clone())
        .with_catalog_openai_chat_completions("fixture", Some("fixture-large".to_string()));
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    metadata_store
        .upsert_provider(
            LlmProviderRecord::new(
                "fixture",
                ProviderApi::OpenAIChatCompletions,
                "https://example.invalid/v1",
            )
            .with_display_name("Fixture Models")
            .with_model(
                crate::LlmProviderModelRecord::new("fixture-small")
                    .with_display_name("Fixture Small")
                    .with_context_window_tokens(1024),
            )
            .with_model(
                crate::LlmProviderModelRecord::new("fixture-large")
                    .with_display_name("Fixture Large")
                    .with_max_output_tokens(2048),
            ),
        )
        .await
        .unwrap();
    sync_catalog_provider_identity(&mut config, &metadata_store)
        .await
        .unwrap();
    let runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "fixture", "fixture-large");
    let runtime_factory = runtime_factory_from_provider_parts(
        runtime_config,
        Arc::new(InspectingCapsuleClient::default()),
        CapsuleBindingsConfig::default(),
    );
    let app = CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let models = app
        .dispatch_request(&connection, "model/list", None)
        .await
        .unwrap();
    assert_eq!(models["nextCursor"], Value::Null);
    let data = models["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["providerId"].as_str(), Some("fixture"));
    assert_eq!(data[0]["id"].as_str(), Some("fixture-small"));
    assert_eq!(data[0]["isDefault"].as_bool(), Some(false));
    assert_eq!(data[1]["id"].as_str(), Some("fixture-large"));
    assert_eq!(data[1]["displayName"].as_str(), Some("Fixture Large"));
    assert_eq!(data[1]["maxOutputTokens"].as_u64(), Some(2048));
    assert_eq!(data[1]["isDefault"].as_bool(), Some(true));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_list_appends_configured_default_when_catalog_omits_it() {
    let root = unique_test_root("app-server-model-missing-default");
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-model-missing-default-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, root.clone())
        .with_catalog_openai_chat_completions("fixture", Some("fixture-default".to_string()));
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    metadata_store
        .upsert_provider(
            LlmProviderRecord::new(
                "fixture",
                ProviderApi::OpenAIChatCompletions,
                "https://example.invalid/v1",
            )
            .with_display_name("Fixture Models")
            .with_model(crate::LlmProviderModelRecord::new("fixture-small"))
            .with_model(crate::LlmProviderModelRecord::new("fixture-large")),
        )
        .await
        .unwrap();
    sync_catalog_provider_identity(&mut config, &metadata_store)
        .await
        .unwrap();
    let runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "fixture", "fixture-default");
    let runtime_factory = runtime_factory_from_provider_parts(
        runtime_config,
        Arc::new(InspectingCapsuleClient::default()),
        CapsuleBindingsConfig::default(),
    );
    let app = CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let models = app
        .dispatch_request(&connection, "model/list", None)
        .await
        .unwrap();
    let data = models["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    assert_eq!(
        data.iter()
            .filter(|model| model["isDefault"].as_bool() == Some(true))
            .count(),
        1
    );
    let default = data
        .iter()
        .find(|model| model["isDefault"].as_bool() == Some(true))
        .unwrap();
    assert_eq!(default["id"].as_str(), Some("fixture-default"));
    assert_eq!(default["providerId"].as_str(), Some("fixture"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_persists_thread_lifecycle_to_metadata_store() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-thread-store-{}.sock", Uuid::now_v7())),
    );
    let root = std::env::temp_dir().join(format!("cooldis-thread-store-{}", Uuid::now_v7()));
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let metadata_path = config.metadata_store_path();

    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app);
    initialize_for_test(&connection).await;

    let thread_start = connection
        .app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = ThreadId::parse_str(thread_start["thread"]["id"].as_str().unwrap())
        .expect("thread/start should return a thread id");

    let store = crate::SqliteMetadataStore::open(&metadata_path)
        .await
        .unwrap();
    let record = store
        .get_thread_lifecycle(thread_id)
        .await
        .unwrap()
        .expect("app-server thread/start should persist thread lifecycle metadata");
    assert_eq!(record.coordinates.tenant_id, "cooldis_app_server");
    assert_eq!(record.coordinates.user_id, "local_user");
    assert_eq!(record.status, crate::ThreadLifecycleStatus::Idle);
    assert_eq!(record.topology, ThreadTopology::root());
    assert_eq!(
        store
            .list_thread_lifecycle(&record.coordinates.scope())
            .await
            .unwrap()
            .len(),
        1
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_handle_dispatch_retries_fold_through_rpc() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-thread-spawn-{}.sock", Uuid::now_v7())),
    );
    let root = std::env::temp_dir().join(format!("cooldis-thread-spawn-{}", Uuid::now_v7()));
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");

    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let started = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let parent_thread_id = started["thread"]["id"].as_str().unwrap();
    let params = json!({
        "threadId": parent_thread_id,
        "taskName": "worker",
        "message": "echo dispatched child",
        "dispatchId": "rpc-dispatch-1",
    });

    let first = app
        .dispatch_request(&connection, "thread/spawn", Some(params.clone()))
        .await
        .unwrap();
    let retry = app
        .dispatch_request(&connection, "thread/spawn", Some(params))
        .await
        .unwrap();
    let conflicting_retry = app
        .dispatch_request(
            &connection,
            "thread/spawn",
            Some(json!({
                "threadId": parent_thread_id,
                "taskName": "retry-alias-must-not-win",
                "message": "echo retry message must not run",
                "dispatchId": "rpc-dispatch-1",
            })),
        )
        .await
        .unwrap();
    let distinct = app
        .dispatch_request(
            &connection,
            "thread/spawn",
            Some(json!({
                "threadId": parent_thread_id,
                "taskName": "worker-2",
                "message": "echo second child",
                "dispatchId": "rpc-dispatch-2",
            })),
        )
        .await
        .unwrap();

    assert_eq!(first["handle"], retry["handle"]);
    assert_eq!(first["dispatchId"], "rpc-dispatch-1");
    assert_eq!(retry["dispatchId"], "rpc-dispatch-1");
    assert_eq!(conflicting_retry["handle"], first["handle"]);
    assert_eq!(conflicting_retry["taskName"], "worker");
    assert_ne!(first["handle"], distinct["handle"]);
    let submit_params = json!({
        "threadId": parent_thread_id,
        "message": "echo submitted once",
        "dispatchId": "rpc-submit-dispatch-1",
    });
    let first_submit = app
        .dispatch_request(&connection, "thread/submit", Some(submit_params.clone()))
        .await
        .unwrap();
    let retry_submit = app
        .dispatch_request(&connection, "thread/submit", Some(submit_params))
        .await
        .unwrap();
    assert_eq!(first_submit["dispatchId"], "rpc-submit-dispatch-1");
    assert_eq!(first_submit["turnId"], retry_submit["turnId"]);
    assert_eq!(first_submit["interactionId"], retry_submit["interactionId"]);
    let parent = app.handle_for_thread(parent_thread_id).await.unwrap();
    let store = app
        .inner
        .supervisor
        .runtime_store(&app.inner.tenant_id)
        .await
        .unwrap();
    let control_events = store
        .read_events(
            &EventStreamId::new(format!(
                "control:{}",
                parent.context().coordinates.thread_id
            )),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        control_events
            .iter()
            .filter(|event| {
                event.kind == EventKind::ThreadSpawnRequested
                    && event.payload["correlation_id"] == "rpc-dispatch-1"
                    && event.provenance.discharged_by.as_deref() == Some("dispatcher:thread-spawn")
            })
            .count(),
        1
    );

    app.inner.supervisor.shutdown_all().await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn ref_less_thread_start_default_manifest_gate_allows_lowered_params() {
    let params = ThreadStartParams::default();
    assert_eq!(
        thread_start_default_agent_ref(&params),
        Some(default_manifest::DEFAULT_AGENT_REF)
    );

    let mut params = ThreadStartParams {
        capsule_bindings: Some(ThreadCapsuleBindingsParams::default()),
        ..ThreadStartParams::default()
    };
    assert_eq!(
        thread_start_default_agent_ref(&params),
        Some(default_manifest::DEFAULT_AGENT_REF)
    );

    params.model = Some(APP_SERVER_LOCAL_MODEL.to_string());
    assert_eq!(
        thread_start_default_agent_ref(&params),
        Some(default_manifest::DEFAULT_AGENT_REF)
    );

    let params = ThreadStartParams {
        model_provider: Some(APP_SERVER_LOCAL_PROVIDER.to_string()),
        ..ThreadStartParams::default()
    };
    assert_eq!(
        thread_start_default_agent_ref(&params),
        Some(default_manifest::DEFAULT_AGENT_REF)
    );

    let params = ThreadStartParams {
        cwd: Some("workspace".to_string()),
        ..ThreadStartParams::default()
    };
    assert_eq!(
        thread_start_default_agent_ref(&params),
        Some(default_manifest::DEFAULT_AGENT_REF)
    );

    let params = ThreadStartParams {
        capsule_bindings: Some(ThreadCapsuleBindingsParams {
            operation_names: vec!["global".to_string()],
        }),
        ..ThreadStartParams::default()
    };
    assert_eq!(
        thread_start_default_agent_ref(&params),
        Some(default_manifest::DEFAULT_AGENT_REF)
    );

    let params = ThreadStartParams {
        agent_ref: Some(default_manifest::DEFAULT_AGENT_REF.to_string()),
        ..ThreadStartParams::default()
    };
    assert_eq!(thread_start_default_agent_ref(&params), None);

    let params = ThreadStartParams {
        runtime_overrides: Some(AgentManifestBindOverrides {
            default_cwd: Some("workspace".to_string()),
            ..AgentManifestBindOverrides::default()
        }),
        ..ThreadStartParams::default()
    };
    assert_eq!(
        thread_start_default_agent_ref(&params),
        Some(default_manifest::DEFAULT_AGENT_REF)
    );

    let params = ThreadStartParams {
        model: Some(APP_SERVER_LOCAL_MODEL.to_string()),
        runtime_overrides: Some(AgentManifestBindOverrides {
            default_cwd: Some("workspace".to_string()),
            ..AgentManifestBindOverrides::default()
        }),
        ..ThreadStartParams::default()
    };
    assert_eq!(
        thread_start_default_agent_ref(&params),
        Some(default_manifest::DEFAULT_AGENT_REF)
    );
}

#[tokio::test]
async fn ref_less_thread_start_binds_default_manifest() {
    use crate::EventStore;

    let root = unique_test_root("app-server-default-manifest-start");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let registry = LocalAgentRegistry::new(&agent_registry_root);
    assert!(registry.list_records().unwrap().is_empty());

    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-default-manifest-start-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.clone();
    let metadata_path = config.metadata_store_path();
    let session_path = config.state_home.join("session_history.sqlite3");
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let default_record = registry.load_ref("agent://cooldis/default@latest").unwrap();
    assert_eq!(default_record.name, "default");
    assert_eq!(default_record.namespace.as_deref(), Some("cooldis"));
    assert_eq!(default_record.version, "1.0.0");
    assert_eq!(default_record.tool_count, 0);
    assert_eq!(default_record.resource_count, 0);

    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let thread_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "cwd": "override-workspace"
            })),
        )
        .await
        .unwrap();
    let thread_id = ThreadId::parse_str(thread_start["thread"]["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        thread_start["cwd"].as_str(),
        Some(cwd_string(&workspace.join("override-workspace")).as_str())
    );

    let metadata_store = SqliteMetadataStore::open(metadata_path).await.unwrap();
    let lifecycle = metadata_store
        .get_thread_lifecycle(thread_id)
        .await
        .unwrap()
        .expect("default manifest start should persist lifecycle metadata");
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_REF_METADATA],
        default_record.ref_uri
    );
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_MANIFEST_HASH_METADATA],
        default_record.manifest_hash
    );
    assert_eq!(
        serde_json::from_str::<AgentManifestBindOverrides>(
            &lifecycle.metadata[THREAD_AGENT_RUNTIME_OVERRIDES_METADATA]
        )
        .unwrap()
        .default_cwd
        .as_deref(),
        Some(cwd_string(&workspace.join("override-workspace")).as_str())
    );

    let session_store = SqliteSessionStore::open(session_path).await.unwrap();
    let stream_id = EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 4);
    let compile = event_by_kind(&events, crate::EventKind::ManifestCompileCompleted);
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    assert_eq!(compile.origin, EventOrigin::Discharged);
    assert_eq!(bind.origin, EventOrigin::Discharged);
    assert_eq!(compile.payload["alias"]["alias"].as_str(), Some("latest"));
    assert_eq!(
        compile.payload["alias"]["manifest_hash"].as_str(),
        Some(default_record.manifest_hash.as_str())
    );
    assert_eq!(
        bind.payload["manifest_hash"].as_str(),
        Some(default_record.manifest_hash.as_str())
    );
    assert_eq!(
        bind.payload["overridden_keys"].as_array().unwrap(),
        &vec![json!("default_cwd")]
    );

    let rejected = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "runtimeOverrides": {
                    "streaming": false
                }
            })),
        )
        .await
        .unwrap_err();
    assert!(
        rejected
            .message
            .contains("runtime override \"streaming\" is not allowlisted")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_start_placement_override_wins_daemon_default_and_is_witnessed_once() {
    use crate::EventStore;

    let root = unique_test_root("app-server-placement-override");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-placement-override-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    config.default_placement = AgentManifestPlacementBinding {
        target: crate::PlacementTarget::Sandbox,
        executor_ref: Some("executor://sandbox/default".to_string()),
        config: BTreeMap::new(),
    };
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let rejected = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap_err();
    assert!(rejected.message.contains("remote EventStore backend"));

    let started = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({"placement": {"target": "local"}})),
        )
        .await
        .unwrap();
    let thread_id = ThreadId::parse_str(started["thread"]["id"].as_str().unwrap()).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(thread_id)
        .await
        .unwrap()
        .unwrap();
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let events = session_store
        .read_events(&EventStreamId::for_thread(&lifecycle.coordinates), None)
        .await
        .unwrap();
    let bind = event_by_kind(&events, EventKind::ManifestBindCompleted);
    assert_eq!(bind.payload["placement"]["target"], "local");
    let placement_events = events
        .iter()
        .filter(|event| event.kind == EventKind::PlacementDecision)
        .collect::<Vec<_>>();
    assert_eq!(placement_events.len(), 1);
    assert_eq!(placement_events[0].origin, EventOrigin::Witnessed);
    assert_eq!(placement_events[0].payload["placement"], "local");
    assert_eq!(
        placement_events[0].payload["snapshot_id"],
        bind.payload["manifest_hash"]
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_spawn_placement_requires_agent_ref_and_override_is_witnessed_once() {
    use crate::EventStore;

    let root = unique_test_root("app-server-spawn-placement-override");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-spawn-placement-override-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    config.default_placement = AgentManifestPlacementBinding {
        target: crate::PlacementTarget::Sandbox,
        executor_ref: Some("executor://sandbox/default".to_string()),
        config: BTreeMap::new(),
    };
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let started = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({"placement": {"target": "local"}})),
        )
        .await
        .unwrap();
    let parent_thread_id = started["thread"]["id"].as_str().unwrap();

    let rejected = app
        .dispatch_request(
            &connection,
            "thread/spawn",
            Some(json!({
                "threadId": parent_thread_id,
                "taskName": "worker",
                "message": "placement without a manifest bind",
                "placement": {"target": "local"}
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(rejected.code, -32602);
    assert_eq!(
        rejected.message,
        "placement requires agentRef on thread/spawn"
    );

    let spawned = app
        .dispatch_request(
            &connection,
            "thread/spawn",
            Some(json!({
                "threadId": parent_thread_id,
                "taskName": "worker",
                "message": "placement with a manifest bind",
                "agentRef": default_manifest::DEFAULT_AGENT_REF,
                "placement": {"target": "local"}
            })),
        )
        .await
        .unwrap();
    let child_id = ThreadId::parse_str(spawned["thread"]["id"].as_str().unwrap()).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(child_id)
        .await
        .unwrap()
        .unwrap();
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let events = session_store
        .read_events(&EventStreamId::for_thread(&lifecycle.coordinates), None)
        .await
        .unwrap();
    let bind = event_by_kind(&events, EventKind::ManifestBindCompleted);
    assert_eq!(bind.payload["placement"]["target"], "local");
    let placement_events = events
        .iter()
        .filter(|event| event.kind == EventKind::PlacementDecision)
        .collect::<Vec<_>>();
    assert_eq!(placement_events.len(), 1);
    assert_eq!(placement_events[0].origin, EventOrigin::Witnessed);
    assert_eq!(placement_events[0].payload["placement"], "local");
    assert_eq!(
        placement_events[0].payload["snapshot_id"],
        bind.payload["manifest_hash"]
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_start_model_param_selects_declared_manifest_profile() {
    use crate::EventStore;

    let root = unique_test_root("app-server-manifest-profile-select");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("profiles.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "profiles"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "small"
provider_ref = "provider://fixture"
model_ref = "model://fixture-small"

[[model_profiles]]
id = "large"
provider_ref = "provider://fixture"
model_ref = "model://fixture-large"

[runtime]
default_cwd = "."
streaming = false

[runtime.overrides]
allow = ["default_cwd"]
"#,
    )
    .unwrap();
    let record = LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-profile-select-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace)
        .with_catalog_openai_chat_completions("fixture", Some("fixture-large".to_string()));
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    metadata_store
        .upsert_provider(
            LlmProviderRecord::new(
                "fixture",
                ProviderApi::OpenAIChatCompletions,
                "https://example.invalid/v1",
            )
            .with_model(crate::LlmProviderModelRecord::new("fixture-small"))
            .with_model(crate::LlmProviderModelRecord::new("fixture-large")),
        )
        .await
        .unwrap();
    sync_catalog_provider_identity(&mut config, &metadata_store)
        .await
        .unwrap();
    let session_path = config.state_home.join("session_history.sqlite3");
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::OpenAIChatCompletions,
        "fixture",
        "fixture-large",
    );
    let runtime_factory = runtime_factory_from_provider_parts(
        runtime_config,
        Arc::new(InspectingCapsuleClient::default()),
        CapsuleBindingsConfig::default(),
    );
    let app = CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://profiles@latest",
                "model": "fixture-small"
            })),
        )
        .await
        .unwrap();
    let thread_id = ThreadId::parse_str(thread_start["thread"]["id"].as_str().unwrap()).unwrap();
    assert_eq!(thread_start["model"].as_str(), Some("fixture-small"));
    assert_eq!(thread_start["modelProvider"].as_str(), Some("fixture"));

    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(thread_id)
        .await
        .unwrap()
        .expect("profile-selected start should persist lifecycle metadata");
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_MODEL_PROFILE_ID_METADATA],
        "small"
    );
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_MODEL_ID_METADATA],
        "fixture-small"
    );
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_PROVIDER_ID_METADATA],
        "fixture"
    );

    let session_store = SqliteSessionStore::open(session_path).await.unwrap();
    let stream_id = EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    assert_eq!(
        bind.payload["ref_uri"].as_str(),
        Some(record.ref_uri.as_str())
    );
    assert_eq!(bind.payload["model_profile_id"].as_str(), Some("small"));
    assert_eq!(
        bind.payload["model_profile_origin"].as_str(),
        Some("selected-at-start")
    );
    assert_eq!(bind.payload["model_id"].as_str(), Some("fixture-small"));

    let large_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://profiles@latest",
                "modelProvider": "fixture",
                "model": "fixture-large"
            })),
        )
        .await
        .unwrap();
    let large_thread_id =
        ThreadId::parse_str(large_start["thread"]["id"].as_str().unwrap()).unwrap();
    assert_eq!(large_start["model"].as_str(), Some("fixture-large"));
    let large_lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(large_thread_id)
        .await
        .unwrap()
        .expect("provider/model selected start should persist lifecycle metadata");
    assert_eq!(
        large_lifecycle.metadata[THREAD_AGENT_MODEL_PROFILE_ID_METADATA],
        "large"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_start_rejects_undeclared_model_provider_with_declared_profiles() {
    let root = unique_test_root("app-server-manifest-profile-reject");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("profiles.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "profiles"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "small"
provider_ref = "provider://fixture"
model_ref = "model://fixture-small"

[[model_profiles]]
id = "large"
provider_ref = "provider://fixture"
model_ref = "model://fixture-large"

[runtime]
default_cwd = "."
streaming = false

[runtime.overrides]
allow = ["default_cwd"]
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-profile-reject-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace)
        .with_catalog_openai_chat_completions("fixture", Some("fixture-large".to_string()));
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    metadata_store
        .upsert_provider(
            LlmProviderRecord::new(
                "fixture",
                ProviderApi::OpenAIChatCompletions,
                "https://example.invalid/v1",
            )
            .with_model(crate::LlmProviderModelRecord::new("fixture-small"))
            .with_model(crate::LlmProviderModelRecord::new("fixture-large")),
        )
        .await
        .unwrap();
    sync_catalog_provider_identity(&mut config, &metadata_store)
        .await
        .unwrap();
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::OpenAIChatCompletions,
        "fixture",
        "fixture-large",
    );
    let runtime_factory = runtime_factory_from_provider_parts(
        runtime_config,
        Arc::new(InspectingCapsuleClient::default()),
        CapsuleBindingsConfig::default(),
    );
    let app = CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let model_err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://profiles@latest",
                "model": "fixture-tiny"
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(model_err.code, -32602);
    assert!(model_err.message.contains("declared model profiles"));
    assert!(model_err.message.contains("small"));
    assert!(model_err.message.contains("fixture-small"));
    assert!(model_err.message.contains("large"));
    assert!(model_err.message.contains("fixture-large"));

    let case_err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://profiles@latest",
                "model": "Fixture-Small"
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(case_err.code, -32602);
    assert!(case_err.message.contains("declared model profiles"));
    assert!(case_err.message.contains("fixture-small"));

    let provider_err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://profiles@latest",
                "modelProvider": "other"
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(provider_err.code, -32602);
    assert!(provider_err.message.contains("declared model profiles"));
    assert!(provider_err.message.contains("provider=fixture"));

    let ambiguous_provider_err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://profiles@latest",
                "modelProvider": "fixture"
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(ambiguous_provider_err.code, -32602);
    assert!(
        ambiguous_provider_err
            .message
            .contains("ambiguous declared model profile selection")
    );
    assert!(
        ambiguous_provider_err
            .message
            .contains("declared model profiles")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn default_manifest_publish_is_idempotent_and_patch_bumps_on_model_change() {
    let root = unique_test_root("app-server-default-manifest-idempotent");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");

    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-default-manifest-idem-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.clone();
    let _ = CooldisAppServer::new_local(config.clone()).await.unwrap();

    let registry = LocalAgentRegistry::new(&agent_registry_root);
    let first = registry.load_ref("agent://cooldis/default@latest").unwrap();
    assert_eq!(first.version, "1.0.0");
    assert!(
        first
            .authored_source
            .as_deref()
            .is_some_and(|source| source.contains("name = \"default\""))
    );
    assert_eq!(default_agent_version_count(&agent_registry_root), 1);

    let _ = CooldisAppServer::new_local(config.clone()).await.unwrap();
    let second = registry.load_ref("agent://cooldis/default@latest").unwrap();
    assert_eq!(second.version, "1.0.0");
    assert_eq!(second.manifest_hash, first.manifest_hash);
    assert_eq!(default_agent_version_count(&agent_registry_root), 1);

    config.model = "echo-v2".to_string();
    let _ = CooldisAppServer::new_local(config).await.unwrap();
    let third = registry.load_ref("agent://cooldis/default@latest").unwrap();
    assert_eq!(third.version, "1.0.1");
    assert_ne!(third.manifest_hash, first.manifest_hash);
    assert_eq!(
        third.resolved_manifest["model_profiles"][0]["model_ref"].as_str(),
        Some("model://local_offline/echo-v2")
    );
    assert_eq!(default_agent_version_count(&agent_registry_root), 2);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn startup_publishes_cooldis_threads_and_default_manifest_direct_rows() {
    use crate::EventStore;

    let root = unique_test_root("app-server-default-thread-ops");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root),
    )
    .await;

    let operation_record = LocalOperationRegistry::new(&operation_registry_root)
        .load_record(COOLDIS_THREADS_PACKAGE)
        .expect("startup should publish cooldis-threads");
    assert!(matches!(
        &operation_record.source,
        PublishedOperationSource::Kernel { package } if package == COOLDIS_THREADS_PACKAGE
    ));
    assert_eq!(
        operation_record
            .metadata
            .get(OPERATION_METADATA_RUNTIME_KIND)
            .and_then(Value::as_str),
        Some(KERNEL_RUNTIME_KIND)
    );
    let expected_operations = thread_operation_names();
    assert_eq!(
        operation_record
            .manifest
            .operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<Vec<_>>(),
        expected_operations
    );
    let schedule_record = LocalOperationRegistry::new(&operation_registry_root)
        .load_record(COOLDIS_SCHEDULE_PACKAGE)
        .expect("startup should publish cooldis-schedule");
    assert!(matches!(
        &schedule_record.source,
        PublishedOperationSource::Kernel { package } if package == COOLDIS_SCHEDULE_PACKAGE
    ));
    assert_eq!(
        schedule_record
            .metadata
            .get(OPERATION_METADATA_RUNTIME_KIND)
            .and_then(Value::as_str),
        Some(KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        schedule_record
            .manifest
            .operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            MANDATE_START_OPERATION,
            MANDATE_REVOKE_OPERATION,
            MANDATE_LIST_OPERATION,
        ]
    );
    assert_eq!(
        schedule_record.capability_grants,
        BTreeSet::from([
            SCHEDULE_MANAGE_CAPABILITY.to_string(),
            SCHEDULE_READ_CAPABILITY.to_string()
        ])
    );
    let process_record = LocalOperationRegistry::new(&operation_registry_root)
        .load_record(COOLDIS_PROCESS_PACKAGE)
        .expect("startup should publish cooldis-process");
    assert!(matches!(
        &process_record.source,
        PublishedOperationSource::Kernel { package } if package == COOLDIS_PROCESS_PACKAGE
    ));
    assert_eq!(
        process_record
            .metadata
            .get(OPERATION_METADATA_RUNTIME_KIND)
            .and_then(Value::as_str),
        Some(KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        process_record
            .manifest
            .operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            PROCESS_EXEC_OPERATION,
            PROCESS_POLL_OPERATION,
            PROCESS_WRITE_OPERATION,
            PROCESS_TERMINATE_OPERATION,
        ]
    );
    let notify_record = LocalOperationRegistry::new(&operation_registry_root)
        .load_record(COOLDIS_NOTIFY_PACKAGE)
        .expect("startup should publish cooldis-notify");
    assert!(matches!(
        &notify_record.source,
        PublishedOperationSource::Kernel { package } if package == COOLDIS_NOTIFY_PACKAGE
    ));
    assert_eq!(
        notify_record
            .metadata
            .get(OPERATION_METADATA_RUNTIME_KIND)
            .and_then(Value::as_str),
        Some(KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        notify_record
            .manifest
            .operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<Vec<_>>(),
        vec![NOTIFY_PREVIEW_OPERATION, CHANNEL_EMIT_OPERATION]
    );

    let agent = LocalAgentRegistry::new(root.join("agents"))
        .load_ref(default_manifest::DEFAULT_AGENT_REF)
        .expect("default manifest should publish");
    let tools = agent.resolved_manifest["tools"].as_array().unwrap();
    assert_eq!(tools.len(), expected_operations.len());
    assert!(tools.iter().all(|tool| {
        !tool["operation_ref"].as_str().is_some_and(|operation_ref| {
            operation_ref.contains(COOLDIS_PROCESS_PACKAGE)
                || operation_ref.contains(COOLDIS_NOTIFY_PACKAGE)
                || operation_ref.contains(COOLDIS_SCHEDULE_PACKAGE)
        })
    }));
    for operation in expected_operations {
        let row = tools
            .iter()
            .find(|tool| tool["tool_name"].as_str() == Some(operation))
            .unwrap_or_else(|| panic!("missing direct tool row for {operation}"));
        assert_eq!(row["type"].as_str(), Some("direct_tool"));
        assert_eq!(
            row["operation_ref"].as_str(),
            Some(
                format!(
                    "op://{COOLDIS_THREADS_PACKAGE}/{operation}@sha256:{}",
                    operation_record.active_artifact_hash
                )
                .as_str()
            )
        );
        assert_eq!(
            json_array_string_set(&row["grants"]),
            BTreeSet::from([thread_operation_capability(operation).to_string()])
        );
    }

    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(&thread_id).unwrap())
        .await
        .unwrap()
        .expect("default manifest thread should persist lifecycle metadata");
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let stream_id = EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    let binding = &bind.payload["operation_bindings"][0];
    assert_eq!(binding["name"].as_str(), Some(COOLDIS_THREADS_PACKAGE));
    assert!(
        bind.payload
            .get("operation_bindings")
            .and_then(Value::as_array)
            .unwrap()
            .iter()
            .all(|binding| {
                !matches!(
                    binding["name"].as_str(),
                    Some(
                        COOLDIS_PROCESS_PACKAGE | COOLDIS_NOTIFY_PACKAGE | COOLDIS_SCHEDULE_PACKAGE
                    )
                )
            })
    );
    assert_eq!(
        binding["artifact_hash"].as_str(),
        Some(operation_record.active_artifact_hash.as_str())
    );
    assert_eq!(
        json_array_string_set(&binding["operations"]),
        thread_operation_names()
            .iter()
            .map(|operation| (*operation).to_string())
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(
        json_array_string_set(&binding["grants"]),
        BTreeSet::from([
            THREADS_CONTROL_CAPABILITY.to_string(),
            THREADS_READ_CAPABILITY.to_string(),
            THREADS_SPAWN_CAPABILITY.to_string()
        ])
    );
    let direct_tools = binding["direct_tools"].as_array().unwrap();
    assert_eq!(direct_tools.len(), thread_operation_names().len());
    for direct_tool in direct_tools {
        assert_eq!(
            direct_tool["tool_name"].as_str(),
            direct_tool["operation"].as_str()
        );
    }

    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "inspect thread tools", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();
    wait_for_provider_requests(&client, 1).await;
    let requests = client.requests();
    let names = tool_names(&requests[0]);
    assert_eq!(
        names
            .iter()
            .filter(|name| name.starts_with("thread_"))
            .cloned()
            .collect::<BTreeSet<_>>(),
        thread_operation_names()
            .iter()
            .map(|operation| (*operation).to_string())
            .collect::<BTreeSet<_>>()
    );
    assert!(!names.iter().any(|name| name.starts_with("cooldis_")));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_with_child_agents_disabled_gets_no_thread_tools_without_rows() {
    let root = unique_test_root("app-server-researcher-thread-tools");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("researcher.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "researcher"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[policies]
allow_child_agents = false

[runtime]
default_cwd = "."
streaming = false
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();

    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://researcher@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "inspect researcher tools", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    wait_for_provider_requests(&client, 1).await;
    let requests = client.requests();
    let names = tool_names(&requests[0]);
    assert!(!names.iter().any(|name| name.starts_with("thread_")));
    assert!(!names.iter().any(|name| name == "bash"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn skill_resource_static_index_and_bodies_are_available_in_live_turn() {
    let root = unique_test_root("app-server-skill-resource");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let skill_registry_root = root.join("skills");
    let package_dir = root.join("skill-src").join("karl-skills");
    write_skill_fixture(
        &package_dir,
        "alpha",
        r#"---
name: alpha
description: Alpha description.
---
# Alpha

Alpha body marker.
"#,
    );
    let skill_record = LocalSkillRegistry::new(&skill_registry_root)
        .publish_directory(PublishSkillPackageRequest {
            package_dir: package_dir.clone(),
            name: None,
        })
        .unwrap();
    let manifest_path = root.join("skill-runner.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "skill-runner"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[resources]]
name = "karl_skills"
kind = "skill"
ref = "skill://karl-skills"

[runtime]
default_cwd = "."
streaming = false
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();

    let client = Arc::new(SkillResourceClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-skill-resource-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    config.skill_registry_root = skill_registry_root.clone();
    let mut runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    runtime_config.max_tokens = 128;
    let runtime_factory = runtime_factory_from_provider_parts_with_app_paths(
        runtime_config,
        provider_client,
        config.capsule_bindings.clone(),
        None,
        &config,
    );
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    let app = CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap();
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://skill-runner@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    std::fs::write(
        package_dir.join("alpha/SKILL.md"),
        "# Alpha\n\nChanged description.\n\nChanged body marker.\n",
    )
    .unwrap();
    let changed_skill_record = LocalSkillRegistry::new(&skill_registry_root)
        .publish_directory(PublishSkillPackageRequest {
            package_dir,
            name: None,
        })
        .unwrap();
    assert_ne!(
        changed_skill_record.active_artifact_hash,
        skill_record.active_artifact_hash
    );
    let parsed_thread_id = ThreadId::parse_str(&thread_id).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(parsed_thread_id)
        .await
        .unwrap()
        .expect("thread/start should persist the skill-bound lifecycle");
    app.inner
        .supervisor
        .shutdown_thread_at(&lifecycle.coordinates)
        .await
        .unwrap();
    app.inner.state.write().await.threads.remove(&thread_id);
    app.dispatch_request(
        &connection,
        "thread/resume",
        Some(json!({
            "threadId": thread_id,
            "excludeTurns": true,
        })),
    )
    .await
    .unwrap();
    let resumed_handle = app.handle_for_thread(&thread_id).await.unwrap();
    let (_, resumed_bind_receipt) = active_manifest_receipt_payloads(&resumed_handle)
        .await
        .unwrap()
        .expect("the resumed thread must retain a manifest bind witness");
    assert_eq!(
        resumed_bind_receipt["skill_packages"][0]["artifact_hash"].as_str(),
        Some(skill_record.active_artifact_hash.as_str()),
        "resume must retain the source thread's pinned skill binding"
    );
    let fork = app
        .dispatch_request(
            &connection,
            "thread/fork",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    let fork_id = fork["thread"]["id"].as_str().unwrap();
    let fork_handle = app.handle_for_thread(fork_id).await.unwrap();
    let fork_bindings = thread_manifest_skill_packages(fork_handle.context()).unwrap();
    assert_eq!(fork_bindings.len(), 1);
    assert_eq!(
        fork_bindings[0].artifact_hash, skill_record.active_artifact_hash,
        "an ordinary fork must inherit the source thread's pinned skill binding"
    );
    assert!(
        fork_handle
            .context()
            .metadata
            .contains_key(THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA)
    );
    let (_, fork_bind_receipt) = active_manifest_receipt_payloads(&fork_handle)
        .await
        .unwrap()
        .expect("an ordinary fork must inherit the source manifest receipts");
    assert_eq!(
        fork_bind_receipt["skill_packages"][0]["artifact_hash"].as_str(),
        Some(skill_record.active_artifact_hash.as_str())
    );
    let turn = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "read skill body", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    wait_for_provider_requests(&client, 2).await;
    wait_for_turn_completed_notification(
        &mut outbound_rx,
        &thread_id,
        turn["turn"]["id"].as_str().unwrap(),
    )
    .await;

    let context_page =
        wait_for_event_kind(&app, &connection, &thread_id, "context.compile.completed").await;
    let segment = &context_page["data"][0]["payload"]["static_context_segments"][0];
    assert_eq!(segment["id"].as_str(), Some("skill-index:karl_skills"));
    assert_eq!(
        segment["assembler"].as_str(),
        Some("kernel://assembler/static")
    );
    assert_eq!(segment["input"].as_str(), Some("karl_skills"));
    assert_eq!(segment["pinned"].as_bool(), Some(true));
    assert_eq!(segment["budget_share"], Value::Null);
    assert_eq!(
        segment["ref_uri"].as_str(),
        Some(skill_record.ref_uri().as_str())
    );
    assert!(
        segment["content_sha256"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );

    let bind_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "kinds": ["manifest.bind.completed"],
            })),
        )
        .await
        .unwrap();
    let binding = &bind_page["data"][0]["payload"]["skill_packages"][0];
    assert_eq!(binding["resource_name"].as_str(), Some("karl_skills"));
    let expected_package_digest = format!("sha256:{}", skill_record.active_artifact_hash);
    assert_eq!(
        binding["package_digest"].as_str(),
        Some(expected_package_digest.as_str())
    );
    assert_eq!(
        binding["index_sha256"].as_str(),
        segment["content_sha256"].as_str()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn workspace_skill_discovery_is_pinned_across_resume_and_fork_but_body_reads_stay_live() {
    let root = unique_test_root("app-server-workspace-skill-discovery");
    let app_cwd = root.join("app-cwd");
    let host_workspace = root.join("host-workspace");
    std::fs::create_dir_all(&app_cwd).unwrap();
    write_skill_fixture(
        &host_workspace.join(".agents/skills"),
        "alpha",
        r#"---
name: alpha
description: Original discovery description.
---
# Alpha

Original discovery body.
"#,
    );
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("workspace-skill-runner.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "workspace-skill-runner"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[workspace]
guest_path = "/work"
min_mode = "rw"

[skills]
discover = true

[runtime]
default_cwd = "/work"
streaming = false
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();

    let client = Arc::new(WorkspaceSkillDiscoveryClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-workspace-skill-discovery-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, &app_cwd);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    config.default_workspace = Some(AgentManifestWorkspaceBinding {
        host_path: host_workspace.clone(),
        mode: crate::AgentManifestWorkspaceMode::ReadWrite,
    });
    let mut runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    runtime_config.max_tokens = 128;
    let runtime_factory = runtime_factory_from_provider_parts_with_app_paths(
        runtime_config,
        provider_client,
        // lexicon-allow: capsule - existing app-server compatibility config field
        config.capsule_bindings.clone(),
        None,
        &config,
    );
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    let app = CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap();
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://workspace-skill-runner@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let initial_handle = app.handle_for_thread(&thread_id).await.unwrap();
    let (_, initial_bind_receipt) = active_manifest_receipt_payloads(&initial_handle)
        .await
        .unwrap()
        .expect("initial bind receipt");
    let original_hash = initial_bind_receipt["skill_discovery"]["skills"][0]["content_sha256"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(original_hash.starts_with("sha256:"));

    std::fs::write(
        host_workspace.join(".agents/skills/alpha/SKILL.md"),
        r#"---
name: alpha
description: Changed discovery description.
---
# Alpha

Changed discovery body marker.
"#,
    )
    .unwrap();
    let parsed_thread_id = ThreadId::parse_str(&thread_id).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(parsed_thread_id)
        .await
        .unwrap()
        .expect("thread/start should persist discovery metadata");
    app.inner
        .supervisor
        .shutdown_thread_at(&lifecycle.coordinates)
        .await
        .unwrap();
    app.inner.state.write().await.threads.remove(&thread_id);
    app.dispatch_request(
        &connection,
        "thread/resume",
        Some(json!({
            "threadId": thread_id,
            "excludeTurns": true,
        })),
    )
    .await
    .unwrap();

    let resumed_handle = app.handle_for_thread(&thread_id).await.unwrap();
    let resumed_discovery = thread_manifest_skill_discovery(resumed_handle.context())
        .unwrap()
        .expect("resumed discovery metadata");
    assert_eq!(resumed_discovery.skills[0].content_sha256, original_hash);
    assert_eq!(
        resumed_discovery.skills[0].description,
        "Original discovery description."
    );
    let (_, resumed_bind_receipt) = active_manifest_receipt_payloads(&resumed_handle)
        .await
        .unwrap()
        .expect("resumed bind receipt");
    assert_eq!(
        resumed_bind_receipt["skill_discovery"]["skills"][0]["content_sha256"].as_str(),
        Some(original_hash.as_str())
    );

    let checkpoint = app
        .inner
        .supervisor
        .create_checkpoint_at(
            &resumed_handle.context().coordinates,
            None,
            Some("workspace-skill-explicit-fork".to_string()),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let fork = app
        .dispatch_request(
            &connection,
            "thread/fork",
            Some(json!({
                "threadId": thread_id,
                "checkpointId": checkpoint.id.to_string(),
            })),
        )
        .await
        .unwrap();
    let fork_id = fork["thread"]["id"].as_str().unwrap();
    let fork_handle = app.handle_for_thread(fork_id).await.unwrap();
    let fork_discovery = thread_manifest_skill_discovery(fork_handle.context())
        .unwrap()
        .expect("fork discovery metadata");
    assert_eq!(fork_discovery.skills[0].content_sha256, original_hash);
    let (_, fork_bind_receipt) = active_manifest_receipt_payloads(&fork_handle)
        .await
        .unwrap()
        .expect("fork bind receipt");
    assert_eq!(
        fork_bind_receipt["skill_discovery"]["skills"][0]["content_sha256"].as_str(),
        Some(original_hash.as_str())
    );

    let turn = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "read the discovered skill", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    wait_for_provider_requests(&client, 2).await;
    wait_for_turn_completed_notification(
        &mut outbound_rx,
        &thread_id,
        turn["turn"]["id"].as_str().unwrap(),
    )
    .await;
    let requests = client.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let first_request_text = text_from_canonical_messages(&requests[0].messages);
    assert!(
        first_request_text
            .contains("alpha — Original discovery description. — .agents/skills/alpha/SKILL.md")
    );
    assert!(!first_request_text.contains("Changed discovery description."));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn explicit_context_folder_first_prompt_reaches_route_bound_agent_provider_request() {
    let root = unique_test_root("app-server-folder-first-prompt");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let operation =
        publish_echo_operation(&operation_registry_root, "lookup", "lookup", "lookup").await;
    let agent_registry_root = root.join("agents");
    let blob_registry_root = root.join("blobs");
    let project = root.join("prompt-runner");
    std::fs::create_dir_all(project.join("prompts")).unwrap();
    std::fs::write(
        project.join("prompts/system.md"),
        "You are the route-bound prompt runner.\n",
    )
    .unwrap();
    let manifest_path = project.join("cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "prompt-runner"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false

[[tools]]
type = "direct_tool"
id = "lookup"
tool_name = "lookup"
operation_ref = "op://lookup/lookup@sha256:{operation_hash}"

[context]
[[context.pipelines]]
id = "default"

[[context.pipelines.sources]]
id = "identity"
assembler = "kernel://assembler/static"
pinned = true

[[context.pipelines.sources]]
id = "history"
assembler = "kernel://assembler/anchored-window"
select = {{ stream = "thread", since = "anchor|start" }}
budget_share = 0.75
"#,
            operation_hash = operation.active_artifact_hash
        ),
    )
    .unwrap();
    let record = LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, &operation_registry_root)
        .unwrap();
    let prompt_ref = record.resolved_manifest["resources"][0]["ref"]
        .as_str()
        .unwrap();
    let prompt_hash = prompt_ref
        .strip_prefix("resource://artifact/sha256:")
        .unwrap();
    let expected_prompt_digest = format!("sha256:{prompt_hash}");

    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-prompt-resource-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace).with_capsule_bindings(
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root),
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    config.blob_registry_root = blob_registry_root;
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let runtime_factory = runtime_factory_from_provider_parts_with_app_paths(
        runtime_config,
        provider_client,
        config.capsule_bindings.clone(),
        None,
        &config,
    );
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    let app = CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap();
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://prompt-runner@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "hello", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    wait_for_provider_requests(&client, 1).await;
    wait_for_turn_completed_notification(
        &mut outbound_rx,
        &thread_id,
        turn["turn"]["id"].as_str().unwrap(),
    )
    .await;

    let requests = client.requests();
    assert_eq!(
        requests[0].system[0].text,
        "You are the route-bound prompt runner.\n"
    );
    assert!(
        requests[0].system[1]
            .text
            .contains("You are running as agent://prompt-runner@0.1.0"),
        "{:?}",
        requests[0].system
    );
    let context_page =
        wait_for_event_kind(&app, &connection, &thread_id, "context.compile.completed").await;
    let segment = &context_page["data"][0]["payload"]["static_context_segments"][0];
    assert_eq!(segment["id"].as_str(), Some("identity"));
    assert_eq!(
        segment["content_sha256"].as_str(),
        Some(expected_prompt_digest.as_str())
    );
    let bind_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "kinds": ["manifest.bind.completed"],
            })),
        )
        .await
        .unwrap();
    let bound_segment = &bind_page["data"][0]["payload"]["static_context_segments"][0];
    assert_eq!(bound_segment["id"].as_str(), Some("identity"));
    assert_eq!(
        bound_segment["content_sha256"].as_str(),
        Some(expected_prompt_digest.as_str())
    );
    assert_eq!(
        bound_segment["content_sha256"].as_str(),
        segment["content_sha256"].as_str()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn child_agent_policy_rejects_manifest_thread_spawn_row() {
    let root = unique_test_root("app-server-thread-spawn-policy");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    let operation_record = crate::ensure_cooldis_threads_published(Some(&operation_registry_root))
        .unwrap()
        .expect("kernel package should publish for policy test");
    let manifest_path = root.join("blocked-spawn.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "blocked-spawn"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "direct_tool"
id = "thread_spawn"
tool_name = "thread_spawn"
operation_ref = "op://cooldis-threads/thread_spawn@sha256:{}"
grants = ["threads.spawn"]

[policies]
allow_child_agents = false

[runtime]
default_cwd = "."
streaming = false
"#,
            operation_record.active_artifact_hash
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, &operation_registry_root)
        .unwrap();

    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client;
    let app = test_app_with_provider_root(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://blocked-spawn@latest" })),
        )
        .await
        .unwrap_err();
    assert!(err.message.contains("allow_child_agents = false"));
    assert!(err.message.contains("thread_spawn"));
    assert!(err.message.contains("threads.spawn"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn schedule_manifest_direct_tool_starts_mandate_and_requires_grant() {
    let root = unique_test_root("app-server-schedule-direct-tool");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    let operation_record = crate::ensure_cooldis_schedule_published(Some(&operation_registry_root))
        .unwrap()
        .expect("kernel package should publish for schedule direct-tool test");

    let manifest_path = root.join("scheduler.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "scheduler"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "direct_tool"
id = "mandate_start"
tool_name = "mandate_start"
operation_ref = "op://cooldis-schedule/mandate_start@sha256:{}"
grants = ["{}"]

[runtime]
default_cwd = "."
streaming = false
"#,
            operation_record.active_artifact_hash, SCHEDULE_MANAGE_CAPABILITY
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, &operation_registry_root)
        .unwrap();

    let no_grant_manifest_path = root.join("scheduler-no-grant.cooldis.agent.toml");
    std::fs::write(
        &no_grant_manifest_path,
        format!(
            r#"
[agent]
name = "scheduler-no-grant"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "direct_tool"
id = "mandate_start"
tool_name = "mandate_start"
operation_ref = "op://cooldis-schedule/mandate_start@sha256:{}"
grants = []

[runtime]
default_cwd = "."
streaming = false
"#,
            operation_record.active_artifact_hash
        ),
    )
    .unwrap();
    let err = LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path_with_operation_registry(
            &no_grant_manifest_path,
            &operation_registry_root,
        )
        .unwrap_err();
    assert!(err.to_string().contains("requires grants"));
    assert!(err.to_string().contains("mandate_start:schedule.manage"));

    let client = Arc::new(ScheduleMandateStartClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://scheduler@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "remind me in a minute", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    wait_for_provider_requests(&client, 2).await;
    let list = app
        .dispatch_request(
            &connection,
            "mandate/list",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    let mandates = list["data"].as_array().unwrap();
    assert_eq!(mandates.len(), 1);
    assert_eq!(
        mandates[0]["schedule"],
        json!({ "interval": { "every_ms": 60_000 } })
    );
    assert_eq!(
        mandates[0]["inputTemplate"].as_str(),
        Some("remind me in a minute")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_spawn_agent_ref_binds_child_manifest() {
    let root = unique_test_root("app-server-thread-spawn-agent-ref");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    let worker_manifest_path = root.join("worker.cooldis.agent.toml");
    std::fs::write(
        &worker_manifest_path,
        r#"
[agent]
name = "worker"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://echo"

[runtime]
default_cwd = "."
streaming = false
max_tool_rounds = 64
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&worker_manifest_path)
        .unwrap();

    let client = Arc::new(ThreadSpawnAgentRefClient::new("agent://worker@latest"));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-thread-spawn-agent-ref-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, &workspace).with_capsule_bindings(
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root),
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let mut runtime_config = AgentLoopConfig::new(
        ProviderApi::Other("local_offline".to_string()),
        "local_offline",
        "echo",
    );
    runtime_config.max_tokens = 128;
    let metadata_path = config.metadata_store_path();
    let runtime_factory = runtime_factory_from_provider_parts_with_app_paths(
        runtime_config,
        provider_client,
        config.capsule_bindings.clone(),
        None,
        &config,
    );
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    let app = CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let root_thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": root_thread_id,
            "input": [{ "type": "text", "text": "spawn worker with manifest", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    wait_for_provider_requests(&client, 3).await;
    let root_id = ThreadId::parse_str(&root_thread_id).unwrap();
    let list = app
        .dispatch_request(&connection, "thread/list", None)
        .await
        .unwrap();
    let child_thread = list["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|thread| thread["parentThreadId"].as_str() == Some(root_thread_id.as_str()))
        .expect("thread/list should expose the spawned child thread");
    let child_thread_id = child_thread["id"].as_str().unwrap().to_string();

    let metadata_store = SqliteMetadataStore::open(metadata_path).await.unwrap();
    let child_record = metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(&child_thread_id).unwrap())
        .await
        .unwrap()
        .expect("thread_spawn should persist child thread lifecycle metadata");
    assert_eq!(child_record.parent_thread_id, Some(root_id));
    assert_eq!(
        child_record
            .metadata
            .get(THREAD_AGENT_REF_METADATA)
            .map(String::as_str),
        Some("agent://worker@0.1.0")
    );
    assert_eq!(
        child_record
            .metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .map(|hash| hash.starts_with("sha256:")),
        Some(true)
    );
    assert_eq!(
        child_record
            .metadata
            .get(THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA)
            .map(String::as_str),
        Some("64")
    );

    let events = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({ "threadId": child_thread_id })),
        )
        .await
        .unwrap();
    assert!(
        events["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["kind"].as_str() == Some("manifest.bind.completed")),
        "thread/events/list should expose the child manifest bind receipt"
    );

    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": root_thread_id,
            "input": [{ "type": "text", "text": "cancel worker", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();
    wait_for_provider_requests(&client, 5).await;
    let child_id = ThreadId::parse_str(&child_thread_id).unwrap();
    let stopped_child_record = wait_for_lifecycle_status(
        &metadata_store,
        child_id,
        crate::ThreadLifecycleStatus::Stopped,
    )
    .await;
    assert_eq!(stopped_child_record.parent_thread_id, Some(root_id));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn default_manifest_publish_recovers_partial_version_without_latest() {
    let root = unique_test_root("app-server-default-manifest-partial");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");

    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-default-manifest-partial-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.clone();
    default_manifest::ensure_default_manifest_published(&config, false).unwrap();

    let registry = LocalAgentRegistry::new(&agent_registry_root);
    std::fs::remove_file(
        registry
            .alias_record_path(default_manifest::DEFAULT_AGENT_NAME, "latest")
            .unwrap(),
    )
    .unwrap();
    std::fs::remove_file(
        registry
            .record_path(default_manifest::DEFAULT_AGENT_NAME)
            .unwrap(),
    )
    .unwrap();

    config.model = "echo-v2".to_string();
    let recovered = default_manifest::ensure_default_manifest_published(&config, false).unwrap();

    assert_eq!(recovered.version, "1.0.1");
    assert_eq!(default_agent_version_count(&agent_registry_root), 2);
    let latest = registry
        .load_ref(default_manifest::DEFAULT_AGENT_REF)
        .unwrap();
    assert_eq!(latest.version, "1.0.1");
    assert_eq!(
        latest.resolved_manifest["model_profiles"][0]["model_ref"].as_str(),
        Some("model://local_offline/echo-v2")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn default_manifest_publish_serializes_concurrent_startup() {
    let root = unique_test_root("app-server-default-manifest-concurrent");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();

    for model in ["echo-a", "echo-b"] {
        let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
            "cooldis-default-manifest-concurrent-{}.sock",
            Uuid::now_v7()
        )));
        let mut config = CooldisAppServerConfig::local(listen, &workspace);
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        config.agent_registry_root = agent_registry_root.clone();
        config.model = model.to_string();
        let barrier = std::sync::Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            default_manifest::ensure_default_manifest_published(&config, false)
        }));
    }

    let records = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    let versions = records
        .iter()
        .map(|record| record.version.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(versions, BTreeSet::from(["1.0.0", "1.0.1"]));

    let registry = LocalAgentRegistry::new(&agent_registry_root);
    let latest = registry
        .load_ref(default_manifest::DEFAULT_AGENT_REF)
        .unwrap();
    assert_eq!(latest.version, "1.0.1");
    assert_eq!(default_agent_version_count(&agent_registry_root), 2);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn default_manifest_thread_rebinds_after_config_model_changes() {
    let root = unique_test_root("app-server-default-manifest-restore-model");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let metadata_path;
    let thread_id;
    let first_record;

    {
        let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
            "cooldis-default-manifest-restore-a-{}.sock",
            Uuid::now_v7()
        )));
        let mut config = CooldisAppServerConfig::local(listen, &workspace);
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        config.agent_registry_root = agent_registry_root.clone();
        config.model = "echo-v1".to_string();
        metadata_path = config.metadata_store_path();
        let app = CooldisAppServer::new_local(config).await.unwrap();
        let (connection, _outbound_rx) = test_connection(app.clone());
        initialize_for_test(&connection).await;

        let thread_start = app
            .dispatch_request(&connection, "thread/start", Some(json!({})))
            .await
            .unwrap();
        thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
        first_record = LocalAgentRegistry::new(&agent_registry_root)
            .load_ref(default_manifest::DEFAULT_AGENT_REF)
            .unwrap();
        assert_eq!(first_record.version, "1.0.0");
        assert_eq!(
            first_record.resolved_manifest["model_profiles"][0]["model_ref"].as_str(),
            Some("model://local_offline/echo-v1")
        );
    }

    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-default-manifest-restore-b-{}.sock",
        Uuid::now_v7()
    )));
    let mut restarted_config = CooldisAppServerConfig::local(listen, &workspace);
    restarted_config.runtime_home = root.join("runtime");
    restarted_config.state_home = root.join("state");
    restarted_config.agent_registry_root = agent_registry_root.clone();
    restarted_config.model = "echo-v2".to_string();
    let restarted = CooldisAppServer::new_local(restarted_config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&connection).await;

    let latest = LocalAgentRegistry::new(&agent_registry_root)
        .load_ref(default_manifest::DEFAULT_AGENT_REF)
        .unwrap();
    assert_eq!(latest.version, "1.0.1");
    assert_eq!(
        latest.resolved_manifest["model_profiles"][0]["model_ref"].as_str(),
        Some("model://local_offline/echo-v2")
    );

    let resume = restarted
        .dispatch_request(
            &connection,
            "thread/resume",
            Some(json!({
                "threadId": thread_id,
                "excludeTurns": true,
            })),
        )
        .await
        .unwrap();
    assert_eq!(resume["thread"]["id"].as_str(), Some(thread_id.as_str()));

    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let lifecycle = SqliteMetadataStore::open(metadata_path)
        .await
        .unwrap()
        .get_thread_lifecycle(parsed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_REF_METADATA],
        first_record.ref_uri
    );
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_MANIFEST_HASH_METADATA],
        first_record.manifest_hash
    );
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_MODEL_ID_METADATA],
        "echo-v1"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_startup_skips_stale_manifest_threads() {
    let root = unique_test_root("app-server-stale-manifest-startup");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let metadata_path;
    let thread_id;

    {
        let listen = AppServerListenAddr::Unix(
            std::env::temp_dir().join(format!("cooldis-stale-manifest-a-{}.sock", Uuid::now_v7())),
        );
        let mut config = CooldisAppServerConfig::local(listen, &workspace);
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        config.agent_registry_root = agent_registry_root.clone();
        metadata_path = config.metadata_store_path();
        let app = CooldisAppServer::new_local(config).await.unwrap();
        let (connection, _outbound_rx) = test_connection(app.clone());
        initialize_for_test(&connection).await;

        let thread_start = app
            .dispatch_request(&connection, "thread/start", Some(json!({})))
            .await
            .unwrap();
        thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    }

    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let store = SqliteMetadataStore::open(&metadata_path).await.unwrap();
    let mut lifecycle = store
        .get_thread_lifecycle(parsed)
        .await
        .unwrap()
        .expect("thread/start should persist lifecycle metadata");
    lifecycle.metadata.insert(
        THREAD_AGENT_MANIFEST_HASH_METADATA.to_string(),
        "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string(),
    );
    store.upsert_thread_lifecycle(lifecycle).await.unwrap();
    drop(store);

    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-stale-manifest-b-{}.sock", Uuid::now_v7())),
    );
    let mut restarted_config = CooldisAppServerConfig::local(listen, &workspace);
    restarted_config.runtime_home = root.join("runtime");
    restarted_config.state_home = root.join("state");
    restarted_config.agent_registry_root = agent_registry_root.clone();
    let restarted = CooldisAppServer::new_local(restarted_config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&connection).await;

    let err = restarted
        .dispatch_request(
            &connection,
            "thread/resume",
            Some(json!({
                "threadId": thread_id,
                "excludeTurns": true,
            })),
        )
        .await
        .unwrap_err();
    assert!(
        err.message.contains("manifest thread stored hash"),
        "unexpected resume error: {err:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_start_with_agent_ref_records_manifest_receipts_before_turns() {
    use crate::EventStore;

    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-manifest-start-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-manifest-start");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("local.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "local-runner"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://echo"

[runtime]
default_cwd = "agent-workspace"
streaming = true

[runtime.overrides]
allow = ["streaming"]
"#,
    )
    .unwrap();
    let record = LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();

    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let metadata_path = config.metadata_store_path();
    let session_path = config.state_home.join("session_history.sqlite3");
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://local-runner@latest",
                "runtimeOverrides": {
                    "streaming": false
                }
            })),
        )
        .await
        .unwrap();
    let thread_id = ThreadId::parse_str(thread_start["thread"]["id"].as_str().unwrap())
        .expect("thread/start should return a thread id");
    assert_eq!(thread_start["model"].as_str(), Some(APP_SERVER_LOCAL_MODEL));
    assert_eq!(
        thread_start["modelProvider"].as_str(),
        Some(APP_SERVER_LOCAL_PROVIDER)
    );
    assert_eq!(
        thread_start["cwd"].as_str(),
        Some(cwd_string(&workspace.join("agent-workspace")).as_str())
    );

    let metadata_store = SqliteMetadataStore::open(metadata_path).await.unwrap();
    let lifecycle = metadata_store
        .get_thread_lifecycle(thread_id)
        .await
        .unwrap()
        .expect("manifest start should persist lifecycle metadata");
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_REF_METADATA],
        record.ref_uri
    );
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_MANIFEST_HASH_METADATA],
        record.manifest_hash
    );

    let session_store = crate::SqliteSessionStore::open(session_path).await.unwrap();
    let stream_id = crate::EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 4);
    let compile = event_by_kind(&events, crate::EventKind::ManifestCompileCompleted);
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    assert_eq!(compile.origin, crate::EventOrigin::Discharged);
    assert_eq!(bind.origin, crate::EventOrigin::Discharged);
    assert_eq!(
        compile.provenance.discharged_by.as_deref(),
        Some(crate::MANIFEST_COMPILER_DISCHARGED_BY)
    );
    assert_eq!(
        bind.provenance.discharged_by.as_deref(),
        Some(crate::MANIFEST_BINDER_DISCHARGED_BY)
    );
    assert_eq!(bind.provenance.source_event_ids, vec![compile.id]);
    assert_eq!(compile.payload["alias"]["alias"].as_str(), Some("latest"));
    assert_eq!(
        compile.payload["alias"]["manifest_hash"].as_str(),
        Some(record.manifest_hash.as_str())
    );
    assert_eq!(
        bind.payload["manifest_hash"].as_str(),
        Some(record.manifest_hash.as_str())
    );
    assert_eq!(
        bind.payload["overridden_keys"].as_array().unwrap(),
        &vec![json!("streaming")]
    );
    assert_eq!(bind.payload["placement"]["target"], "local");
    let placement_events = events
        .iter()
        .filter(|event| event.kind == crate::EventKind::PlacementDecision)
        .collect::<Vec<_>>();
    assert_eq!(placement_events.len(), 1);
    assert_eq!(placement_events[0].origin, crate::EventOrigin::Witnessed);
    assert_eq!(placement_events[0].payload["placement"], "local");
    assert_eq!(
        placement_events[0].payload["snapshot_id"],
        record.manifest_hash
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn cancelled_manifest_lifecycle_caller_cannot_split_receipt_from_metadata() {
    let app = test_app().await;
    let bound = app
        .bind_app_server_agent_ref(
            default_manifest::DEFAULT_AGENT_REF,
            &AgentManifestModelProfileSelection::default(),
            &AgentManifestBindOverrides::default(),
            None,
            None,
        )
        .await
        .unwrap();
    let mut metadata = BTreeMap::new();
    append_bound_agent_metadata(&mut metadata, &bound, None, None).unwrap();
    let handle = app
        .inner
        .supervisor
        .start_thread(ThreadStartRequest {
            tenant_id: app.inner.tenant_id.clone(),
            user_id: app.inner.user_id.clone(),
            session_id: "manifest-lifecycle-cancel".to_string(),
            topology: ThreadTopology::root(),
            metadata,
        })
        .await
        .unwrap();
    wait_for_initial_thread_status(&handle).await;
    let thread_id = handle.context().coordinates.thread_id;
    let observer = handle.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let caller_app = app.clone();
    let operation_app = app.clone();
    let operation_entered = entered.clone();
    let operation_release = release.clone();
    let caller = tokio::spawn(async move {
        caller_app
            .witness_and_persist_lifecycle(handle, move |handle| async move {
                operation_entered.notify_one();
                operation_release.notified().await;
                record_bound_agent_receipts(&handle, &bound).await?;
                operation_app
                    .persist_thread_lifecycle_record_with_metadata(&handle, BTreeMap::new())
                    .await?;
                Ok(())
            })
            .await
    });

    entered.notified().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    release.notify_one();

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let has_receipt = observer
                .read_thread_events(None)
                .await
                .unwrap()
                .iter()
                .any(|event| event.kind == EventKind::ManifestBindCompleted);
            let has_lifecycle = app
                .inner
                .metadata_store
                .get_thread_lifecycle(thread_id)
                .await
                .unwrap()
                .is_some();
            if has_receipt && has_lifecycle {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("manifest receipt and lifecycle metadata did not finish after caller cancellation");
}

struct StartingOnlyRuntimeFactory {
    started: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::AgentRuntimeFactory for StartingOnlyRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::ThreadContext,
    ) -> crate::CooldisResult<Box<dyn crate::AgentRuntime>> {
        Ok(Box::new(StartingOnlyRuntime {
            started: self.started.clone(),
        }))
    }
}

struct StartingOnlyRuntime {
    started: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::AgentRuntime for StartingOnlyRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::ThreadContext,
        _services: crate::RuntimeServices,
        mut commands: mpsc::Receiver<crate::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::ThreadEvent>,
        _status: tokio::sync::watch::Sender<crate::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let _ = events.send(crate::ThreadEvent::Started { context });
        self.started.notify_one();
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = commands.recv() => {}
        }
    }
}

#[tokio::test]
async fn thread_start_witnesses_workspace_before_waiting_for_initial_status() {
    let root = unique_test_root("app-server-workspace-starting-witness");
    let app_cwd = root.join("app-cwd");
    let host_workspace = root.join("host-workspace");
    let agent_registry_root = root.join("agents");
    std::fs::create_dir_all(&app_cwd).unwrap();
    std::fs::create_dir_all(&host_workspace).unwrap();
    let manifest_path = root.join("starting-workspace.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "starting-workspace"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[workspace]
guest_path = "/work"
min_mode = "rw"

[runtime]
default_cwd = "."
streaming = false
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();

    let listen = AppServerListenAddr::Unix(root.join("app.sock"));
    let mut config = CooldisAppServerConfig::local(listen, &app_cwd);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    config.default_workspace = Some(AgentManifestWorkspaceBinding {
        host_path: host_workspace,
        mode: crate::AgentManifestWorkspaceMode::ReadWrite,
    });
    let started = Arc::new(tokio::sync::Notify::new());
    let app = CooldisAppServer::with_runtime_factory(
        config,
        Arc::new(StartingOnlyRuntimeFactory {
            started: started.clone(),
        }),
    )
    .await
    .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let caller_app = app.clone();
    let caller_connection = connection.clone();
    let caller = tokio::spawn(async move {
        caller_app
            .dispatch_request(
                &caller_connection,
                "thread/start",
                Some(json!({"agentRef": "agent://starting-workspace@latest"})),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), started.notified())
        .await
        .expect("workspace runtime did not start");

    let coordinates = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let snapshot = app.inner.supervisor.lifecycle_snapshot().await;
            if let Some(record) = snapshot
                .tenants
                .iter()
                .flat_map(|tenant| &tenant.records)
                .find(|record| {
                    record
                        .metadata
                        .contains_key(THREAD_AGENT_WORKSPACE_METADATA)
                })
            {
                break record.coordinates.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("workspace runtime did not become resident");
    let handle = app
        .inner
        .supervisor
        .get_thread_at(&coordinates)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        loop {
            if handle
                .read_thread_events(None)
                .await
                .unwrap()
                .iter()
                .any(|event| {
                    event.kind == EventKind::ManifestBindCompleted
                        && event.payload["workspace"].is_object()
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("workspace bind was not witnessed before the initial-status wait");
    assert!(
        !caller.is_finished(),
        "thread/start should still be waiting for the runtime's initial status"
    );

    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if app
                .inner
                .metadata_store
                .get_thread_lifecycle(coordinates.thread_id)
                .await
                .unwrap()
                .is_some_and(|record| {
                    record
                        .metadata
                        .contains_key(THREAD_AGENT_WORKSPACE_METADATA)
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("workspace lifecycle metadata did not survive caller cancellation");
    let _ = app.inner.supervisor.shutdown_thread_at(&coordinates).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn protocol_tool_import_mounts_search_surface_and_records_receipts() {
    let root = unique_test_root("app-server-tool-universe");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    publish_agent_manifest(
        &root,
        &agent_registry_root,
        "mcp-runner",
        "MCP Runner",
        "Uses a protocol universe",
        &[r#"
[[tools]]
type = "protocol_tool_import"
id = "echo"
protocol = "mcp"
server_ref = "mcp://arcade"
"#
        .to_string()],
    );
    let (mcp_url, mcp_task) = spawn_app_mcp_http_fixture("string").await;
    let app = app_server_with_tool_client(&root, &workspace, &agent_registry_root, {
        Arc::new(UniverseCallingClient::default())
    })
    .await;
    SqliteMcpSourceRegistry::open_async(&app.inner.metadata_store_path)
        .await
        .unwrap()
        .upsert_source_async(
            crate::McpRemoteServerConfig::new(
                "arcade",
                crate::McpRemoteTransport::StreamableHttp,
                mcp_url,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://mcp-runner@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(parsed)
        .await
        .unwrap()
        .unwrap();
    assert!(
        lifecycle
            .metadata
            .contains_key(THREAD_AGENT_TOOL_UNIVERSES_METADATA)
    );

    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "use the universe", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();
    wait_for_session_text(&app, &thread_id, "universe completed").await;

    let discovery_page = wait_for_event_kind(
        &app,
        &connection,
        &thread_id,
        "tool.universe.discovery.completed",
    )
    .await;
    let discovery = &discovery_page["data"][0]["payload"];
    assert_eq!(discovery["server_ref"].as_str(), Some("mcp://arcade"));
    assert_eq!(
        discovery["tools"][0]["tool_name"].as_str(),
        Some("cooldis_mcp_echo")
    );
    assert!(
        discovery["tools"][0]["schema_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );

    let call_page = wait_for_event_kind(
        &app,
        &connection,
        &thread_id,
        "tool.universe.call.completed",
    )
    .await;
    let call = &call_page["data"][0]["payload"];
    assert_eq!(call["server_ref"].as_str(), Some("mcp://arcade"));
    assert_eq!(call["tool_name"].as_str(), Some("cooldis_mcp_echo"));
    assert_eq!(call["is_error"].as_bool(), Some(false));
    assert_eq!(
        call["output_hash"].as_str(),
        Some(crate::agent::contracts::sha256_hex(b"REMOTE_MCP_OK hello").as_str())
    );
    mcp_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn pinned_protocol_tool_import_projects_direct_row() {
    let root = unique_test_root("app-server-tool-universe-pin");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let schema = app_mcp_echo_schema("string");
    let schema_hash = crate::schema_hash_of(&schema).unwrap();
    publish_agent_manifest(
        &root,
        &agent_registry_root,
        "mcp-pinned",
        "MCP Pinned",
        "Uses a pinned protocol tool",
        &[format!(
            r#"
[[tools]]
type = "protocol_tool_import"
id = "echo"
protocol = "mcp"
server_ref = "mcp://arcade"
expose = ["direct_tool"]
pin = "mcptool://arcade/cooldis_mcp_echo@{schema_hash}"
"#
        )],
    );
    let (mcp_url, mcp_task) = spawn_app_mcp_http_fixture("string").await;
    let app = app_server_with_tool_client(&root, &workspace, &agent_registry_root, {
        Arc::new(PinnedDirectCallingClient::default())
    })
    .await;
    SqliteMcpSourceRegistry::open_async(&app.inner.metadata_store_path)
        .await
        .unwrap()
        .upsert_source_async(
            crate::McpRemoteServerConfig::new(
                "arcade",
                crate::McpRemoteTransport::StreamableHttp,
                mcp_url,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://mcp-pinned@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "use the pinned row", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    wait_for_session_text(&app, &thread_id, "pinned completed").await;
    let call_page = wait_for_event_kind(
        &app,
        &connection,
        &thread_id,
        "tool.universe.call.completed",
    )
    .await;
    assert_eq!(
        call_page["data"][0]["payload"]["tool_name"].as_str(),
        Some("cooldis_mcp_echo")
    );
    assert_eq!(
        call_page["data"][0]["payload"]["schema_hash"].as_str(),
        Some(schema_hash.as_str())
    );
    mcp_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_events_list_pages_filters_and_reports_clear_errors() {
    use crate::EventStore;

    let root = unique_test_root("app-server-thread-events-query");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    publish_agent_manifest(
        &root,
        &agent_registry_root,
        "event-runner",
        "Event Runner",
        "Produces event receipts",
        &[],
    );
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-event-query-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://event-runner@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();

    let first_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({ "threadId": thread_id, "limit": 1 })),
        )
        .await
        .unwrap();
    assert_eq!(first_page["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        first_page["data"][0]["kind"].as_str(),
        Some("session.entry.appended")
    );
    assert_eq!(
        first_page["data"][0]["schema"].as_str(),
        Some("cooldis.stream.record/1")
    );
    assert_eq!(
        first_page["data"][0]["payload_schema"].as_str(),
        Some("cooldis.event.session.entry.appended/1")
    );
    assert_eq!(first_page["data"][0]["sequence"].as_i64(), Some(1));
    assert_eq!(
        first_page["data"][0]["stream_id"].as_str(),
        Some(format!("thread:{thread_id}").as_str())
    );
    assert_eq!(
        first_page["data"][0]["event_id"].as_str(),
        first_page["data"][0]["eventId"].as_str()
    );
    assert_eq!(
        first_page["data"][0]["created_at_ms"].as_i64(),
        first_page["data"][0]["atMs"].as_i64()
    );
    let cursor = first_page["cursor"].as_str().unwrap().to_string();
    assert_eq!(
        first_page["streamCursor"]["schema"].as_str(),
        Some("cooldis.stream.cursor/1")
    );
    assert_eq!(
        first_page["streamCursor"]["stream_id"].as_str(),
        first_page["data"][0]["stream_id"].as_str()
    );
    assert_eq!(
        first_page["streamCursor"]["sequence"].as_i64(),
        first_page["data"][0]["sequence"].as_i64()
    );
    assert_eq!(
        first_page["streamCursor"]["event_id"].as_str(),
        first_page["data"][0]["event_id"].as_str()
    );
    let stream_cursor = first_page["streamCursor"].clone();

    let second_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({ "threadId": thread_id, "streamCursor": stream_cursor, "limit": 1 })),
        )
        .await
        .unwrap();
    assert_eq!(second_page["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        second_page["data"][0]["kind"].as_str(),
        Some("manifest.compile.completed")
    );
    assert_eq!(
        second_page["data"][0]["origin"].as_str(),
        Some("discharged")
    );
    assert!(second_page["data"][0]["provenance"].is_object());

    let legacy_cursor_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({ "threadId": thread_id, "cursor": cursor, "limit": 1 })),
        )
        .await
        .unwrap();
    assert_eq!(
        legacy_cursor_page["data"][0]["eventId"],
        second_page["data"][0]["eventId"]
    );

    let mut wrong_event_cursor = first_page["streamCursor"].clone();
    wrong_event_cursor["event_id"] = json!(second_page["data"][0]["event_id"].as_str().unwrap());
    let bad_stream_cursor = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "streamCursor": wrong_event_cursor,
                "limit": 1,
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(bad_stream_cursor.code, -32602);
    assert!(bad_stream_cursor.message.contains("cursor"));

    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "compile context", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    let context_page =
        wait_for_event_kind(&app, &connection, &thread_id, "context.compile.completed").await;
    let context_event = &context_page["data"][0];
    assert_eq!(
        context_event["kind"].as_str(),
        Some("context.compile.completed")
    );
    assert_eq!(
        context_event["payload_schema"].as_str(),
        Some("cooldis.event.context.compile.completed/1")
    );
    assert_eq!(context_event["origin"].as_str(), Some("discharged"));
    assert_eq!(
        context_event["provenance"]["discharged_by"].as_str(),
        Some("projection:context-compiler")
    );
    assert!(
        context_event["provenance"]["source_streams"]
            .as_array()
            .is_some_and(|streams| !streams.is_empty())
    );
    assert!(context_event["payload"].is_object());

    let listed_thread_id = ThreadId::parse_str(&thread_id).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(listed_thread_id)
        .await
        .unwrap()
        .unwrap();
    let session_store = crate::SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    session_store
        .append_events(
            &crate::EventStreamId::new(format!("control:{}", lifecycle.coordinates.thread_id)),
            vec![crate::NewEventRecord::witnessed(
                lifecycle.coordinates.clone(),
                crate::EventKind::MandateStarted,
                json!({
                    "subject": { "loop_id": "loop-1" },
                    "mandate_id": "mandate-1",
                    "snapshot_id": "snapshot-1",
                    "api_key": "raw-secret",
                }),
            )],
        )
        .await
        .unwrap();
    let control_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "stream": "control",
                "kinds": ["mandate.started"],
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        control_page["data"][0]["kind"].as_str(),
        Some("mandate.started")
    );
    assert_eq!(
        control_page["data"][0]["origin"].as_str(),
        Some("witnessed")
    );

    let thread_stream_id = crate::EventStreamId::for_thread(&lifecycle.coordinates);
    let control_stream_id =
        crate::EventStreamId::new(format!("control:{}", lifecycle.coordinates.thread_id));
    let request_event = crate::NewEventRecord::witnessed(
        lifecycle.coordinates.clone(),
        crate::EventKind::ToolCallRequested,
        json!({
            "subject": { "turn_id": "turn-pending", "call_id": "call-approval" },
            "snapshot_id": "snapshot-approval",
            "tool_name": "bash",
            "arguments": { "command": "deploy" },
        }),
    );
    let request_event_id = request_event.id;
    session_store
        .append_events(&thread_stream_id, vec![request_event])
        .await
        .unwrap();
    let suspended_event = crate::NewEventRecord::discharged(
        lifecycle.coordinates.clone(),
        crate::EventKind::ToolCallSuspended,
        json!({
            "schema": crate::EventKind::ToolCallSuspended.payload_schema_id(),
            "subject": { "turn_id": "turn-pending", "call_id": "call-approval" },
            "snapshot_id": "snapshot-approval",
            "approval_id": "approval-1",
            "reason": "operator approval required",
        }),
        crate::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_event_ids: vec![request_event_id],
            discharged_by: Some("coupling:test-approval".to_string()),
            function: Some("approval_wait/v1".to_string()),
            ..crate::EventProvenance::default()
        },
    );
    let suspended_event_id = suspended_event.id;
    let waiting_event = crate::NewEventRecord::discharged(
        lifecycle.coordinates.clone(),
        crate::EventKind::TurnWaiting,
        json!({
            "schema": crate::EventKind::TurnWaiting.payload_schema_id(),
            "turn_id": "turn-pending",
            "subject": { "turn_id": "turn-pending", "call_id": "call-approval" },
            "snapshot_id": "snapshot-approval",
            "waiting_on_event_id": suspended_event_id.to_string(),
            "approval_id": "approval-1",
            "reason": "operator approval required",
            "continuation": "tool.call",
        }),
        crate::EventProvenance {
            source_streams: vec![control_stream_id.clone()],
            source_event_ids: vec![suspended_event_id],
            discharged_by: Some("scheduler:tool-decision".to_string()),
            function: Some("tool_wait/v1".to_string()),
            ..crate::EventProvenance::default()
        },
    );
    session_store
        .append_events(&control_stream_id, vec![suspended_event, waiting_event])
        .await
        .unwrap();

    let approvals = app
        .dispatch_request(
            &connection,
            "thread/approvals/list",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    assert_eq!(approvals["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        approvals["data"][0]["approvalId"].as_str(),
        Some("approval-1")
    );
    assert_eq!(
        approvals["data"][0]["kind"].as_str(),
        Some("tool.call.suspended")
    );
    assert_eq!(
        approvals["data"][0]["turnId"].as_str(),
        Some("turn-pending")
    );
    let waiting = app
        .dispatch_request(
            &connection,
            "thread/waiting/list",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    assert_eq!(waiting["data"].as_array().unwrap().len(), 1);
    assert_eq!(waiting["data"][0]["kind"].as_str(), Some("turn.waiting"));
    let suspended_event_id_string = suspended_event_id.to_string();
    assert_eq!(
        waiting["data"][0]["waitingOnEventId"].as_str(),
        Some(suspended_event_id_string.as_str())
    );
    assert_eq!(
        waiting["data"][0]["approvalId"].as_str(),
        Some("approval-1")
    );
    let resolved = app
        .dispatch_request(
            &connection,
            "approval/resolve",
            Some(json!({
                "threadId": thread_id,
                "approvalId": "approval-1",
                "decision": "approved",
                "reason": "Reviewed by operator.",
            })),
        )
        .await
        .unwrap();
    assert_eq!(resolved["status"].as_str(), Some("resolved"));
    assert_eq!(resolved["approvalId"].as_str(), Some("approval-1"));
    assert_eq!(resolved["decision"].as_str(), Some("approved"));
    assert_eq!(
        resolved["streamId"].as_str(),
        Some(control_stream_id.as_str())
    );
    let approval_resolved = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "stream": "control",
                "kinds": ["approval.resolved"],
            })),
        )
        .await
        .unwrap();
    assert_eq!(approval_resolved["data"].as_array().unwrap().len(), 1);
    let approval_resolved_event = &approval_resolved["data"][0];
    let approval_resolved_event_id = approval_resolved_event["eventId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        resolved["eventId"].as_str(),
        Some(approval_resolved_event_id.as_str())
    );
    assert_eq!(
        approval_resolved_event["origin"].as_str(),
        Some("witnessed")
    );
    assert_eq!(
        approval_resolved_event["payload_schema"].as_str(),
        Some("cooldis.event.approval.resolved/1")
    );
    assert_eq!(
        approval_resolved_event["payload"]["subject"]["approval_id"].as_str(),
        Some("approval-1")
    );
    assert_eq!(
        approval_resolved_event["payload"]["snapshot_id"].as_str(),
        Some("snapshot-approval")
    );
    assert_eq!(
        approval_resolved_event["payload"]["approved"].as_bool(),
        Some(true)
    );
    assert_eq!(
        approval_resolved_event["payload"]["reason"].as_str(),
        Some("Reviewed by operator.")
    );
    let duplicate_resolution = app
        .dispatch_request(
            &connection,
            "approval/resolve",
            Some(json!({
                "threadId": thread_id,
                "approvalId": "approval-1",
                "decision": "approved",
                "reason": "Reviewed by operator.",
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        duplicate_resolution["status"].as_str(),
        Some("already_resolved")
    );
    assert_eq!(
        duplicate_resolution["eventId"].as_str(),
        Some(approval_resolved_event_id.as_str())
    );
    let conflicting_resolution = app
        .dispatch_request(
            &connection,
            "approval/resolve",
            Some(json!({
                "threadId": thread_id,
                "approvalId": "approval-1",
                "decision": "denied",
                "reason": "Changed mind.",
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(conflicting_resolution.code, -32602);
    assert!(
        conflicting_resolution
            .message
            .contains("approval approval-1 already resolved")
    );

    let decision_event = crate::NewEventRecord::discharged(
        lifecycle.coordinates.clone(),
        crate::EventKind::ToolCallDecision,
        json!({
            "subject": { "turn_id": "turn-pending", "call_id": "call-approval" },
            "snapshot_id": "snapshot-approval",
            "outcome": { "decision": "allow" },
        }),
        crate::EventProvenance {
            source_streams: vec![control_stream_id.clone()],
            source_event_ids: vec![suspended_event_id],
            discharged_by: Some("coupling:test-approval".to_string()),
            function: Some("approval_decision/v1".to_string()),
            ..crate::EventProvenance::default()
        },
    );
    let decision_event_id = decision_event.id;
    let resumed_event = crate::NewEventRecord::discharged(
        lifecycle.coordinates.clone(),
        crate::EventKind::TurnResumed,
        json!({
            "turn_id": "turn-pending",
            "consumed_fact_id": decision_event_id.to_string(),
        }),
        crate::EventProvenance {
            source_streams: vec![control_stream_id.clone()],
            source_event_ids: vec![decision_event_id],
            discharged_by: Some("scheduler:tool-decision".to_string()),
            function: Some("tool_resume/v1".to_string()),
            ..crate::EventProvenance::default()
        },
    );
    session_store
        .append_events(&control_stream_id, vec![decision_event, resumed_event])
        .await
        .unwrap();
    let approvals_after_decision = app
        .dispatch_request(
            &connection,
            "thread/approvals/list",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    assert!(
        approvals_after_decision["data"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let waiting_after_resume = app
        .dispatch_request(
            &connection,
            "thread/waiting/list",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    assert!(waiting_after_resume["data"].as_array().unwrap().is_empty());

    session_store
        .append_events(
            &crate::EventStreamId::new(format!(
                "derived:memory:{}",
                lifecycle.coordinates.thread_id
            )),
            vec![crate::NewEventRecord::discharged(
                lifecycle.coordinates.clone(),
                crate::EventKind::SessionEntryAppended,
                json!({ "fact": "likes receipts" }),
                crate::EventProvenance {
                    source_streams: vec![crate::EventStreamId::for_thread(&lifecycle.coordinates)],
                    discharged_by: Some("coupling:test-memory".to_string()),
                    function: Some("op://test/memory@sha256:test".to_string()),
                    ..crate::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    let derived_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "stream": "derived:memory",
                "kinds": ["session.entry.appended"],
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        derived_page["data"][0]["payload"]["fact"].as_str(),
        Some("likes receipts")
    );

    let export = app
        .dispatch_request(
            &connection,
            "thread/debug/export",
            Some(json!({
                "threadId": thread_id,
                "streams": ["thread", "control", "derived:memory"],
                "includeThread": false,
                "maxEventsPerStream": 1,
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        export["schema"].as_str(),
        Some("cooldis.debug.thread_export/1")
    );
    assert_eq!(export["thread"], Value::Null);
    assert_eq!(
        export["redaction"]["mode"].as_str(),
        Some("secret-shaped-json-keys")
    );
    assert_eq!(export["backend"]["kind"].as_str(), Some("sqlite"));
    assert!(
        export["backend"]["sessionStorePath"]
            .as_str()
            .is_some_and(|path| path.ends_with("session_history.sqlite3"))
    );
    assert_eq!(
        export["ackClasses"].as_array().unwrap(),
        &vec![json!("local_committed"), json!("query_projected")]
    );
    let export_streams = export["streams"].as_array().unwrap();
    let thread_stream = export_streams
        .iter()
        .find(|stream| stream["selector"].as_str() == Some("thread"))
        .unwrap();
    assert_eq!(
        thread_stream["ackClasses"].as_array().unwrap(),
        &vec![json!("local_committed"), json!("query_projected")]
    );
    assert_eq!(thread_stream["eventCount"].as_u64(), Some(1));
    assert_eq!(thread_stream["truncated"].as_bool(), Some(true));
    assert!(thread_stream["cursor"].as_str().is_some());
    assert_eq!(
        thread_stream["streamCursor"]["schema"].as_str(),
        Some("cooldis.stream.cursor/1")
    );
    assert_eq!(
        thread_stream["streamCursor"]["stream_id"].as_str(),
        thread_stream["streamId"].as_str()
    );
    assert_eq!(thread_stream["streamCursor"]["sequence"].as_u64(), Some(1));
    assert_eq!(
        thread_stream["streamCursor"]["event_id"].as_str(),
        thread_stream["data"][0]["event_id"].as_str()
    );
    assert_eq!(thread_stream["range"]["fromSequence"].as_u64(), Some(1));
    assert_eq!(
        thread_stream["range"]["lastExportedSequence"].as_u64(),
        Some(1)
    );
    assert!(thread_stream["range"]["toCursor"].as_str().is_some());
    assert!(thread_stream["range"]["tailCursor"].as_str().is_some());
    assert_eq!(
        thread_stream["range"]["lastExportedStreamCursor"],
        thread_stream["streamCursor"]
    );
    assert_eq!(
        thread_stream["range"]["tailStreamCursor"]["schema"].as_str(),
        Some("cooldis.stream.cursor/1")
    );
    assert_eq!(
        thread_stream["data"][0]["schema"].as_str(),
        Some("cooldis.stream.record/1")
    );
    assert_eq!(
        thread_stream["data"][0]["stream_id"].as_str(),
        Some(format!("thread:{thread_id}").as_str())
    );
    let control_stream = export_streams
        .iter()
        .find(|stream| stream["selector"].as_str() == Some("control"))
        .unwrap();
    assert_eq!(control_stream["truncated"].as_bool(), Some(true));
    assert!(control_stream["cursor"].as_str().is_some());
    assert_eq!(
        control_stream["streamCursor"]["schema"].as_str(),
        Some("cooldis.stream.cursor/1")
    );
    let redaction_export = app
        .dispatch_request(
            &connection,
            "thread/debug/export",
            Some(json!({
                "threadId": thread_id,
                "streams": ["control"],
                "includeThread": false,
                "maxEventsPerStream": 3,
            })),
        )
        .await
        .unwrap();
    let redaction_control = redaction_export["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|stream| stream["selector"].as_str() == Some("control"))
        .unwrap();
    let mandate_export = redaction_control["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["kind"].as_str() == Some("mandate.started"))
        .unwrap();
    assert_eq!(
        mandate_export["payload"]["api_key"].as_str(),
        Some("[REDACTED]")
    );
    assert!(
        redaction_export["redaction"]["redactedKeys"]
            .as_array()
            .unwrap()
            .contains(&json!("api_key"))
    );
    let derived_stream = export_streams
        .iter()
        .find(|stream| stream["selector"].as_str() == Some("derived:memory"))
        .unwrap();
    assert_eq!(
        derived_stream["data"][0]["payload"]["fact"].as_str(),
        Some("likes receipts")
    );
    let receipts = export["receipts"].as_array().unwrap();
    assert!(receipts.iter().any(|receipt| {
        receipt["streamId"].as_str() == Some(format!("derived:memory:{thread_id}").as_str())
            && receipt["kind"].as_str() == Some("session.entry.appended")
            && receipt["origin"].as_str() == Some("discharged")
            && receipt["payloadSchema"].as_str() == Some("cooldis.event.session.entry.appended/1")
    }));

    let child_thread_id = ThreadId::new();
    let thread_spawned = crate::NewEventRecord::witnessed(
        lifecycle.coordinates.clone(),
        crate::EventKind::ThreadSpawned,
        json!({
            "schema": crate::EventKind::ThreadSpawned.payload_schema_id(),
            "parent_thread_id": lifecycle.coordinates.thread_id.to_string(),
            "child_thread_id": child_thread_id.to_string(),
            "child_manifest_hash": "sha256:debug-child",
            "granted": [],
            "inputs_hash": "sha256:debug-inputs",
        }),
    );
    let spawned_event_id = thread_spawned.id;
    let io_ingress = crate::NewEventRecord::witnessed(
        lifecycle.coordinates.clone(),
        crate::EventKind::IoIngressReceived,
        json!({
            "schema": crate::EventKind::IoIngressReceived.payload_schema_id(),
            "route_id": "debug-route",
            "envelope_digest": "sha256:debug-envelope",
        }),
    );
    let ingress_event_id = io_ingress.id;
    session_store
        .append_events(
            &control_stream_id,
            vec![
                thread_spawned,
                crate::NewEventRecord::witnessed(
                    lifecycle.coordinates.clone(),
                    crate::EventKind::ThreadJoined,
                    json!({
                        "schema": crate::EventKind::ThreadJoined.payload_schema_id(),
                        "child_thread_id": child_thread_id.to_string(),
                        "spawned_event_id": spawned_event_id.to_string(),
                        "terminal_state": "completed",
                    }),
                ),
                crate::NewEventRecord::witnessed(
                    lifecycle.coordinates.clone(),
                    crate::EventKind::PolicyBound,
                    json!({
                        "schema": crate::EventKind::PolicyBound.payload_schema_id(),
                        "policy_kind": "coupling_set",
                        "policy_id": "debug-policy",
                        "content_hash": "sha256:debug-policy",
                        "valid_from_note": "debug export fixture",
                    }),
                ),
                io_ingress,
                crate::NewEventRecord::witnessed(
                    lifecycle.coordinates.clone(),
                    crate::EventKind::AdmissionDecided,
                    json!({
                        "schema": crate::EventKind::AdmissionDecided.payload_schema_id(),
                        "route_id": "debug-route",
                        "policy_hash": "sha256:debug-policy",
                        "decision": "queue",
                        "admissible": ["queue"],
                        "source_ingress_event_ids": [ingress_event_id.to_string()],
                    }),
                ),
            ],
        )
        .await
        .unwrap();
    let new_kind_export = app
        .dispatch_request(
            &connection,
            "thread/debug/export",
            Some(json!({
                "threadId": thread_id,
                "streams": ["control"],
                "includeThread": false,
                "maxEventsPerStream": 100,
                "redact": false,
            })),
        )
        .await
        .unwrap();
    let exported_kinds = new_kind_export["streams"][0]["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["kind"].as_str())
        .collect::<Vec<_>>();
    for expected in [
        "thread.spawned",
        "thread.joined",
        "policy.bound",
        "io.ingress.received",
        "admission.decided",
    ] {
        assert!(
            exported_kinds.contains(&expected),
            "thread/debug/export should include {expected}"
        );
    }

    let bad_stream = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({ "threadId": thread_id, "stream": "derived:" })),
        )
        .await
        .unwrap_err();
    assert_eq!(bad_stream.code, -32602);
    assert!(bad_stream.message.contains("stream"));

    let filtered_first = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "limit": 1,
                "kinds": ["manifest.bind.completed", "context.compile.completed"],
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        filtered_first["data"][0]["kind"].as_str(),
        Some("manifest.bind.completed")
    );
    let filtered_cursor = filtered_first["cursor"].as_str().unwrap().to_string();
    let filtered_second = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "cursor": filtered_cursor,
                "limit": 1,
                "kinds": ["manifest.bind.completed", "context.compile.completed"],
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        filtered_second["data"][0]["kind"].as_str(),
        Some("context.compile.completed")
    );
    assert_ne!(
        filtered_first["data"][0]["eventId"],
        filtered_second["data"][0]["eventId"]
    );

    let empty_thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let empty_thread_id = empty_thread["thread"]["id"].as_str().unwrap().to_string();
    let empty_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({ "threadId": empty_thread_id, "limit": 100 })),
        )
        .await
        .unwrap();
    let empty_events = empty_page["data"].as_array().unwrap();
    assert_eq!(empty_events.len(), 4);
    assert_eq!(
        empty_events[0]["kind"].as_str(),
        Some("session.entry.appended")
    );
    assert_eq!(
        empty_events[1]["kind"].as_str(),
        Some("manifest.compile.completed")
    );
    assert_eq!(
        empty_events[2]["kind"].as_str(),
        Some("manifest.bind.completed")
    );
    assert_eq!(empty_events[3]["kind"].as_str(), Some("placement.decision"));
    assert_eq!(empty_page["cursor"], Value::Null);

    let bulk_thread_id = ThreadId::parse_str(&empty_thread_id).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(bulk_thread_id)
        .await
        .unwrap()
        .unwrap();
    let stream_id = crate::EventStreamId::for_thread(&lifecycle.coordinates);
    let session_store = crate::SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let bulk_events = (0..501)
        .map(|idx| {
            crate::NewEventRecord::witnessed(
                lifecycle.coordinates.clone(),
                crate::EventKind::SessionEntryAppended,
                json!({ "idx": idx }),
            )
        })
        .collect::<Vec<_>>();
    session_store
        .append_events(&stream_id, bulk_events)
        .await
        .unwrap();

    let limit_zero = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": empty_thread_id,
                "limit": 0,
                "kinds": ["session.entry.appended"],
            })),
        )
        .await
        .unwrap();
    assert_eq!(limit_zero["data"].as_array().unwrap().len(), 1);
    assert!(limit_zero["cursor"].as_str().is_some());

    let clamped_page = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": empty_thread_id,
                "limit": 999,
                "kinds": ["session.entry.appended"],
            })),
        )
        .await
        .unwrap();
    assert_eq!(clamped_page["data"].as_array().unwrap().len(), 500);
    let clamped_cursor = clamped_page["cursor"].as_str().unwrap().to_string();
    let clamped_tail = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": empty_thread_id,
                "cursor": clamped_cursor,
                "limit": 999,
                "kinds": ["session.entry.appended"],
            })),
        )
        .await
        .unwrap();
    assert!(!clamped_tail["data"].as_array().unwrap().is_empty());
    assert_eq!(clamped_tail["cursor"], Value::Null);

    let past_end_cursor = encode_thread_events_cursor(10_000).unwrap();
    let past_end = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": empty_thread_id,
                "cursor": past_end_cursor,
                "limit": 100,
            })),
        )
        .await
        .unwrap();
    assert_eq!(past_end["data"].as_array().unwrap().len(), 0);
    assert_eq!(past_end["cursor"], Value::Null);

    let bad_cursor = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({ "threadId": thread_id, "cursor": "not-a-cursor" })),
        )
        .await
        .unwrap_err();
    assert_eq!(bad_cursor.code, -32602);
    assert!(bad_cursor.message.contains("malformed"));

    let unknown_thread = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": Uuid::now_v7().to_string(),
                "limit": 1,
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(unknown_thread.code, -32001);
    assert!(unknown_thread.message.contains("thread not found"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mandate_rpc_validates_and_folds_control_stream_events() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();

    let malformed_cron = app
        .dispatch_request(
            &connection,
            "mandate/start",
            Some(json!({
                "threadId": thread_id,
                "schedule": { "cron": { "expr": "not cron", "tz": "UTC" } },
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(malformed_cron.code, -32602);

    let unknown_tz = app
        .dispatch_request(
            &connection,
            "mandate/start",
            Some(json!({
                "threadId": thread_id,
                "schedule": { "cron": { "expr": "0 * * * * *", "tz": "Mars/Olympus" } },
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(unknown_tz.code, -32602);

    let short_interval = app
        .dispatch_request(
            &connection,
            "mandate/start",
            Some(json!({
                "threadId": thread_id,
                "schedule": { "interval": { "every_ms": 59_999 } },
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(short_interval.code, -32602);

    let past_at = app
        .dispatch_request(
            &connection,
            "mandate/start",
            Some(json!({
                "threadId": thread_id,
                "schedule": { "at": { "when": "2000-01-01T00:00:00Z" } },
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(past_at.code, -32602);

    let coalesced = app
        .dispatch_request(
            &connection,
            "mandate/start",
            Some(json!({
                "threadId": thread_id,
                "schedule": { "at": { "when": "2000-01-01T00:00:00Z" } },
                "catchUp": "coalesce_missed",
            })),
        )
        .await
        .unwrap();
    let coalesced_id = coalesced["mandateEventId"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "mandate/revoke",
        Some(json!({
            "threadId": thread_id,
            "mandateEventId": coalesced_id,
        })),
    )
    .await
    .unwrap();

    let start = app
        .dispatch_request(
            &connection,
            "mandate/start",
            Some(json!({
                "threadId": thread_id,
                "schedule": { "interval": { "every_ms": 60_000 } },
                "maxOccurrences": 3,
                "catchUp": "skip_missed",
                "inputTemplate": "continue summary",
            })),
        )
        .await
        .unwrap();
    let mandate_event_id = start["mandateEventId"].as_str().unwrap().to_string();

    let list = app
        .dispatch_request(
            &connection,
            "mandate/list",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    let mandates = list["data"].as_array().unwrap();
    assert_eq!(mandates.len(), 1);
    assert_eq!(
        mandates[0]["mandateEventId"].as_str(),
        Some(mandate_event_id.as_str())
    );
    assert_eq!(mandates[0]["schedule"]["interval"]["every_ms"], 60_000);
    assert_eq!(mandates[0]["maxOccurrences"], 3);
    assert_eq!(mandates[0]["catchUp"].as_str(), Some("skip_missed"));
    assert_eq!(
        mandates[0]["inputTemplate"].as_str(),
        Some("continue summary")
    );

    let events = app
        .dispatch_request(
            &connection,
            "thread/events/list",
            Some(json!({
                "threadId": thread_id,
                "stream": "control",
                "kinds": ["mandate.started"],
            })),
        )
        .await
        .unwrap();
    assert!(events["data"].as_array().unwrap().iter().any(|event| {
        event["eventId"].as_str() == Some(mandate_event_id.as_str())
            && event["kind"].as_str() == Some("mandate.started")
            && event["payload_schema"].as_str() == Some("cooldis.event.mandate.started/1")
    }));

    let revoked = app
        .dispatch_request(
            &connection,
            "mandate/revoke",
            Some(json!({
                "threadId": thread_id,
                "mandateEventId": mandate_event_id,
            })),
        )
        .await
        .unwrap();
    assert_eq!(revoked["status"].as_str(), Some("revoked"));
    let revoked_again = app
        .dispatch_request(
            &connection,
            "mandate/revoke",
            Some(json!({
                "threadId": thread_id,
                "mandateEventId": mandate_event_id,
            })),
        )
        .await
        .unwrap();
    assert_eq!(revoked_again["status"].as_str(), Some("already_revoked"));

    let empty = app
        .dispatch_request(
            &connection,
            "mandate/list",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    assert_eq!(empty["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn thread_start_with_agent_ref_lowers_cwd_and_rejects_operation_injection() {
    use crate::EventStore;

    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-manifest-closed-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("app-server-manifest-closed");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("closed.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "closed-runner"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false
max_tool_rounds = 64

[runtime.overrides]
allow = ["default_cwd", "max_tool_rounds"]
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();
    let no_cwd_manifest_path = root.join("closed-no-cwd.cooldis.agent.toml");
    std::fs::write(
        &no_cwd_manifest_path,
        r#"
[agent]
name = "closed-no-cwd"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&no_cwd_manifest_path)
        .unwrap();

    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let metadata_path = config.metadata_store_path();
    let session_path = config.state_home.join("session_history.sqlite3");
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let cwd_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://closed-runner@latest",
                "cwd": "outside-manifest",
            })),
        )
        .await
        .unwrap();
    let thread_id = ThreadId::parse_str(cwd_start["thread"]["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        cwd_start["cwd"].as_str(),
        Some(cwd_string(&workspace.join("outside-manifest")).as_str())
    );
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(thread_id)
        .await
        .unwrap()
        .expect("cwd-lowered manifest start should persist lifecycle metadata");
    assert_eq!(
        serde_json::from_str::<AgentManifestBindOverrides>(
            &lifecycle.metadata[THREAD_AGENT_RUNTIME_OVERRIDES_METADATA]
        )
        .unwrap()
        .default_cwd
        .as_deref(),
        Some(cwd_string(&workspace.join("outside-manifest")).as_str())
    );
    assert_eq!(
        lifecycle.metadata[THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA],
        "64"
    );
    let session_store = SqliteSessionStore::open(session_path).await.unwrap();
    let stream_id = EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    assert_eq!(
        bind.payload["overridden_keys"].as_array().unwrap(),
        &vec![json!("default_cwd")]
    );
    assert_eq!(
        bind.payload["effective_runtime"]["default_cwd"].as_str(),
        Some(cwd_string(&workspace.join("outside-manifest")).as_str())
    );
    assert_eq!(
        bind.payload["effective_runtime"]["max_tool_rounds"],
        json!(64)
    );

    let no_cwd_err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://closed-no-cwd@latest",
                "cwd": "outside-manifest",
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(no_cwd_err.code, -32602);
    assert!(
        no_cwd_err
            .message
            .contains("runtime override \"default_cwd\" is not allowlisted"),
        "{}",
        no_cwd_err.message
    );

    let rebind = app
        .dispatch_request(
            &connection,
            "thread/rebindFork",
            Some(json!({
                "threadId": thread_id.to_string(),
                "agentRef": "agent://closed-runner@latest",
                "runtimeOverrides": {"maxToolRounds": "unlimited"},
            })),
        )
        .await
        .unwrap();
    let rebound_id = ThreadId::parse_str(rebind["thread"]["id"].as_str().unwrap()).unwrap();
    let rebound = app
        .inner
        .metadata_store
        .get_thread_lifecycle(rebound_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rebound.metadata[THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA],
        "unlimited"
    );

    let rebind_override_err = app
        .dispatch_request(
            &connection,
            "thread/rebindFork",
            Some(json!({
                "threadId": thread_id.to_string(),
                "agentRef": "agent://closed-no-cwd@latest",
                "runtimeOverrides": {"maxToolRounds": "unlimited"},
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(rebind_override_err.code, -32602);
    assert!(
        rebind_override_err
            .message
            .contains("runtime override \"max_tool_rounds\" is not allowlisted"),
        "{}",
        rebind_override_err.message
    );

    let capsule_err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://closed-runner@latest",
                "capsuleBindings": {
                    "operationNames": ["ambient"]
                }
            })),
        )
        .await
        .unwrap_err();
    assert!(
        capsule_err
            .message
            .contains("operations are declared in an agent manifest")
    );

    let default_capsule_err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "capsuleBindings": {
                    "operationNames": ["ambient"]
                }
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(default_capsule_err.code, -32602);
    assert!(
        default_capsule_err
            .message
            .contains("operations are declared in an agent manifest")
    );

    let metadata_store = SqliteMetadataStore::open(metadata_path).await.unwrap();
    assert_eq!(
        metadata_store
            .list_thread_lifecycle_for_user("cooldis_app_server", "local_user")
            .await
            .unwrap()
            .len(),
        2
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_threads_do_not_inherit_global_or_unbound_capsule_operations() {
    let root = unique_test_root("app-server-manifest-no-ambient-tools");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    publish_echo_operation(
        &operation_registry_root,
        "global",
        "global_search",
        "global",
    )
    .await;
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("no-tools.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "no-tools"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();

    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let capsule_bindings = CapsuleBindingsConfig::default()
        .with_registry_root(&operation_registry_root)
        .with_global_operation_name("global");
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-manifest-ambient-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace)
        .with_capsule_bindings(capsule_bindings.clone()); // lexicon-allow: capsule - existing app-server config method and parameter
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let mut runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    runtime_config.max_tokens = 128;
    let runtime_factory = runtime_factory_from_provider_parts(
        runtime_config,
        provider_client,
        capsule_bindings, // lexicon-allow: capsule - existing app-server config parameter
    );
    let app = CooldisAppServer::with_runtime_factory(config, runtime_factory)
        .await
        .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://no-tools@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "no ambient tools", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    wait_for_provider_requests(&client, 1).await;
    let requests = client.requests();
    assert!(!tool_names(&requests[0]).contains(&"global_global_search".to_string()));
    assert!(!tool_names(&requests[0]).contains(&TOOL_SEARCH_TOOL.to_string()));
    assert!(
        !requests[0]
            .tools
            .iter()
            .any(|tool| tool.description.contains("global_search"))
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_operation_bindings_pin_artifact_hashes() {
    let root = unique_test_root("app-server-manifest-pinned-operation");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let first = publish_echo_operation(&operation_registry_root, "search", "search", "old").await;
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("pinned.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "pinned"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "bash_tool"
id = "search"
command = "search"
operation_ref = "op://search@sha256:{}"

[runtime]
default_cwd = "."
streaming = false
"#,
            first.active_artifact_hash
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, &operation_registry_root)
        .unwrap();
    publish_echo_operation(&operation_registry_root, "search", "search", "new").await;

    let client = Arc::new(BashCallingCapsuleClient::new(
        "search",
        "search",
        "printf cooldis | search",
        "old:cooldis",
    ));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let capsule_bindings =
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root);
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-manifest-pinned-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace)
        .with_capsule_bindings(capsule_bindings.clone());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let mut runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    runtime_config.max_tokens = 128;
    let runtime_factory =
        runtime_factory_from_provider_parts(runtime_config, provider_client, capsule_bindings);
    let app = CooldisAppServer::with_runtime_factory(config, runtime_factory)
        .await
        .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://pinned@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "use pinned search", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    wait_for_provider_requests(&client, 2).await;
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn manifest_rw_workspace_binding_round_trips_real_files_and_blocks_host_escape() {
    use crate::EventStore;
    use std::os::unix::fs::symlink;

    let root = unique_test_root("app-server-manifest-workspace");
    let app_cwd = root.join("app-cwd");
    let host_workspace = root.join("host-workspace");
    let outside = root.join("outside");
    std::fs::create_dir_all(&app_cwd).unwrap();
    std::fs::create_dir_all(&host_workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(host_workspace.join("note.txt"), "seed\n").unwrap();
    std::fs::write(outside.join("secret.txt"), "outside-safe\n").unwrap();
    symlink(
        outside.join("secret.txt"),
        host_workspace.join("outside-link"),
    )
    .unwrap();

    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("workspace.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "workspace-agent"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[workspace]
guest_path = "/work"
min_mode = "rw"

[runtime]
default_cwd = "."
streaming = false
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();

    let client = Arc::new(WorkspaceBindingClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root(
        &root,
        &app_cwd,
        provider_client,
        CapsuleBindingsConfig::default(),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let undeclared = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "workspace": {"hostPath": host_workspace, "mode": "rw"}
            })),
        )
        .await
        .unwrap_err();
    assert!(undeclared.message.contains("did not declare"));

    let unbound = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({"agentRef": "agent://workspace-agent@latest"})),
        )
        .await
        .unwrap_err();
    assert!(unbound.message.contains("requires a workspace binding"));

    let started = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "agentRef": "agent://workspace-agent@latest",
                "workspace": {"hostPath": host_workspace, "mode": "rw"}
            })),
        )
        .await
        .unwrap();
    let thread_id = started["thread"]["id"].as_str().unwrap().to_string();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(&thread_id).unwrap())
        .await
        .unwrap()
        .unwrap();
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let events = session_store
        .read_events(&EventStreamId::for_thread(&lifecycle.coordinates), None)
        .await
        .unwrap();
    let bind = event_by_kind(&events, EventKind::ManifestBindCompleted);
    assert_eq!(bind.payload["workspace"]["guest_path"], "/work");
    assert_eq!(bind.payload["workspace"]["mode"], "rw");
    assert_eq!(
        bind.payload["workspace"]["host_path"],
        std::fs::canonicalize(&host_workspace)
            .unwrap()
            .to_string_lossy()
            .as_ref()
    );

    let fork = app
        .dispatch_request(
            &connection,
            "thread/fork",
            Some(json!({"threadId": thread_id})),
        )
        .await
        .unwrap();
    let fork_id = ThreadId::parse_str(fork["thread"]["id"].as_str().unwrap()).unwrap();
    let fork_lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(fork_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fork_lifecycle.metadata.get(THREAD_AGENT_WORKSPACE_METADATA),
        lifecycle.metadata.get(THREAD_AGENT_WORKSPACE_METADATA),
        "clone forks must inherit the exact resolved bind-time workspace"
    );

    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "edit the workspace", "text_elements": []}]
        })),
    )
    .await
    .unwrap();
    wait_for_provider_requests(&client, 2).await;

    assert_eq!(
        std::fs::read_to_string(host_workspace.join("note.txt")).unwrap(),
        "updated\n"
    );
    assert_eq!(
        std::fs::read_to_string(outside.join("secret.txt")).unwrap(),
        "outside-safe\n"
    );
    assert!(!root.join("outside.txt").exists());
    assert!(!root.join("absolute-outside.txt").exists());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_bound_coupling_set_is_persisted_to_thread_metadata() {
    let root = unique_test_root("app-server-manifest-bound-coupling-set");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let operation = publish_echo_operation(
        &operation_registry_root,
        "std-context-spill",
        "run",
        "spill",
    )
    .await;
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("coupled.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "coupled"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[couplings]]
id = "std::context.spill"
function_ref = "op://std-context-spill/run@sha256:{}"
grants = []

[couplings.trigger]
kind = "context.compile.completed"

[[couplings.source.selectors]]
stream = "thread"
kind = "context.compile.completed"

[couplings.sink]
stream = "derived:context"
kind = ["context.summary.completed", "context.read_plan.set"]

[couplings.budget]
max_discharge_events = 2

[runtime]
default_cwd = "."
streaming = false
"#,
            operation.active_artifact_hash
        ),
    )
    .unwrap();
    let agent = LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, &operation_registry_root)
        .unwrap();

    let capsule_bindings =
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root);
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-manifest-bound-coupling-{}.sock",
        Uuid::now_v7()
    )));
    let mut config =
        CooldisAppServerConfig::local(listen, &workspace).with_capsule_bindings(capsule_bindings);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://coupled@latest" })),
        )
        .await
        .unwrap();
    let thread_id_string = thread["thread"]["id"].as_str().unwrap().to_string();
    let thread_id = ThreadId::parse_str(&thread_id_string).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(thread_id)
        .await
        .unwrap()
        .expect("manifest start should persist lifecycle metadata");
    let coupling_set: crate::BoundCouplingSet = serde_json::from_str(
        lifecycle
            .metadata
            .get(THREAD_BOUND_COUPLING_SET_METADATA)
            .expect("bound coupling set metadata should be persisted"),
    )
    .unwrap();

    assert_eq!(coupling_set.snapshot_id, agent.manifest_hash);
    assert_eq!(coupling_set.couplings.len(), 1);
    let coupling = &coupling_set.couplings[0];
    assert_eq!(coupling.id, "std::context.spill");
    assert_eq!(
        coupling.trigger_kind,
        crate::EventKind::ContextCompileCompleted
    );
    assert_eq!(coupling.sink.stream, "derived:context");
    assert_eq!(
        coupling.function.artifact_hash,
        operation.active_artifact_hash
    );
    assert_eq!(coupling.function.operation_name.as_deref(), Some("run"));

    let coupling_list = app
        .dispatch_request(
            &connection,
            "thread/couplings/list",
            Some(json!({ "threadId": thread_id_string })),
        )
        .await
        .unwrap();
    assert_eq!(coupling_list["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        coupling_list["manifestHash"].as_str(),
        Some(agent.manifest_hash.as_str())
    );
    assert_eq!(
        coupling_list["data"][0]["id"].as_str(),
        Some("std::context.spill")
    );
    assert_eq!(
        coupling_list["data"][0]["role"].as_str(),
        Some("projection")
    );
    assert_eq!(
        coupling_list["data"][0]["triggerKind"].as_str(),
        Some("context.compile.completed")
    );
    assert_eq!(
        coupling_list["data"][0]["sourceStreams"]
            .as_array()
            .unwrap(),
        &vec![json!("thread")]
    );
    assert_eq!(
        coupling_list["data"][0]["sourceKinds"].as_array().unwrap(),
        &vec![json!("context.compile.completed")]
    );
    assert_eq!(
        coupling_list["data"][0]["sinkStream"].as_str(),
        Some("derived:context")
    );
    assert_eq!(
        coupling_list["data"][0]["sinkKinds"].as_array().unwrap(),
        &vec![
            json!("context.summary.completed"),
            json!("context.read_plan.set")
        ]
    );
    assert_eq!(
        coupling_list["data"][0]["functionRef"].as_str(),
        Some(
            format!(
                "op://std-context-spill/run@sha256:{}",
                operation.active_artifact_hash
            )
            .as_str()
        )
    );
    assert_eq!(
        coupling_list["data"][0]["artifactHash"].as_str(),
        Some(operation.active_artifact_hash.as_str())
    );
    assert_eq!(
        coupling_list["data"][0]["operationName"].as_str(),
        Some("run")
    );
    assert_eq!(
        coupling_list["data"][0]["budget"]["maxDischargeEvents"].as_u64(),
        Some(2)
    );
    assert!(coupling_list["data"][0]["configHash"].as_str().is_some());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_operation_binding_filters_two_segment_ref_from_thread_catalog() {
    use crate::EventStore;

    let root = unique_test_root("app-server-manifest-operation-segment");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let record = publish_multi_echo_operation(
        &operation_registry_root,
        "analytics",
        &[("profile", "profile"), ("summarize", "summary")],
    )
    .await;
    let agent_registry_root = root.join("agents");
    let manifest_path = root.join("operation-segment.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "operation-segment"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "bash_tool"
id = "profile"
command = "profile"
operation_ref = "op://analytics/profile@sha256:{}"
effect_class = "idempotent"

[runtime]
default_cwd = "."
streaming = false
"#,
            record.active_artifact_hash
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, &operation_registry_root)
        .unwrap();

    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let capsule_bindings =
        CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root);
    let listen = AppServerListenAddr::Unix(std::env::temp_dir().join(format!(
        "cooldis-manifest-operation-segment-{}.sock",
        Uuid::now_v7()
    )));
    let mut config = CooldisAppServerConfig::local(listen, &workspace)
        .with_capsule_bindings(capsule_bindings.clone());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let session_path = config.state_home.join("session_history.sqlite3");
    let mut runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    runtime_config.max_tokens = 128;
    let runtime_factory =
        runtime_factory_from_provider_parts(runtime_config, provider_client, capsule_bindings);
    let app = CooldisAppServer::with_runtime_factory(config, runtime_factory)
        .await
        .unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "agentRef": "agent://operation-segment@latest" })),
        )
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();

    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(&thread_id).unwrap())
        .await
        .unwrap()
        .expect("manifest start should persist lifecycle metadata");
    let session_store = SqliteSessionStore::open(session_path).await.unwrap();
    let stream_id = EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    assert_eq!(
        bind.payload["operation_bindings"][0]["operations"],
        json!(["profile"])
    );
    let request = crate::ToolCallRequestedPayload {
        subject: crate::ToolCallSubject {
            turn_id: "turn-effect-class".to_string(),
            call_id: "call-effect-class".to_string(),
        },
        snapshot_id: bind.payload["manifest_hash"].as_str().unwrap().to_string(),
        tool_name: crate::BASH_TOOL.to_string(),
        arguments: json!({"command":"profile customer-1"}),
        args_fingerprint: None,
        holds: Vec::new(),
    };
    assert_eq!(
        crate::adapters::agent_loop::effect_class_for_request(&events, &request).unwrap(),
        EffectClass::Idempotent,
        "the runtime lookup must read the class from the real top-level bind receipt shape"
    );

    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "inspect scoped catalog", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    wait_for_provider_requests(&client, 1).await;
    let requests = client.requests();
    let tools = tool_names(&requests[0]);
    assert!(tools.contains(&"analytics_profile".to_string()));
    assert!(!tools.contains(&"analytics_summarize".to_string()));
    assert_bash_tool_describes(&requests[0], "profile");
    assert_bash_tool_omits(&requests[0], "summarize");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn thread_manifest_operation_bindings_accept_legacy_metadata_without_operations() {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        THREAD_AGENT_OPERATION_BINDINGS_METADATA.to_string(),
        r#"[{"name":"analytics","artifact_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","grants":["net:https://example.com"]}]"#
            .to_string(),
    );
    let context = ThreadContext::with_topology_and_metadata(
        crate::ThreadCoordinates::new("tenant", "user", "session"),
        ThreadTopology::root(),
        metadata,
    );

    let bindings = thread_manifest_operation_bindings(&context).unwrap();
    assert_eq!(
        bindings,
        vec![AgentManifestOperationBinding {
            name: "analytics".to_string(),
            artifact_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_string(),
            effect_class: EffectClass::AtMostOnce,
            grants: vec!["net:https://example.com".to_string()],
            grant_expiries: Vec::new(),
            operations: Vec::new(),
            direct_tools: Vec::new(),
        }]
    );
}

#[test]
fn apply_manifest_runtime_metadata_injects_tool_use_instruction_once() {
    let instruction = "Use the Cooldis tools when they fit the request.";
    let mut metadata = BTreeMap::new();
    metadata.insert(
        THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA.to_string(),
        instruction.to_string(),
    );
    let context = ThreadContext::with_topology_and_metadata(
        crate::ThreadCoordinates::new("tenant", "user", "session"),
        ThreadTopology::root(),
        metadata,
    );
    let mut config = AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.system.push(SystemBlock::text("Base instruction."));

    apply_manifest_runtime_metadata(&context, &mut config).unwrap();
    apply_manifest_runtime_metadata(&context, &mut config).unwrap();

    assert_eq!(config.system[0].text, "Base instruction.");
    assert_eq!(
        config
            .system
            .iter()
            .filter(|block| block.text == instruction)
            .count(),
        1
    );
}

#[test]
fn apply_manifest_runtime_metadata_injects_legacy_tool_use_instruction() {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        THREAD_AGENT_REF_METADATA.to_string(),
        "agent://legacy@0.1.0".to_string(),
    );
    metadata.insert(
        THREAD_AGENT_OPERATION_BINDINGS_METADATA.to_string(),
        r#"[{"name":"file-read","artifact_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","grants":[]}]"#
            .to_string(),
    );
    let context = ThreadContext::with_topology_and_metadata(
        crate::ThreadCoordinates::new("tenant", "user", "session"),
        ThreadTopology::root(),
        metadata,
    );
    let mut config = AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");

    apply_manifest_runtime_metadata(&context, &mut config).unwrap();

    assert_eq!(config.system.len(), 1);
    assert!(config.system[0].text.contains("agent://legacy@0.1.0"));
    assert!(config.system[0].text.contains("call the tool immediately"));
}

#[tokio::test]
async fn manifest_operation_grants_extend_loaded_record_without_duplicates() {
    let root = unique_test_root("app-server-manifest-operation-grants");
    let operation_registry_root = root.join("operations");
    let mut record =
        publish_echo_operation(&operation_registry_root, "search", "search", "search").await;
    record
        .capability_grants
        .insert("package:required".to_string());

    apply_manifest_operation_grants(
        &mut record,
        [
            "net.http:GET:https://example.com".to_string(),
            "package:required".to_string(),
            "net.http:GET:https://example.com".to_string(),
        ],
    );

    assert_eq!(
        record.capability_grants,
        BTreeSet::from([
            "net.http:GET:https://example.com".to_string(),
            "package:required".to_string(),
        ])
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn catalog_provider_resolution_uses_seeded_openai_compatible_store_and_stored_auth() {
    let root = std::env::temp_dir().join(format!("cooldis-provider-resolve-{}", Uuid::now_v7()));
    let store_path = root.join("metadata.sqlite3");
    let store = crate::SqliteMetadataStore::open(&store_path).await.unwrap();
    crate::seed_default_llm_providers(&store).await.unwrap();
    store
        .set_credential(
            crate::OPENAI_COMPATIBLE_PROVIDER_ID,
            crate::LlmProviderCredential::ApiKey {
                key: "stored-openai_compatible-key".to_string(),
            },
        )
        .await
        .unwrap();

    let resolved = resolve_catalog_openai_chat_completions_provider(
        &store,
        &store,
        &crate::LlmProviderAuthContext::new(),
        crate::OPENAI_COMPATIBLE_PROVIDER_ID,
        None,
        777,
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        resolved.runtime_config.provider,
        crate::OPENAI_COMPATIBLE_PROVIDER_ID
    );
    assert_eq!(
        resolved.runtime_config.model,
        crate::OPENAI_COMPATIBLE_DEFAULT_MODEL
    );
    assert_eq!(resolved.runtime_config.max_tokens, 777);
    assert!(!resolved.runtime_config.stream);
    assert_eq!(
        resolved.endpoint.url,
        "https://api.example.invalid/v1/chat/completions"
    );
    assert_eq!(
        resolved.endpoint.auth,
        crate::ProviderAuth::Bearer {
            token: "stored-openai_compatible-key".to_string()
        }
    );
    assert_eq!(
        resolved.endpoint.headers,
        vec![("X-Example-Provider".to_string(), "required".to_string())]
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_fork_creates_child_app_server_thread() {
    let app = test_app().await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let source_thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();

    let fork = app
        .dispatch_request(
            &connection,
            "thread/fork",
            Some(json!({
                "threadId": source_thread_id,
                "ephemeral": true,
            })),
        )
        .await
        .unwrap();
    let fork_thread = &fork["thread"];
    let fork_thread_id = fork_thread["id"].as_str().unwrap();
    assert_ne!(fork_thread_id, source_thread_id);
    assert_eq!(
        fork_thread["forkedFromId"].as_str(),
        Some(source_thread_id.as_str())
    );
    assert_eq!(fork_thread["ephemeral"].as_bool(), Some(true));
    assert_eq!(fork["fork"]["mode"].as_str(), Some("clone"));
    assert_eq!(
        fork["fork"]["parentThreadId"].as_str(),
        Some(source_thread_id.as_str())
    );
    assert!(
        fork["fork"]["checkpointId"].as_str().is_some(),
        "thread/fork should report the checkpoint it cloned from"
    );
    assert_eq!(
        fork["fork"]["sourceCut"]["threadId"].as_str(),
        Some(source_thread_id.as_str())
    );
    assert_eq!(
        fork["fork"]["sourceCut"]["checkpointId"].as_str(),
        fork["fork"]["checkpointId"].as_str()
    );
    assert_eq!(
        fork["fork"]["sourceCut"]["streamId"].as_str(),
        Some(format!("thread:{source_thread_id}").as_str())
    );
    assert!(fork["fork"]["sourceCut"]["streamToSequence"].is_null());

    let mut saw_fork_started = false;
    while let Ok(message) = outbound_rx.try_recv() {
        if let JsonRpcMessage::Notification(notification) = message
            && notification.method == "thread/started"
            && notification
                .params
                .as_ref()
                .and_then(|params| params.get("thread"))
                .and_then(|thread| thread.get("id"))
                .and_then(Value::as_str)
                == Some(fork_thread_id)
        {
            saw_fork_started = true;
        }
    }
    assert!(saw_fork_started);
}

#[tokio::test]
async fn thread_fork_can_use_explicit_checkpoint_id() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let source_thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let source_id = ThreadId::parse_str(&source_thread_id).unwrap();
    let source_lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(source_id)
        .await
        .unwrap()
        .unwrap();
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let _source_entry = session_store
        .append(
            &source_lifecycle.coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("explicit fork checkpoint"),
            },
        )
        .await
        .unwrap();
    let checkpoint = app
        .inner
        .supervisor
        .create_checkpoint_at(
            &source_lifecycle.coordinates,
            None,
            Some("explicit-fork".to_string()),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let checkpoint_leaf = checkpoint
        .active_entry_id
        .map(|entry_id| entry_id.to_string());

    let fork = app
        .dispatch_request(
            &connection,
            "thread/fork",
            Some(json!({
                "threadId": source_thread_id,
                "checkpointId": checkpoint.id.to_string(),
            })),
        )
        .await
        .unwrap();

    assert_eq!(fork["fork"]["mode"].as_str(), Some("clone"));
    assert_eq!(
        fork["fork"]["checkpointId"].as_str(),
        Some(checkpoint.id.to_string().as_str())
    );
    assert_eq!(
        fork["fork"]["sourceCut"]["leafEntryId"].as_str(),
        checkpoint_leaf.as_deref()
    );
    assert!(
        checkpoint_leaf.is_some(),
        "explicit checkpoint should record the active leaf it cloned from"
    );
    assert_eq!(
        fork["fork"]["sourceCut"]["streamId"].as_str(),
        Some(format!("thread:{source_thread_id}").as_str())
    );
}

#[tokio::test]
async fn thread_fork_rejects_invalid_checkpoint_id() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let source_thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();

    let err = app
        .dispatch_request(
            &connection,
            "thread/fork",
            Some(json!({
                "threadId": source_thread_id,
                "checkpointId": "not-a-checkpoint-id",
            })),
        )
        .await
        .unwrap_err();

    assert_eq!(err.code, -32602);
    assert!(err.message.contains("invalid checkpointId"));
}

#[tokio::test]
async fn thread_fork_rejects_unavailable_checkpoint_id() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let source_thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let checkpoint_id = ThreadCheckpointId::new();

    let err = app
        .dispatch_request(
            &connection,
            "thread/fork",
            Some(json!({
                "threadId": source_thread_id,
                "checkpointId": checkpoint_id.to_string(),
            })),
        )
        .await
        .unwrap_err();

    assert_eq!(err.code, -32000);
    assert!(err.message.contains("is not available for thread"));
    assert!(err.message.contains(checkpoint_id.to_string().as_str()));
}

#[tokio::test]
async fn thread_rebind_fork_creates_borrowed_prefix_manifest_child() {
    let app = test_app().await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let source_thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let source_id = ThreadId::parse_str(&source_thread_id).unwrap();
    let source_lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(source_id)
        .await
        .unwrap()
        .unwrap();
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let source_entry = session_store
        .append(
            &source_lifecycle.coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("borrowed source message"),
            },
        )
        .await
        .unwrap();

    let rebind = app
        .dispatch_request(
            &connection,
            "thread/rebindFork",
            Some(json!({
                "threadId": source_thread_id,
                "agentRef": default_manifest::DEFAULT_AGENT_REF,
                "placement": {"target": "local"},
                "reason": "manifest_update",
            })),
        )
        .await
        .unwrap();
    let child_thread = &rebind["thread"];
    let child_thread_id = child_thread["id"].as_str().unwrap();
    assert_ne!(child_thread_id, source_thread_id);
    assert_eq!(
        child_thread["parentThreadId"].as_str(),
        Some(source_thread_id.as_str())
    );
    assert_eq!(
        rebind["fork"]["parentThreadId"].as_str(),
        Some(source_thread_id.as_str())
    );
    assert_eq!(rebind["fork"]["mode"].as_str(), Some("reference"));
    assert!(
        rebind["fork"]["agentRef"]
            .as_str()
            .is_some_and(|agent_ref| agent_ref.starts_with("agent://cooldis/default@"))
    );
    assert!(
        rebind["fork"]["sourceCut"]["leafEntryId"]
            .as_str()
            .is_some()
    );

    let child_id = ThreadId::parse_str(child_thread_id).unwrap();
    let child_lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        child_lifecycle.parent_thread_id,
        Some(source_lifecycle.coordinates.thread_id)
    );
    assert_eq!(
        child_lifecycle.metadata[THREAD_REBIND_FORK_REASON_METADATA],
        "manifest_update"
    );
    assert!(
        child_lifecycle.metadata[THREAD_AGENT_REF_METADATA].starts_with("agent://cooldis/default@")
    );

    let child_context = session_store
        .build_context(&child_lifecycle.coordinates)
        .await
        .unwrap();
    assert_eq!(
        text_from_canonical_messages(&child_context.messages),
        "borrowed source message"
    );
    assert_eq!(child_context.entries[0].entry_id, source_entry.entry_id);
    assert_eq!(
        child_context.entries[0].coordinates,
        source_lifecycle.coordinates
    );
    assert!(matches!(
        child_context.entries.last().unwrap().kind,
        SessionEntryKind::Runtime { ref kind, .. } if kind == "thread_rebind_fork"
    ));
    assert_eq!(child_context.source_cuts.len(), 2);
    assert!(child_context.source_cuts[0].inherited);
    assert!(!child_context.source_cuts[1].inherited);

    let child_events = session_store
        .read_events(
            &EventStreamId::for_thread(&child_lifecycle.coordinates),
            None,
        )
        .await
        .unwrap();
    let child_bind_events = child_events
        .iter()
        .filter(|event| event.kind == EventKind::ManifestBindCompleted)
        .collect::<Vec<_>>();
    assert_eq!(child_bind_events.len(), 1);
    assert_eq!(child_bind_events[0].payload["placement"]["target"], "local");
    let child_placement_events = child_events
        .iter()
        .filter(|event| event.kind == EventKind::PlacementDecision)
        .collect::<Vec<_>>();
    assert_eq!(child_placement_events.len(), 1);
    assert_eq!(child_placement_events[0].origin, EventOrigin::Witnessed);
    assert_eq!(child_placement_events[0].payload["placement"], "local");
    assert_eq!(
        child_placement_events[0].payload["snapshot_id"],
        child_bind_events[0].payload["manifest_hash"]
    );
    assert!(!child_events.iter().any(|event| {
        event.payload.get("entry_id").and_then(Value::as_str)
            == Some(source_entry.entry_id.to_string().as_str())
    }));

    let mut saw_rebind_started = false;
    while let Ok(message) = outbound_rx.try_recv() {
        if let JsonRpcMessage::Notification(notification) = message
            && notification.method == "thread/started"
            && notification
                .params
                .as_ref()
                .and_then(|params| params.get("thread"))
                .and_then(|thread| thread.get("id"))
                .and_then(Value::as_str)
                == Some(child_thread_id)
        {
            saw_rebind_started = true;
        }
    }
    assert!(saw_rebind_started);
}

#[tokio::test]
async fn thread_rebind_fork_rejects_active_source_thread() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let source_thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    {
        let mut state = app.inner.state.write().await;
        let source = state.threads.get_mut(&source_thread_id).unwrap();
        source.active_turn_id = Some("active-turn".to_string());
    }

    let err = app
        .dispatch_request(
            &connection,
            "thread/rebindFork",
            Some(json!({
                "threadId": source_thread_id,
                "agentRef": default_manifest::DEFAULT_AGENT_REF,
            })),
        )
        .await
        .unwrap_err();

    assert!(
        err.message
            .contains("requires the source thread to be idle")
    );
}

#[tokio::test]
async fn thread_start_accepts_parent_thread_shorthand() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let root = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let root_thread = &root["thread"];
    let root_thread_id = root_thread["id"].as_str().unwrap().to_string();
    let root_session_id = root_thread["sessionId"].as_str().unwrap().to_string();

    let child = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({ "parentThreadId": root_thread_id })),
        )
        .await
        .unwrap();
    let child_thread = &child["thread"];
    assert_eq!(
        child_thread["sessionId"].as_str(),
        Some(root_session_id.as_str())
    );
    assert_eq!(
        child_thread["parentThreadId"].as_str(),
        Some(root_thread_id.as_str())
    );
    assert_eq!(
        child_thread["forkedFromId"].as_str(),
        Some(root_thread_id.as_str())
    );
    assert_eq!(
        child_thread["topology"]["spawn_attribution"]["source_thread_id"].as_str(),
        Some(root_thread_id.as_str())
    );
}

#[tokio::test]
async fn thread_resume_returns_loaded_thread() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();

    let resume = app
        .dispatch_request(
            &connection,
            "thread/resume",
            Some(json!({
                "threadId": thread_id,
                "modelProvider": "resume-provider",
                "excludeTurns": true,
            })),
        )
        .await
        .unwrap();
    assert_eq!(resume["thread"]["id"].as_str(), Some(thread_id.as_str()));
    assert_eq!(
        resume["thread"]["modelProvider"].as_str(),
        Some("resume-provider")
    );
    assert_eq!(resume["modelProvider"].as_str(), Some("resume-provider"));
    assert_eq!(resume["thread"]["turns"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn thread_resume_loads_thread_from_metadata_when_not_resident() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let record = app
        .inner
        .metadata_store
        .get_thread_lifecycle(parsed)
        .await
        .unwrap()
        .expect("thread/start should persist a loadable lifecycle record");

    app.inner
        .supervisor
        .shutdown_thread_at(&record.coordinates)
        .await
        .unwrap();
    app.inner.state.write().await.threads.remove(&thread_id);

    let loaded_before_resume = app
        .dispatch_request(&connection, "thread/loaded/list", Some(json!({})))
        .await
        .unwrap();
    assert_eq!(loaded_before_resume["data"].as_array().unwrap().len(), 0);

    let resume = app
        .dispatch_request(
            &connection,
            "thread/resume",
            Some(json!({
                "threadId": thread_id,
                "excludeTurns": true,
            })),
        )
        .await
        .unwrap();
    assert_eq!(resume["thread"]["id"].as_str(), Some(thread_id.as_str()));

    let loaded_after_resume = app
        .dispatch_request(&connection, "thread/loaded/list", Some(json!({})))
        .await
        .unwrap();
    assert_eq!(
        loaded_after_resume["data"].as_array().unwrap()[0].as_str(),
        Some(thread_id.as_str())
    );
}

#[tokio::test]
async fn reload_keeps_bind_time_placement_when_metadata_is_absent_or_corrupt() {
    use crate::EventStore;

    let root = unique_test_root("app-server-placement-reload");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let config_for = |default_placement| {
        let listen = AppServerListenAddr::Unix(
            std::env::temp_dir().join(format!("cooldis-placement-reload-{}.sock", Uuid::now_v7())),
        );
        let mut config = CooldisAppServerConfig::local(listen, &workspace);
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        config.agent_registry_root = root.join("agents");
        config.default_placement = default_placement;
        config
    };
    let first = CooldisAppServer::new_local(config_for(AgentManifestPlacementBinding::default()))
        .await
        .unwrap();
    let (connection, _outbound_rx) = test_connection(first.clone());
    initialize_for_test(&connection).await;

    let absent = first
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let corrupt = first
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let absent_id = ThreadId::parse_str(absent["thread"]["id"].as_str().unwrap()).unwrap();
    let corrupt_id = ThreadId::parse_str(corrupt["thread"]["id"].as_str().unwrap()).unwrap();
    let mut absent_lifecycle = first
        .inner
        .metadata_store
        .get_thread_lifecycle(absent_id)
        .await
        .unwrap()
        .unwrap();
    let mut corrupt_lifecycle = first
        .inner
        .metadata_store
        .get_thread_lifecycle(corrupt_id)
        .await
        .unwrap()
        .unwrap();
    absent_lifecycle
        .metadata
        .remove(THREAD_AGENT_PLACEMENT_METADATA);
    corrupt_lifecycle.metadata.insert(
        THREAD_AGENT_PLACEMENT_METADATA.to_string(),
        "not-json".to_string(),
    );
    first
        .inner
        .metadata_store
        .upsert_thread_lifecycle(absent_lifecycle.clone())
        .await
        .unwrap();
    first
        .inner
        .metadata_store
        .upsert_thread_lifecycle(corrupt_lifecycle.clone())
        .await
        .unwrap();
    first
        .inner
        .supervisor
        .shutdown_thread_at(&absent_lifecycle.coordinates)
        .await
        .unwrap();
    first
        .inner
        .supervisor
        .shutdown_thread_at(&corrupt_lifecycle.coordinates)
        .await
        .unwrap();
    drop(connection);
    drop(first);

    let restarted = CooldisAppServer::new_local(config_for(AgentManifestPlacementBinding {
        target: crate::PlacementTarget::Sandbox,
        executor_ref: Some("executor://new-daemon-default".to_string()),
        config: BTreeMap::new(),
    }))
    .await
    .unwrap();
    let (restarted_connection, _outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&restarted_connection).await;
    let loaded = restarted
        .dispatch_request(&restarted_connection, "thread/loaded/list", Some(json!({})))
        .await
        .unwrap();
    let loaded_ids = loaded["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        loaded_ids,
        BTreeSet::from([
            absent["thread"]["id"].as_str().unwrap(),
            corrupt["thread"]["id"].as_str().unwrap()
        ])
    );

    let session_store = SqliteSessionStore::open(&restarted.inner.session_store_path)
        .await
        .unwrap();
    for lifecycle in [&absent_lifecycle, &corrupt_lifecycle] {
        let events = session_store
            .read_events(&EventStreamId::for_thread(&lifecycle.coordinates), None)
            .await
            .unwrap();
        let bind_events = events
            .iter()
            .filter(|event| event.kind == EventKind::ManifestBindCompleted)
            .collect::<Vec<_>>();
        assert_eq!(bind_events.len(), 2);
        assert!(
            bind_events
                .iter()
                .all(|event| event.payload["placement"]["target"] == "local")
        );
        let placement_events = events
            .iter()
            .filter(|event| event.kind == EventKind::PlacementDecision)
            .collect::<Vec<_>>();
        assert_eq!(placement_events.len(), 2);
        assert!(
            placement_events
                .iter()
                .all(|event| event.payload["placement"] == "local")
        );
    }

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn reload_recovers_absent_or_corrupt_workspace_metadata_as_unbound() {
    let root = unique_test_root("app-server-workspace-reload");
    let app_cwd = root.join("app-cwd");
    let first_workspace = root.join("first-workspace");
    let replacement_workspace = root.join("replacement-workspace");
    let agent_registry_root = root.join("agents");
    for path in [&app_cwd, &first_workspace, &replacement_workspace] {
        std::fs::create_dir_all(path).unwrap();
    }
    let manifest_path = root.join("workspace-reload.cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "workspace-reload"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[workspace]
guest_path = "/work"
min_mode = "rw"

[runtime]
default_cwd = "."
streaming = false
"#,
    )
    .unwrap();
    LocalAgentRegistry::new(&agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap();
    let config_for = |host_path: &Path| {
        let listen = AppServerListenAddr::Unix(
            std::env::temp_dir().join(format!("cooldis-workspace-reload-{}.sock", Uuid::now_v7())),
        );
        let mut config = CooldisAppServerConfig::local(listen, &app_cwd);
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        config.agent_registry_root = agent_registry_root.clone();
        config.default_workspace = Some(AgentManifestWorkspaceBinding {
            host_path: host_path.to_path_buf(),
            mode: crate::AgentManifestWorkspaceMode::ReadWrite,
        });
        config
    };

    let first = CooldisAppServer::new_local(config_for(&first_workspace))
        .await
        .unwrap();
    let (connection, _outbound_rx) = test_connection(first.clone());
    initialize_for_test(&connection).await;
    let absent = first
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({"agentRef": "agent://workspace-reload@latest"})),
        )
        .await
        .unwrap();
    let corrupt = first
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({"agentRef": "agent://workspace-reload@latest"})),
        )
        .await
        .unwrap();
    let drifted = first
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({"agentRef": "agent://workspace-reload@latest"})),
        )
        .await
        .unwrap();
    let valid = first
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({"agentRef": "agent://workspace-reload@latest"})),
        )
        .await
        .unwrap();
    let valid_fork = first
        .dispatch_request(
            &connection,
            "thread/fork",
            Some(json!({"threadId": valid["thread"]["id"]})),
        )
        .await
        .unwrap();
    let absent_id = ThreadId::parse_str(absent["thread"]["id"].as_str().unwrap()).unwrap();
    let corrupt_id = ThreadId::parse_str(corrupt["thread"]["id"].as_str().unwrap()).unwrap();
    let drifted_id = ThreadId::parse_str(drifted["thread"]["id"].as_str().unwrap()).unwrap();
    let mut absent_lifecycle = first
        .inner
        .metadata_store
        .get_thread_lifecycle(absent_id)
        .await
        .unwrap()
        .unwrap();
    let mut corrupt_lifecycle = first
        .inner
        .metadata_store
        .get_thread_lifecycle(corrupt_id)
        .await
        .unwrap()
        .unwrap();
    let mut drifted_lifecycle = first
        .inner
        .metadata_store
        .get_thread_lifecycle(drifted_id)
        .await
        .unwrap()
        .unwrap();
    let valid_lifecycle = first
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(valid["thread"]["id"].as_str().unwrap()).unwrap())
        .await
        .unwrap()
        .unwrap();
    let valid_fork_lifecycle = first
        .inner
        .metadata_store
        .get_thread_lifecycle(
            ThreadId::parse_str(valid_fork["thread"]["id"].as_str().unwrap()).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        SqliteSessionStore::open(&first.inner.session_store_path)
            .await
            .unwrap()
            .read_events(
                &EventStreamId::for_thread(&valid_fork_lifecycle.coordinates),
                None,
            )
            .await
            .unwrap()
            .iter()
            .any(|event| event.kind == EventKind::ManifestBindCompleted),
        "a plain fork must receive its own durable workspace bind witness"
    );
    absent_lifecycle
        .metadata
        .remove(THREAD_AGENT_WORKSPACE_METADATA);
    corrupt_lifecycle.metadata.insert(
        THREAD_AGENT_WORKSPACE_METADATA.to_string(),
        "not-json".to_string(),
    );
    drifted_lifecycle.metadata.insert(
        THREAD_AGENT_WORKSPACE_METADATA.to_string(),
        serde_json::to_string(&AgentManifestResolvedWorkspaceMount {
            guest_path: PathBuf::from("/work"),
            host_path: std::fs::canonicalize(&replacement_workspace).unwrap(),
            mode: crate::AgentManifestWorkspaceMode::ReadWrite,
        })
        .unwrap(),
    );
    first
        .inner
        .metadata_store
        .upsert_thread_lifecycle(absent_lifecycle.clone())
        .await
        .unwrap();
    first
        .inner
        .metadata_store
        .upsert_thread_lifecycle(corrupt_lifecycle.clone())
        .await
        .unwrap();
    first
        .inner
        .metadata_store
        .upsert_thread_lifecycle(drifted_lifecycle.clone())
        .await
        .unwrap();
    for lifecycle in [
        &absent_lifecycle,
        &corrupt_lifecycle,
        &drifted_lifecycle,
        &valid_lifecycle,
    ] {
        first
            .inner
            .supervisor
            .shutdown_thread_at(&lifecycle.coordinates)
            .await
            .unwrap();
    }
    drop(connection);
    drop(first);

    let restarted = CooldisAppServer::new_local(config_for(&replacement_workspace))
        .await
        .unwrap();
    let (restarted_connection, _outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&restarted_connection).await;
    let loaded = restarted
        .dispatch_request(&restarted_connection, "thread/loaded/list", Some(json!({})))
        .await
        .unwrap();
    let loaded_ids = loaded["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    assert!(!loaded_ids.contains(absent["thread"]["id"].as_str().unwrap()));
    assert!(!loaded_ids.contains(corrupt["thread"]["id"].as_str().unwrap()));
    assert!(
        !loaded_ids.contains(drifted["thread"]["id"].as_str().unwrap()),
        "valid JSON that disagrees with the durable bind receipt must not be mounted"
    );
    assert!(loaded_ids.contains(valid["thread"]["id"].as_str().unwrap()));
    assert!(
        loaded_ids.contains(valid_fork["thread"]["id"].as_str().unwrap()),
        "a plain fork must carry a durable workspace bind witness"
    );
    for thread_id in [
        absent["thread"]["id"].as_str().unwrap(),
        corrupt["thread"]["id"].as_str().unwrap(),
        drifted["thread"]["id"].as_str().unwrap(),
    ] {
        let err = restarted
            .dispatch_request(
                &restarted_connection,
                "thread/resume",
                Some(json!({"threadId": thread_id})),
            )
            .await
            .unwrap_err();
        assert!(
            err.message.contains("requires a workspace binding"),
            "unexpected reload error: {}",
            err.message
        );
    }
    let valid_reloaded = restarted
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(valid["thread"]["id"].as_str().unwrap()).unwrap())
        .await
        .unwrap()
        .unwrap();
    let inherited: AgentManifestResolvedWorkspaceMount = serde_json::from_str(
        valid_reloaded
            .metadata
            .get(THREAD_AGENT_WORKSPACE_METADATA)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        inherited.host_path,
        std::fs::canonicalize(&first_workspace).unwrap(),
        "a valid resumed binding must not consult the replacement daemon default"
    );
    assert!(
        std::fs::read_dir(&replacement_workspace)
            .unwrap()
            .next()
            .is_none()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thread_resume_ignores_pre_manifest_operation_name_metadata() {
    let root = unique_test_root("app-server-legacy-start-metadata");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    // lexicon-allow: capsule - existing test provider helper type
    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root(
        &root,
        &workspace,
        provider_client,
        // lexicon-allow: capsule - existing operation binding config type
        CapsuleBindingsConfig::default(),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let mut record = app
        .inner
        .metadata_store
        .get_thread_lifecycle(parsed)
        .await
        .unwrap()
        .expect("thread/start should persist a loadable lifecycle record");

    record
        .metadata
        .retain(|key, _| !key.starts_with("cooldis.agent."));
    record.metadata.insert(
        // lexicon-allow: capsule - old persisted metadata key
        "cooldis.capsule_bindings.operation_names".to_string(),
        "[\"legacy_search\"]".to_string(),
    );
    app.inner
        .supervisor
        .shutdown_thread_at(&record.coordinates)
        .await
        .unwrap();
    app.inner.state.write().await.threads.remove(&thread_id);
    app.inner
        .metadata_store
        .upsert_thread_lifecycle(record)
        .await
        .unwrap();

    let resume = app
        .dispatch_request(
            &connection,
            "thread/resume",
            Some(json!({
                "threadId": thread_id,
                "excludeTurns": true,
            })),
        )
        .await
        .unwrap();
    assert_eq!(resume["thread"]["id"].as_str(), Some(thread_id.as_str()));

    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "turn after legacy metadata", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();
    wait_for_provider_requests(&client, 1).await;
    let requests = client.requests();
    assert_bash_tool_absent_or_omits(&requests[0], "legacy_search");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn model_provider_capabilities_read_returns_local_capabilities() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let capabilities = app
        .dispatch_request(&connection, "modelProvider/capabilities/read", None)
        .await
        .unwrap();
    assert_eq!(
        capabilities,
        json!({
            "namespaceTools": true,
            "imageGeneration": false,
            "webSearch": false,
            "supportsStreaming": false,
        })
    );
}

#[tokio::test]
async fn model_provider_capabilities_read_reports_bedrock_streaming() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-bedrock-cap-test-{}.sock", Uuid::now_v7())),
    );
    let root = std::env::temp_dir().join(format!("cooldis-bedrock-cap-test-{}", Uuid::now_v7()));
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap())
        .with_anthropic_bedrock(
            "us-east-1",
            "AKIA_TEST",
            "secret",
            None,
            "anthropic.claude-test-v1:0",
        );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let capabilities = app
        .dispatch_request(&connection, "modelProvider/capabilities/read", None)
        .await
        .unwrap();
    assert_eq!(capabilities["supportsStreaming"], json!(true));
}

#[tokio::test]
async fn app_server_capsule_bindings_expose_published_operation_to_tools_and_bash() {
    use crate::EventStore;

    let registry_root = unique_test_root("capsule-global-registry");
    let record = publish_echo_operation(&registry_root, "search", "search", "search").await;
    let client = Arc::new(BashCallingCapsuleClient::new(
        "search",
        "search",
        "command -v search && printf cooldis | search",
        "search:cooldis",
    ));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_and_capsule_bindings(
        provider_client,
        CapsuleBindingsConfig::default()
            .with_registry_root(&registry_root)
            .with_global_operation_name("search"),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(&thread_id).unwrap())
        .await
        .unwrap()
        .expect("default manifest thread should persist lifecycle metadata");
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let stream_id = EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    let exa_binding = manifest_operation_binding_by_name(&bind.payload, "search");
    assert_eq!(exa_binding["name"].as_str(), Some("search"));
    assert_eq!(
        exa_binding["artifact_hash"].as_str(),
        Some(record.active_artifact_hash.as_str())
    );
    assert_eq!(exa_binding["operations"], json!(["search"]));
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "use search", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    wait_for_provider_requests(&client, 2).await;
    let requests = client.requests();
    assert!(tool_names(&requests[0]).contains(&"search".to_string()));
    assert_bash_tool_describes(&requests[0], "search");
    let _ = std::fs::remove_dir_all(registry_root);
}

#[tokio::test]
async fn default_manifest_synthesizes_load_all_active_operation_rows() {
    use crate::EventStore;

    let registry_root = unique_test_root("capsule-load-all-registry");
    let alpha = publish_echo_operation(&registry_root, "alpha", "alpha_search", "alpha").await;
    let beta = publish_echo_operation(&registry_root, "beta", "beta_search", "beta").await;
    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client;
    let app = test_app_with_provider_and_capsule_bindings(
        provider_client,
        CapsuleBindingsConfig::default()
            .with_registry_root(&registry_root)
            .with_load_all_active_when_unbound(true),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(&thread_id).unwrap())
        .await
        .unwrap()
        .expect("load-all default manifest thread should persist lifecycle metadata");
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let stream_id = EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    let alpha_binding = manifest_operation_binding_by_name(&bind.payload, "alpha");
    let beta_binding = manifest_operation_binding_by_name(&bind.payload, "beta");
    let thread_binding = manifest_operation_binding_by_name(&bind.payload, COOLDIS_THREADS_PACKAGE);
    assert!(
        bind.payload
            .get("operation_bindings")
            .and_then(Value::as_array)
            .unwrap()
            .iter()
            .all(|binding| binding["name"].as_str() != Some(COOLDIS_PROCESS_PACKAGE))
    );
    assert_eq!(
        alpha_binding,
        &json!({
            "name": "alpha",
            "artifact_hash": alpha.active_artifact_hash,
            "operations": ["alpha_search"]
        })
    );
    assert_eq!(
        beta_binding,
        &json!({
            "name": "beta",
            "artifact_hash": beta.active_artifact_hash,
            "operations": ["beta_search"]
        })
    );
    assert_eq!(
        thread_binding["direct_tools"],
        json!([
            { "operation": THREAD_CANCEL_OPERATION, "tool_name": THREAD_CANCEL_OPERATION },
            { "operation": THREAD_SPAWN_OPERATION, "tool_name": THREAD_SPAWN_OPERATION },
            { "operation": THREAD_STATUS_OPERATION, "tool_name": THREAD_STATUS_OPERATION },
            { "operation": THREAD_SUBMIT_OPERATION, "tool_name": THREAD_SUBMIT_OPERATION },
            { "operation": THREAD_WAIT_OPERATION, "tool_name": THREAD_WAIT_OPERATION }
        ])
    );
    let _ = std::fs::remove_dir_all(registry_root);
}

#[tokio::test]
async fn default_manifest_load_all_requires_registry_root() {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-load-all-root-{}.sock", Uuid::now_v7())),
    );
    let root = unique_test_root("default-manifest-load-all-no-root");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    // lexicon-allow: capsule - existing operation binding config API
    let mut config = CooldisAppServerConfig::local(listen, &workspace).with_capsule_bindings(
        // lexicon-allow: capsule - existing operation binding config type
        CapsuleBindingsConfig::default().with_load_all_active_when_unbound(true),
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");

    let err = match CooldisAppServer::new_local(config).await {
        Ok(_) => panic!("load-all operation bindings without registry root should fail startup"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("operation binding registry_root"),
        "{err}"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn default_manifest_load_all_accepts_registry_with_only_kernel_native_records() {
    use crate::EventStore;

    let registry_root = unique_test_root("operation-empty-registry");
    std::fs::create_dir_all(&registry_root).unwrap();
    // lexicon-allow: capsule - existing test provider helper type
    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client;
    // lexicon-allow: capsule - existing operation binding test helper
    let app = test_app_with_provider_and_capsule_bindings(
        provider_client,
        // lexicon-allow: capsule - existing operation binding config type
        CapsuleBindingsConfig::default()
            .with_registry_root(&registry_root)
            .with_load_all_active_when_unbound(true),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(ThreadId::parse_str(&thread_id).unwrap())
        .await
        .unwrap()
        .expect("empty registry default manifest thread should persist lifecycle metadata");
    let session_store = SqliteSessionStore::open(&app.inner.session_store_path)
        .await
        .unwrap();
    let stream_id = EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await.unwrap();
    let bind = event_by_kind(&events, crate::EventKind::ManifestBindCompleted);
    let bindings = bind.payload["operation_bindings"].as_array().unwrap();
    assert_eq!(bindings.len(), 3);
    assert!(
        bindings
            .iter()
            .all(|binding| binding["name"].as_str() != Some(COOLDIS_PROCESS_PACKAGE))
    );
    let thread_binding = manifest_operation_binding_by_name(&bind.payload, COOLDIS_THREADS_PACKAGE);
    assert_eq!(
        thread_binding["direct_tools"],
        json!([
            { "operation": THREAD_CANCEL_OPERATION, "tool_name": THREAD_CANCEL_OPERATION },
            { "operation": THREAD_SPAWN_OPERATION, "tool_name": THREAD_SPAWN_OPERATION },
            { "operation": THREAD_STATUS_OPERATION, "tool_name": THREAD_STATUS_OPERATION },
            { "operation": THREAD_SUBMIT_OPERATION, "tool_name": THREAD_SUBMIT_OPERATION },
            { "operation": THREAD_WAIT_OPERATION, "tool_name": THREAD_WAIT_OPERATION }
        ])
    );
    let notify_binding = manifest_operation_binding_by_name(&bind.payload, COOLDIS_NOTIFY_PACKAGE);
    assert_eq!(
        json_array_string_set(&notify_binding["operations"]),
        BTreeSet::from([
            NOTIFY_PREVIEW_OPERATION.to_string(),
            CHANNEL_EMIT_OPERATION.to_string()
        ])
    );
    let schedule_binding =
        manifest_operation_binding_by_name(&bind.payload, COOLDIS_SCHEDULE_PACKAGE);
    assert_eq!(
        json_array_string_set(&schedule_binding["operations"]),
        BTreeSet::from([
            MANDATE_START_OPERATION.to_string(),
            MANDATE_REVOKE_OPERATION.to_string(),
            MANDATE_LIST_OPERATION.to_string()
        ])
    );
    let _ = std::fs::remove_dir_all(registry_root);
}

#[tokio::test]
async fn app_server_capsule_bindings_reject_thread_operation_scope_injection() {
    let registry_root = unique_test_root("capsule-thread-registry");
    publish_echo_operation(&registry_root, "global", "global_search", "global").await;
    publish_echo_operation(&registry_root, "thread", "thread_search", "thread").await;
    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_and_capsule_bindings(
        provider_client,
        CapsuleBindingsConfig::default()
            .with_registry_root(&registry_root)
            .with_global_operation_name("global"),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let err = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "capsuleBindings": {
                    "operationNames": ["global"]
                }
            })),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, -32602);
    assert!(
        err.message
            .contains("operations are declared in an agent manifest")
    );
    assert!(client.requests().is_empty());
    let _ = std::fs::remove_dir_all(registry_root);
}

#[tokio::test]
async fn app_server_capsule_binding_methods_do_not_update_manifest_runtime_scope() {
    let registry_root = unique_test_root("capsule-binding-methods");
    let record = publish_echo_operation(&registry_root, "search", "search", "search").await;
    let client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_and_capsule_bindings(
        provider_client,
        CapsuleBindingsConfig::default().with_registry_root(&registry_root),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let set = app
        .dispatch_request(
            &connection,
            "capsule/binding/set",
            Some(json!({
                "scope": { "kind": "global" },
                "operationName": "search",
                "artifactHash": record.active_artifact_hash,
            })),
        )
        .await
        .unwrap();
    assert_eq!(set["binding"]["operationName"].as_str(), Some("search"));
    assert_eq!(
        set["binding"]["target"]["artifactHash"].as_str(),
        Some(record.active_artifact_hash.as_str())
    );

    let resolved = app
        .dispatch_request(&connection, "capsule/binding/resolve", Some(json!({})))
        .await
        .unwrap();
    assert_eq!(
        resolved["snapshot"]["records"][0]["name"].as_str(),
        Some("search")
    );

    let bound_thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let bound_thread_id = bound_thread["thread"]["id"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": bound_thread_id,
            "input": [{ "type": "text", "text": "with search", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();
    wait_for_provider_requests(&client, 1).await;

    let delete = app
        .dispatch_request(
            &connection,
            "capsule/binding/delete",
            Some(json!({
                "scope": { "kind": "global" },
                "operationName": "search",
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        delete["binding"]["target"]["kind"].as_str(),
        Some("tombstone")
    );

    let list = app
        .dispatch_request(
            &connection,
            "capsule/binding/list",
            Some(json!({ "scope": { "kind": "global" } })),
        )
        .await
        .unwrap();
    assert_eq!(
        list["data"][0]["target"]["kind"].as_str(),
        Some("tombstone")
    );

    let unbound_thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let unbound_thread_id = unbound_thread["thread"]["id"].as_str().unwrap().to_string();
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": unbound_thread_id,
            "input": [{ "type": "text", "text": "without search", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();
    wait_for_provider_requests(&client, 2).await;

    let requests = client.requests();
    assert!(!tool_names(&requests[0]).contains(&"search".to_string()));
    assert_bash_tool_absent_or_omits(&requests[0], "search");
    assert!(!tool_names(&requests[1]).contains(&"search".to_string()));
    assert_bash_tool_absent_or_omits(&requests[1], "search");
    let _ = std::fs::remove_dir_all(registry_root);
}

#[tokio::test]
async fn app_server_capsule_binding_methods_do_not_reload_as_manifest_runtime_scope() {
    let registry_root = unique_test_root("capsule-binding-reload");
    let record = publish_echo_operation(&registry_root, "search", "search", "search").await;
    let first_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = first_client;
    let app = test_app_with_provider_and_capsule_bindings(
        provider_client,
        CapsuleBindingsConfig::default().with_registry_root(&registry_root),
    )
    .await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    app.dispatch_request(
        &connection,
        "capsule/binding/set",
        Some(json!({
            "scope": { "kind": "global" },
            "operationName": "search",
            "artifactHash": record.active_artifact_hash,
        })),
    )
    .await
    .unwrap();

    // lexicon-allow: capsule - existing test helper type
    let second_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = second_client.clone();
    let restarted = test_app_with_provider_and_capsule_bindings(
        provider_client,
        CapsuleBindingsConfig::default().with_registry_root(&registry_root),
    )
    .await;
    let (restarted_connection, _outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&restarted_connection).await;

    let thread = restarted
        .dispatch_request(&restarted_connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    restarted
        .dispatch_request(
            &restarted_connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "after restart", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    wait_for_provider_requests(&second_client, 1).await;

    let requests = second_client.requests();
    assert!(!tool_names(&requests[0]).contains(&"search".to_string()));
    assert_bash_tool_absent_or_omits(&requests[0], "search");
    let _ = std::fs::remove_dir_all(registry_root);
}

#[tokio::test]
async fn app_server_loads_threads_and_rebuilds_context_from_shared_session_store() {
    let root = unique_test_root("app-server-load-session");
    let first_cwd = root.join("workspace-a");
    let restarted_cwd = root.join("workspace-b");
    std::fs::create_dir_all(&first_cwd).unwrap();
    std::fs::create_dir_all(&restarted_cwd).unwrap();
    let thread_id = {
        let first_client = Arc::new(InspectingCapsuleClient::default());
        let provider_client: Arc<dyn ProviderClient> = first_client.clone();
        let app = test_app_with_provider_root(
            &root,
            &first_cwd,
            provider_client,
            CapsuleBindingsConfig::default(),
        )
        .await;
        let (connection, _outbound_rx) = test_connection(app.clone());
        initialize_for_test(&connection).await;

        let thread = app
            .dispatch_request(
                &connection,
                "thread/start",
                Some(json!({ "cwd": cwd_string(&first_cwd), "ephemeral": true })),
            )
            .await
            .unwrap();
        let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
        app.dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "first durable turn", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
        wait_for_provider_requests(&first_client, 1).await;
        wait_for_session_text(&app, &thread_id, "inspected").await;
        thread_id
    };

    let second_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = second_client.clone();
    let restarted = test_app_with_provider_root(
        &root,
        &restarted_cwd,
        provider_client,
        CapsuleBindingsConfig::default(),
    )
    .await;
    let (restarted_connection, _outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&restarted_connection).await;

    let loaded = restarted
        .dispatch_request(&restarted_connection, "thread/loaded/list", Some(json!({})))
        .await
        .unwrap();
    let loaded_ids = loaded["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert_eq!(loaded_ids, vec![thread_id.as_str()]);

    let thread = restarted
        .dispatch_request(
            &restarted_connection,
            "thread/read",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    assert_eq!(
        thread["thread"]["cwd"].as_str(),
        Some(cwd_string(&first_cwd).as_str())
    );
    assert_eq!(thread["thread"]["ephemeral"].as_bool(), Some(true));

    restarted
            .dispatch_request(
                &restarted_connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": "second turn after restart", "text_elements": [] }],
                })),
            )
            .await
            .unwrap();
    wait_for_provider_requests(&second_client, 1).await;

    let requests = second_client.requests();
    let restored_context = text_from_canonical_messages(&requests[0].messages);
    assert!(
        restored_context.contains("first durable turn"),
        "restored context did not include first user turn: {restored_context}"
    );
    assert!(
        restored_context.contains("inspected"),
        "restored context did not include first assistant turn: {restored_context}"
    );
    assert!(
        restored_context.contains("second turn after restart"),
        "restored context did not include second user turn: {restored_context}"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn restored_thread_start_streams_and_thread_read_returns_persisted_turns() {
    let root = unique_test_root("app-server-restored-history");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let thread_id = {
        let first_client = Arc::new(InspectingCapsuleClient::default());
        let provider_client: Arc<dyn ProviderClient> = first_client.clone();
        let app = test_app_with_provider_root(
            &root,
            &workspace,
            provider_client,
            CapsuleBindingsConfig::default(),
        )
        .await;
        let (connection, mut outbound_rx) = test_connection(app.clone());
        initialize_for_test(&connection).await;

        let thread = app
            .dispatch_request(&connection, "thread/start", Some(json!({})))
            .await
            .unwrap();
        let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
        for (idx, text) in ["first persisted turn", "second persisted turn"]
            .into_iter()
            .enumerate()
        {
            let turn = app
                .dispatch_request(
                    &connection,
                    "turn/start",
                    Some(json!({
                        "threadId": thread_id,
                        "input": [{ "type": "text", "text": text, "text_elements": [] }],
                    })),
                )
                .await
                .unwrap();
            wait_for_provider_requests(&first_client, idx + 1).await;
            wait_for_turn_completed_notification(
                &mut outbound_rx,
                &thread_id,
                turn["turn"]["id"].as_str().unwrap(),
            )
            .await;
        }

        let live_read = app
            .dispatch_request(
                &connection,
                "thread/read",
                Some(json!({ "threadId": thread_id })),
            )
            .await
            .unwrap();
        let live_turns = live_read["thread"]["turns"].as_array().unwrap();
        assert_eq!(
            turn_item_texts(live_turns),
            vec![
                vec!["first persisted turn".to_string(), "inspected".to_string()],
                vec!["second persisted turn".to_string(), "inspected".to_string()],
            ]
        );
        thread_id
    };

    let second_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = second_client.clone();
    let restarted = test_app_with_provider_root(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default(),
    )
    .await;
    let (restarted_connection, mut restarted_outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&restarted_connection).await;

    let listed = restarted
        .dispatch_request(&restarted_connection, "thread/list", None)
        .await
        .unwrap();
    let entry = listed["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|thread| thread["id"].as_str() == Some(thread_id.as_str()))
        .expect("restored thread should be listed");
    assert_eq!(entry["preview"].as_str(), Some("first persisted turn"));

    let restored_read = restarted
        .dispatch_request(
            &restarted_connection,
            "thread/read",
            Some(json!({ "threadId": thread_id })),
        )
        .await
        .unwrap();
    let restored_turns = restored_read["thread"]["turns"].as_array().unwrap();
    assert_eq!(
        turn_item_texts(restored_turns),
        vec![
            vec!["first persisted turn".to_string(), "inspected".to_string()],
            vec!["second persisted turn".to_string(), "inspected".to_string()],
        ]
    );
    assert!(restored_turns.iter().all(|turn| {
        turn["status"].as_str() == Some("completed") && !turn["completedAt"].is_null()
    }));

    let turn = restarted
            .dispatch_request(
                &restarted_connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": "third turn after restore", "text_elements": [] }],
                })),
            )
            .await
            .unwrap();
    wait_for_provider_requests(&second_client, 1).await;
    let completed = wait_for_turn_completed_notification(
        &mut restarted_outbound_rx,
        &thread_id,
        turn["turn"]["id"].as_str().unwrap(),
    )
    .await;
    assert!(turn_has_agent_delta(&completed, "inspected"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fast_stream_completion_reads_saved_assistant_when_projection_is_empty() {
    let root = unique_test_root("app-server-fast-stream-completion");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let expected = "FIRST:COOLDIS_APP_RESUME_debug1";
    let client = Arc::new(SequencedStreamCapsuleClient::new_modes([
        SequencedStreamResponse::text_delta(expected),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default(),
        true,
    )
    .await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn_id = submit_provider_turn_without_subscription(
        &app,
        &thread_id,
        vec![json!({ "type": "text", "text": "instant stream", "text_elements": [] })],
    )
    .await;
    wait_for_provider_requests(&client, 1).await;
    wait_for_session_text(&app, &thread_id, expected).await;

    {
        let mut state = app.inner.state.write().await;
        let thread = state.threads.get_mut(&thread_id).unwrap();
        let turn = thread.turns.get_mut(&turn_id).unwrap();
        turn.assistant_started = true;
        turn.assistant_completed = true;
        turn.assistant_text.clear();
        turn.items
            .retain(|item| item.get("type").and_then(Value::as_str) != Some("agentMessage"));
    }

    let handle = app.handle_for_thread(&thread_id).await.unwrap();
    connection.subscribe_thread(handle).await;
    complete_turn_after_settle(app.clone(), thread_id.clone(), turn_id.clone()).await;

    let completed =
        wait_for_turn_completed_notification(&mut outbound_rx, &thread_id, &turn_id).await;
    assert_eq!(
        completed_turn_agent_text(&completed).as_deref(),
        Some(expected)
    );
    assert_eq!(
        latest_assistant_text(&app, &thread_id).await.as_deref(),
        Some(expected)
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn lagged_thread_stream_resnapshots_from_durable_truth() {
    let root = unique_test_root("app-server-lagged-stream-resnapshot");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let deltas = (0..1_100)
        .map(|index| format!("{index:04}|"))
        .collect::<Vec<_>>();
    let expected = deltas.concat();
    let provider_client: Arc<dyn ProviderClient> = Arc::new(BurstStreamClient { deltas });
    let app = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        // lexicon-allow: capsule - existing app-server test fixture config type
        CapsuleBindingsConfig::default(),
        true,
    )
    .await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "burst", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    let turn_id = turn["turn"]["id"].as_str().unwrap().to_string();

    let (saw_resync_started, resynced) =
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut saw_resync_started = false;
            loop {
                let message = outbound_rx
                    .recv()
                    .await
                    .expect("notification stream closed");
                let JsonRpcMessage::Notification(notification) = message else {
                    continue;
                };
                match notification.method.as_str() {
                    "thread/resync/started" => saw_resync_started = true,
                    "thread/resynced" => {
                        break (
                            saw_resync_started,
                            notification.params.expect("resync params"),
                        );
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("lagged stream did not resynchronize");

    assert!(saw_resync_started, "lag recovery must be explicit");
    assert_eq!(resynced["threadId"].as_str(), Some(thread_id.as_str()));
    assert!(resynced["laggedEvents"].as_u64().unwrap() > 0);
    let resynced_turn = resynced["thread"]["turns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|turn| turn["id"].as_str() == Some(turn_id.as_str()))
        .expect("resync snapshot should contain active turn");
    let resynced_text = resynced_turn["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"].as_str() == Some("agentMessage"))
        .and_then(|item| item["text"].as_str())
        .expect("resync snapshot should contain assistant text");
    assert_eq!(resynced_text, expected);
    assert_eq!(
        latest_assistant_text(&app, &thread_id).await.as_deref(),
        Some(expected.as_str()),
        "durable session truth should contain the complete stream",
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "current_thread")]
async fn lag_resync_degrades_when_turn_submission_has_no_entry_id() {
    let app = test_app().await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn_id = "turn-ingress-applied".to_string();
    let handle = app.handle_for_thread(&thread_id).await.unwrap();
    {
        let mut state = app.inner.state.write().await;
        let thread = state.threads.get_mut(&thread_id).unwrap();
        thread.active_turn_id = Some(turn_id.clone());
        thread.turns.insert(
            turn_id.clone(),
            AppServerTurnState::new(
                turn_id.clone(),
                vec![json!({ "type": "text", "text": "ingress", "text_elements": [] })],
            ),
        );
    }
    handle
        .append_thread_event_record(crate::NewEventRecord::discharged(
            handle.context().coordinates.clone(),
            EventKind::TurnSubmitted,
            json!({
                "schema": EventKind::TurnSubmitted.payload_schema_id(),
                "turn_id": turn_id,
            }),
            crate::EventProvenance {
                source_event_ids: vec![crate::EventRecordId::new()],
                discharged_by: Some("projector:io-ingress-apply".to_string()),
                function: Some("ingress_turn_submit/v1".to_string()),
                ..crate::EventProvenance::default()
            },
        ))
        .await
        .unwrap();

    assert!(
        resynchronize_thread_after_lag(&app, &handle, &thread_id, 1, Some(&turn_id)).await,
        "missing entry_id should degrade the lag resync instead of failing it",
    );

    let resynced = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let message = outbound_rx
                .recv()
                .await
                .expect("notification stream closed");
            let JsonRpcMessage::Notification(notification) = message else {
                continue;
            };
            match notification.method.as_str() {
                "thread/resynced" => break notification.params.expect("resync params"),
                "thread/resync/failed" => panic!("degraded lag resync unexpectedly failed"),
                _ => {}
            }
        }
    })
    .await
    .expect("degraded lag resync did not complete");

    let resynced_turn = resynced["thread"]["turns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|turn| turn["id"].as_str() == Some(turn_id.as_str()))
        .expect("resync snapshot should retain the active turn");
    assert!(
        resynced_turn["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["type"].as_str() != Some("agentMessage")),
        "degraded resync must not synthesize a mid-turn projection",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn lagged_idle_thread_resynchronizes_without_another_status_change() {
    let app = test_app().await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let handle = app.handle_for_thread(&thread_id).await.unwrap();
    assert_eq!(handle.status(), ThreadStatus::Idle);
    tokio::task::yield_now().await;

    for index in 0..1_100 {
        handle.emit_runtime(RuntimeEventKind::TextDelta {
            text: format!("idle-{index}"),
        });
    }

    let (saw_started, saw_resynced) =
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let mut saw_started = false;
            loop {
                let message = outbound_rx
                    .recv()
                    .await
                    .expect("notification stream closed");
                let JsonRpcMessage::Notification(notification) = message else {
                    continue;
                };
                match notification.method.as_str() {
                    "thread/resync/started" => saw_started = true,
                    "thread/resynced" => break (saw_started, true),
                    "thread/resync/failed" => break (saw_started, false),
                    _ => {}
                }
            }
        })
        .await
        .expect("idle lag did not finish resynchronization");

    assert!(saw_started, "idle lag recovery must be explicit");
    assert!(saw_resynced, "idle lag recovery unexpectedly failed");
}

#[tokio::test(flavor = "current_thread")]
async fn lag_resync_does_not_apply_stale_idle_to_a_new_running_turn() {
    let root = unique_test_root("app-server-lag-resync-status-race");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let client = Arc::new(LagThenBlockStreamClient::default());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        // lexicon-allow: capsule - existing app-server test fixture config type
        CapsuleBindingsConfig::default(),
        true,
    )
    .await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let gate = install_thread_resync_test_gate(&thread_id);
    app.dispatch_request(
        &connection,
        "turn/start",
        Some(json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "lag first turn", "text_elements": [] }],
        })),
    )
    .await
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(3), gate.wait_until_entered())
        .await
        .expect("watcher did not enter lag resynchronization");

    let second = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "block second turn", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    let second_turn_id = second["turn"]["id"].as_str().unwrap().to_string();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        client.wait_for_second_request(),
    )
    .await
    .expect("second provider turn did not start");
    assert_eq!(
        app.handle_for_thread(&thread_id).await.unwrap().status(),
        ThreadStatus::Running
    );
    gate.release();

    let status_after_resync = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut saw_resynced = false;
        loop {
            let message = outbound_rx
                .recv()
                .await
                .expect("notification stream closed");
            let JsonRpcMessage::Notification(notification) = message else {
                continue;
            };
            if notification.method == "thread/resynced" {
                saw_resynced = true;
            } else if saw_resynced && notification.method == "thread/status/changed" {
                break notification.params.expect("thread status params");
            }
        }
    })
    .await
    .expect("watcher did not publish status after resynchronization");

    assert_eq!(status_after_resync["status"]["type"], "active");
    {
        let state = app.inner.state.read().await;
        let thread = state.threads.get(&thread_id).unwrap();
        assert_eq!(
            thread.active_turn_id.as_deref(),
            Some(second_turn_id.as_str())
        );
        assert_eq!(thread.status, ThreadStatus::Running);
    }
    client.release_second_request();
    wait_for_turn_completed_notification(&mut outbound_rx, &thread_id, &second_turn_id).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fast_stream_after_thread_start_idle_completes_with_assistant_text() {
    let root = unique_test_root("app-server-fast-stream-after-idle");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let expected = "FIRST:COOLDIS_APP_RESUME_after_idle";
    let client = Arc::new(SequencedStreamCapsuleClient::new_modes([
        SequencedStreamResponse::text_delta(expected),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default(),
        true,
    )
    .await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{
                    "type": "text",
                    "text": "Remember this exact marker. Reply exactly FIRST:COOLDIS_APP_RESUME_after_idle.",
                    "text_elements": []
                }],
            })),
        )
        .await
        .unwrap();
    wait_for_provider_requests(&client, 1).await;

    let turn_id = turn["turn"]["id"].as_str().unwrap();
    let (deltas, completed) =
        collect_agent_deltas_until_turn_completed(&mut outbound_rx, &thread_id, turn_id).await;
    assert_eq!(deltas, expected);
    assert_eq!(
        completed_turn_agent_text(&completed).as_deref(),
        Some(expected)
    );
    assert_eq!(
        latest_assistant_text(&app, &thread_id).await.as_deref(),
        Some(expected)
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn provider_failure_turn_completed_carries_error() {
    let root = unique_test_root("app-server-provider-failure");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let client = Arc::new(FailingProviderClient::new("scripted provider failure"));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default(),
        true,
    )
    .await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "please fail", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    wait_for_provider_requests(&client, 1).await;

    let turn_id = turn["turn"]["id"].as_str().unwrap();
    let completed =
        wait_for_failed_turn_and_closed_thread(&mut outbound_rx, &thread_id, turn_id).await;
    assert_eq!(completed["turn"]["status"].as_str(), Some("failed"));
    let error = &completed["turn"]["error"];
    assert_ne!(error, &Value::Null);
    assert_eq!(error["codexErrorInfo"].as_str(), Some("other"));
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|message| message.contains("scripted provider failure")),
        "turn error did not include provider failure: {completed:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn restored_thread_provider_requests_end_with_current_input() {
    let root = unique_test_root("app-server-restored-context-current-input");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let thread_id = {
        let first_client = Arc::new(InspectingCapsuleClient::default());
        let provider_client: Arc<dyn ProviderClient> = first_client.clone();
        let app = test_app_with_provider_root(
            &root,
            &workspace,
            provider_client,
            CapsuleBindingsConfig::default(),
        )
        .await;
        let (connection, mut outbound_rx) = test_connection(app.clone());
        initialize_for_test(&connection).await;

        let thread = app
            .dispatch_request(&connection, "thread/start", Some(json!({})))
            .await
            .unwrap();
        let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
        let turn = app
            .dispatch_request(
                &connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": "before restart", "text_elements": [] }],
                })),
            )
            .await
            .unwrap();
        wait_for_provider_requests(&first_client, 1).await;
        assert_eq!(
            last_user_message_text(&first_client.requests()[0]).as_deref(),
            Some("before restart")
        );
        wait_for_turn_completed_notification(
            &mut outbound_rx,
            &thread_id,
            turn["turn"]["id"].as_str().unwrap(),
        )
        .await;
        thread_id
    };

    let second_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = second_client.clone();
    let restarted = test_app_with_provider_root(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default(),
    )
    .await;
    let (restarted_connection, mut restarted_outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&restarted_connection).await;

    restarted
        .dispatch_request(
            &restarted_connection,
            "thread/resume",
            Some(json!({
                "threadId": thread_id,
                "excludeTurns": true,
            })),
        )
        .await
        .unwrap();

    for (idx, text) in ["restored current input one", "restored current input two"]
        .into_iter()
        .enumerate()
    {
        let turn = restarted
            .dispatch_request(
                &restarted_connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": text, "text_elements": [] }],
                })),
            )
            .await
            .unwrap();
        wait_for_provider_requests(&second_client, idx + 1).await;
        let requests = second_client.requests();
        assert_eq!(
            last_user_message_text(&requests[idx]).as_deref(),
            Some(text),
            "provider request {idx} should end with the just-submitted restored input"
        );
        wait_for_turn_completed_notification(
            &mut restarted_outbound_rx,
            &thread_id,
            turn["turn"]["id"].as_str().unwrap(),
        )
        .await;
    }

    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn restored_thread_notifications_use_current_completion_and_persist_once() {
    let root = unique_test_root("app-server-restored-current-notifications");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let thread_id = {
        let first_client = Arc::new(SequencedStreamCapsuleClient::new([
            "before restart completion",
        ]));
        let provider_client: Arc<dyn ProviderClient> = first_client.clone();
        let app = test_app_with_provider_root_and_stream(
            &root,
            &workspace,
            provider_client,
            CapsuleBindingsConfig::default(),
            true,
        )
        .await;
        let (connection, mut outbound_rx) = test_connection(app.clone());
        initialize_for_test(&connection).await;

        let thread = app
            .dispatch_request(&connection, "thread/start", Some(json!({})))
            .await
            .unwrap();
        let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
        let turn = app
            .dispatch_request(
                &connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": "before restart", "text_elements": [] }],
                })),
            )
            .await
            .unwrap();
        let turn_id = turn["turn"]["id"].as_str().unwrap();
        let (delta, _completed) =
            collect_agent_deltas_until_turn_completed(&mut outbound_rx, &thread_id, turn_id).await;
        assert_eq!(delta, "before restart completion");
        assert_eq!(
            wait_for_assistant_texts(&app, &thread_id, 1).await,
            vec!["before restart completion".to_string()]
        );
        thread_id
    };

    let second_client = Arc::new(SequencedStreamCapsuleClient::new([
        "restored completion one",
        "restored completion two",
    ]));
    let provider_client: Arc<dyn ProviderClient> = second_client.clone();
    let restarted = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default(),
        true,
    )
    .await;
    let (restarted_connection, mut restarted_outbound_rx) = test_connection(restarted.clone());
    initialize_for_test(&restarted_connection).await;

    restarted
        .dispatch_request(
            &restarted_connection,
            "thread/resume",
            Some(json!({
                "threadId": thread_id,
                "excludeTurns": true,
            })),
        )
        .await
        .unwrap();

    for (idx, (input, expected)) in [
        ("restored prompt one", "restored completion one"),
        ("restored prompt two", "restored completion two"),
    ]
    .into_iter()
    .enumerate()
    {
        let turn = restarted
            .dispatch_request(
                &restarted_connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": input, "text_elements": [] }],
                })),
            )
            .await
            .unwrap();
        wait_for_provider_requests(&second_client, idx + 1).await;
        let turn_id = turn["turn"]["id"].as_str().unwrap();
        let (delta, completed) = collect_agent_deltas_until_turn_completed(
            &mut restarted_outbound_rx,
            &thread_id,
            turn_id,
        )
        .await;
        assert_eq!(delta, expected);
        assert!(turn_has_agent_delta(&completed, expected));
        let assistant_texts = wait_for_assistant_texts(&restarted, &thread_id, idx + 2).await;
        assert_eq!(assistant_texts.last().map(String::as_str), Some(expected));
    }

    let assistant_texts = wait_for_assistant_texts(&restarted, &thread_id, 3).await;
    assert_eq!(
        assistant_texts,
        vec![
            "before restart completion".to_string(),
            "restored completion one".to_string(),
            "restored completion two".to_string(),
        ]
    );

    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn restored_thread_multiple_subscribers_receive_single_applied_turns() {
    let root = unique_test_root("app-server-restored-multi-subscriber");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let thread_id = {
        let first_client = Arc::new(SequencedStreamCapsuleClient::new([
            "before restart completion",
        ]));
        let provider_client: Arc<dyn ProviderClient> = first_client.clone();
        let app = test_app_with_provider_root_and_stream(
            &root,
            &workspace,
            provider_client,
            CapsuleBindingsConfig::default(),
            true,
        )
        .await;
        let (connection, mut outbound_rx) = test_connection(app.clone());
        initialize_for_test(&connection).await;

        let thread = app
            .dispatch_request(&connection, "thread/start", Some(json!({})))
            .await
            .unwrap();
        let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
        let turn = app
            .dispatch_request(
                &connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": "before restart", "text_elements": [] }],
                })),
            )
            .await
            .unwrap();
        let turn_id = turn["turn"]["id"].as_str().unwrap();
        collect_agent_deltas_until_turn_completed(&mut outbound_rx, &thread_id, turn_id).await;
        assert_eq!(wait_for_assistant_texts(&app, &thread_id, 1).await.len(), 1);
        thread_id
    };

    let second_client = Arc::new(SequencedStreamCapsuleClient::new_modes([
        SequencedStreamResponse::text_delta("streamed once"),
        SequencedStreamResponse::content("fallback once"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = second_client.clone();
    let restarted = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        CapsuleBindingsConfig::default(),
        true,
    )
    .await;
    let (requesting_connection, mut requesting_rx) = test_connection(restarted.clone());
    let (observer_connection, mut observer_rx) = test_connection(restarted.clone());
    initialize_for_test(&requesting_connection).await;
    initialize_for_test(&observer_connection).await;

    for connection in [&requesting_connection, &observer_connection] {
        restarted
            .dispatch_request(
                connection,
                "thread/resume",
                Some(json!({
                    "threadId": thread_id,
                    "excludeTurns": true,
                })),
            )
            .await
            .unwrap();
    }

    for (idx, (input, expected)) in [
        ("streaming path prompt", "streamed once"),
        ("fallback path prompt", "fallback once"),
    ]
    .into_iter()
    .enumerate()
    {
        let turn = restarted
            .dispatch_request(
                &requesting_connection,
                "turn/start",
                Some(json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": input, "text_elements": [] }],
                })),
            )
            .await
            .unwrap();
        wait_for_provider_requests(&second_client, idx + 1).await;
        let turn_id = turn["turn"]["id"].as_str().unwrap();
        let (requesting_seen, observer_seen) = tokio::join!(
            collect_agent_deltas_until_turn_completed(&mut requesting_rx, &thread_id, turn_id),
            collect_agent_deltas_until_turn_completed(&mut observer_rx, &thread_id, turn_id)
        );
        for (label, (delta, completed)) in
            [("requesting", requesting_seen), ("observer", observer_seen)]
        {
            assert_eq!(delta, expected, "{label} connection saw wrong delta");
            assert_eq!(
                completed_turn_agent_text(&completed).as_deref(),
                Some(expected),
                "{label} connection saw multiply-applied completed item"
            );
        }
        assert_no_extra_turn_delta_or_completed(&mut requesting_rx, &thread_id, turn_id).await;
        assert_no_extra_turn_delta_or_completed(&mut observer_rx, &thread_id, turn_id).await;

        let assistant_texts = wait_for_assistant_texts(&restarted, &thread_id, idx + 2).await;
        assert_eq!(assistant_texts.last().map(String::as_str), Some(expected));
    }

    assert_eq!(
        wait_for_assistant_texts(&restarted, &thread_id, 3).await,
        vec![
            "before restart completion".to_string(),
            "streamed once".to_string(),
            "fallback once".to_string(),
        ]
    );

    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn app_server_unix_socket_restart_loads_saved_session_and_continues_thread() {
    let root = unique_test_root("app-server-socket-load-session");
    let socket = PathBuf::from("/tmp").join(format!("cdis-{}.sock", Uuid::now_v7().simple()));
    let first_cwd = root.join("workspace-a");
    let restarted_cwd = root.join("workspace-b");
    std::fs::create_dir_all(&first_cwd).unwrap();
    std::fs::create_dir_all(&restarted_cwd).unwrap();

    let thread_id = {
        let first_client = Arc::new(InspectingCapsuleClient::default());
        let provider_client: Arc<dyn ProviderClient> = first_client;
        let listen = AppServerListenAddr::Unix(socket.clone());
        let app = test_app_with_provider_root_and_listen(
            &root,
            &first_cwd,
            listen.clone(),
            provider_client,
            // lexicon-allow: capsule - existing test helper parameter type
            CapsuleBindingsConfig::default(),
        )
        .await;
        let server = app.clone();
        let server_task = tokio::spawn(async move { server.serve(listen).await });
        let mut client = connect_tui_test_client(&socket, "socket-load-first").await;

        let thread = client
            .thread_start(json!({ "cwd": cwd_string(&first_cwd), "ephemeral": true }))
            .await
            .unwrap();
        let turn = client
            .turn_start_text(&thread.id, "socket first durable turn")
            .await
            .unwrap();
        let completed = client
            .wait_for_turn_completed(&thread.id, &turn.id, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        assert!(completed.assistant_text.contains("inspected"));
        wait_for_session_text(&app, &thread.id, "socket first durable turn").await;
        wait_for_session_text(&app, &thread.id, "inspected").await;

        client.close().await.unwrap();
        server_task.abort();
        let _ = server_task.await;
        thread.id
    };

    // lexicon-allow: capsule - existing test provider helper type
    let second_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = second_client.clone();
    let listen = AppServerListenAddr::Unix(socket.clone());
    let restarted = test_app_with_provider_root_and_listen(
        &root,
        &restarted_cwd,
        listen.clone(),
        provider_client,
        // lexicon-allow: capsule - existing test helper parameter type
        CapsuleBindingsConfig::default(),
    )
    .await;
    let server = restarted.clone();
    let server_task = tokio::spawn(async move { server.serve(listen).await });
    let mut client = connect_tui_test_client(&socket, "socket-load-second").await;

    let loaded = client.loaded_thread_list().await.unwrap();
    let loaded_ids = loaded["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert_eq!(loaded_ids, vec![thread_id.as_str()]);

    let resumed = client
        .request(
            "thread/resume",
            json!({ "threadId": thread_id, "excludeTurns": true }),
        )
        .await
        .unwrap();
    assert_eq!(
        resumed["thread"]["cwd"].as_str(),
        Some(cwd_string(&first_cwd).as_str())
    );
    assert_eq!(resumed["thread"]["ephemeral"].as_bool(), Some(true));

    let turn = client
        .turn_start_text(&thread_id, "socket second turn after restart")
        .await
        .unwrap();
    client
        .wait_for_turn_completed(&thread_id, &turn.id, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    wait_for_provider_requests(&second_client, 1).await;

    let requests = second_client.requests();
    let restored_context = text_from_canonical_messages(&requests[0].messages);
    assert!(
        restored_context.contains("socket first durable turn"),
        "restored context did not include first user turn: {restored_context}"
    );
    assert!(
        restored_context.contains("inspected"),
        "restored context did not include first assistant turn: {restored_context}"
    );
    assert!(
        restored_context.contains("socket second turn after restart"),
        "restored context did not include second user turn: {restored_context}"
    );

    client.close().await.unwrap();
    server_task.abort();
    let _ = server_task.await;
    let _ = std::fs::remove_file(socket);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_websocket_listen_accepts_codex_tui_client() {
    let root = unique_test_root("app-server-websocket-listen");
    let addr = unused_loopback_addr();
    let listen = AppServerListenAddr::parse(&format!("ws://{addr}/rpc")).unwrap();
    // lexicon-allow: capsule - existing test helper type
    let first_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = first_client;
    let app = test_app_with_provider_root_and_listen(
        &root,
        &root,
        listen.clone(),
        provider_client,
        CapsuleBindingsConfig::default(), // lexicon-allow: capsule - existing test helper parameter type
    )
    .await;
    let token = mint_app_server_test_token(&app).await;
    let server = app.clone();
    let server_task = tokio::spawn(async move { server.serve(listen).await });
    let mut client = connect_ws_tui_test_client(&format!("ws://{addr}/rpc"), &token).await;

    assert_eq!(
        client.initialize_result()["userAgent"],
        "cooldis-app-server/0.1"
    );
    let completed = client
        .run_prompt("hello over websocket", std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(completed.assistant_text.contains("inspected"));

    client.close().await.unwrap();
    server_task.abort();
    let _ = server_task.await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_websocket_query_methods_are_callable() {
    let root = unique_test_root("app-server-websocket-query");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let agent_registry_root = root.join("agents");
    publish_agent_manifest(
        &root,
        &agent_registry_root,
        "wire-runner",
        "Wire Runner",
        "Exposes wire query methods",
        &[],
    );
    let operation_registry_root = root.join("operations");
    publish_echo_operation(&operation_registry_root, "lookup", "lookup", "wire").await;
    let addr = unused_loopback_addr();
    let listen = AppServerListenAddr::parse(&format!("ws://{addr}/rpc")).unwrap();
    let mut config = CooldisAppServerConfig::local(listen.clone(), &workspace)
        // lexicon-allow: capsule - existing app-server config method.
        .with_capsule_bindings(
            // lexicon-allow: capsule - existing app-server config type.
            CapsuleBindingsConfig::default().with_registry_root(&operation_registry_root),
        );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root;
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let token = mint_app_server_test_token(&app).await;
    let server = app.clone();
    let server_task = tokio::spawn(async move { server.serve(listen).await });
    let mut client = connect_ws_tui_test_client(&format!("ws://{addr}/rpc"), &token).await;

    let agents = client.request("agent/list", json!({})).await.unwrap();
    assert!(
        agents["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|agent| agent["name"].as_str() == Some("wire-runner"))
    );
    assert_eq!(
        client
            .request("agent/read", json!({ "ref": "agent://wire-runner@latest" }))
            .await
            .unwrap()["aliasResolutionReceipt"]["alias"]
            .as_str(),
        Some("latest")
    );
    let operations = client.request("operation/list", json!({})).await.unwrap();
    assert_eq!(
        operation_record_by_name(operations["data"].as_array().unwrap(), "lookup")["name"].as_str(),
        Some("lookup")
    );
    assert!(
        client.model_list().await.unwrap()["data"]
            .as_array()
            .is_some_and(|models| !models.is_empty())
    );

    let thread = client
        .thread_start(json!({ "agentRef": "agent://wire-runner@latest" }))
        .await
        .unwrap();
    let turn = client
        .turn_start_text(&thread.id, "wire query receipt")
        .await
        .unwrap();
    client
        .wait_for_turn_completed(&thread.id, &turn.id, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let events = client
        .request(
            "thread/events/list",
            json!({
                "threadId": thread.id,
                "kinds": ["context.compile.completed"],
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        events["data"][0]["kind"].as_str(),
        Some("context.compile.completed")
    );

    client.close().await.unwrap();
    server_task.abort();
    let _ = server_task.await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_websocket_listen_serves_health_endpoints() {
    let root = unique_test_root("app-server-websocket-health");
    let addr = unused_loopback_addr();
    let listen = AppServerListenAddr::parse(&format!("ws://{addr}/rpc")).unwrap();
    // lexicon-allow: capsule - existing test client name
    let first_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = first_client;
    let app = test_app_with_provider_root_and_listen(
        &root,
        &root,
        listen.clone(),
        provider_client,
        // lexicon-allow: capsule - existing app-server config type
        CapsuleBindingsConfig::default(),
    )
    .await;
    let server_task = tokio::spawn(async move { app.serve(listen).await });

    for path in ["/healthz", "/readyz"] {
        let response = get_tcp_health_response(addr, path).await;
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "unexpected {path} response: {response:?}"
        );
        assert!(
            response.contains(APP_SERVER_HEALTH_RESPONSE_BODY),
            "unexpected {path} response body: {response:?}"
        );
    }

    server_task.abort();
    let _ = server_task.await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_websocket_listen_serves_console_assets() {
    let root = unique_test_root("app-server-console-assets");
    let assets = root.join("console");
    std::fs::create_dir_all(assets.join("assets")).unwrap();
    std::fs::write(
        assets.join("index.html"),
        "<!doctype html><html><head><title>Console</title></head><body><div id=\"app\"></div></body></html>",
    )
    .unwrap();
    std::fs::write(
        assets.join("assets").join("app.js"),
        "console.log('asset');",
    )
    .unwrap();
    std::fs::write(assets.join("favicon.png"), "png").unwrap();

    let addr = unused_loopback_addr();
    let listen = AppServerListenAddr::parse(&format!("ws://{addr}/rpc")).unwrap();
    let mut config =
        CooldisAppServerConfig::local(listen.clone(), std::env::current_dir().unwrap())
            .with_console_assets(&assets, "fixture-token");
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let server_task = tokio::spawn(async move { app.serve(listen).await });

    let response = get_tcp_response(addr, "/").await;
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected console root response: {response:?}"
    );
    assert!(response.contains("Content-Type: text/html"));
    assert!(response.contains("__COOLDIS_CONSOLE_CONFIG__"));
    assert!(!response.contains("fixture-token"));
    assert!(response.contains("cooldis_id_"));

    let response = get_tcp_response(addr, "/index.html").await;
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected console index response: {response:?}"
    );
    assert!(response.contains("__COOLDIS_CONSOLE_CONFIG__"));

    let response = get_tcp_response(addr, "/assets/app.js").await;
    assert!(
        response.starts_with("HTTP/1.1 200 OK"),
        "unexpected console asset response: {response:?}"
    );
    assert!(response.contains("Content-Type: text/javascript"));
    assert!(response.contains("console.log('asset');"));

    let response = get_tcp_response(addr, "/healthz").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains(APP_SERVER_HEALTH_RESPONSE_BODY));

    server_task.abort();
    let _ = server_task.await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn app_server_websocket_listen_requires_console_session_token() {
    let root = unique_test_root("app-server-console-token");
    let assets = root.join("console");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("index.html"),
        "<html><head></head><body></body></html>",
    )
    .unwrap();

    let addr = unused_loopback_addr();
    let listen = AppServerListenAddr::parse(&format!("ws://{addr}/rpc")).unwrap();
    let mut config =
        CooldisAppServerConfig::local(listen.clone(), std::env::current_dir().unwrap())
            .with_console_assets(&assets, "fixture-token");
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let app = CooldisAppServer::new_local(config).await.unwrap();
    let server_task = tokio::spawn(async move { app.serve(listen).await });
    let index = get_tcp_response(addr, "/").await;
    let session_token = console_token_from_response(&index);

    let missing = get_tcp_raw_response(
        addr,
        &format!(
            "GET /rpc HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        ),
    )
    .await;
    assert!(
        missing.starts_with("HTTP/1.1 401 Unauthorized"),
        "unexpected missing-token response: {missing:?}"
    );

    let wrong = get_tcp_raw_response(
        addr,
        &format!(
            "GET /rpc?token={session_token} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        ),
    )
    .await;
    assert!(
        wrong.starts_with("HTTP/1.1 401 Unauthorized"),
        "unexpected wrong-token response: {wrong:?}"
    );

    let mut client = connect_ws_tui_test_client(&format!("ws://{addr}/rpc"), &session_token).await;
    assert_eq!(
        client.initialize_result()["userAgent"],
        "cooldis-app-server/0.1"
    );

    client.close().await.unwrap();
    server_task.abort();
    let _ = server_task.await;
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn boundary_bearer_parser_accepts_case_and_whitespace_and_skips_unrelated_protocols() {
    let authorization = parse_http_request_head(
        b"GET /rpc HTTP/1.1\r\naUtHoRiZaTiOn:   bEaReR\t boundary-token   \r\n\r\n",
    )
    .unwrap();
    assert_eq!(
        request_bearer_token(&authorization),
        Some(("boundary-token", BoundarySurface::Websocket))
    );

    let protocols = parse_http_request_head(
        b"GET /rpc HTTP/1.1\r\nSec-WebSocket-Protocol: unrelated.v1\r\nsEc-WeBsOcKeT-pRoToCoL: metrics.v1, cooldis-console-token.console-secret\r\n\r\n",
    )
    .unwrap();
    assert_eq!(
        request_bearer_token(&protocols),
        Some(("console-secret", BoundarySurface::Console))
    );
}

#[test]
fn session_close_witness_failure_does_not_mask_the_read_error() {
    let read_error = CooldisError::RuntimeFactory("original websocket read error".to_string());
    let close_error = CooldisError::History("close witness failed".to_string());
    let error = finish_websocket_session(Err(read_error), Err(close_error)).unwrap_err();
    assert!(error.to_string().contains("original websocket read error"));
    assert!(!error.to_string().contains("close witness failed"));
}

#[cfg(unix)]
#[tokio::test]
async fn unix_peer_mapping_rejects_a_uid_other_than_the_daemon_euid() {
    let app = test_app().await;
    let store_path = app.session_store_path().to_path_buf();
    let (mut client, mut server) = tokio::net::UnixStream::pair().unwrap();
    let request = b"GET /rpc HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    client.write_all(request).await.unwrap();

    let mismatched_uid = current_effective_uid().wrapping_add(1);
    let resolved = app
        .authenticate_unix_websocket(&mut server, mismatched_uid)
        .await
        .unwrap();
    assert!(resolved.is_none());
    drop(server);
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 401 Unauthorized"));
    assert_eq!(
        identity_sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'unix_socket'",
        )
        .await,
        1
    );
}

#[tokio::test]
async fn aborted_websocket_session_still_witnesses_its_close() {
    let app = test_app().await;
    let store_path = app.session_store_path().to_path_buf();
    let resolved_principal = app
        .inner
        .identity_authority
        .resolve_peer_uid(current_effective_uid())
        .await
        .unwrap()
        .unwrap();
    let (_client_io, server_io) = tokio::io::duplex(1024);
    let websocket = tokio_tungstenite::WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        Some(websocket_config()),
    )
    .await;
    let server = app.clone();
    let task = tokio::spawn(async move {
        server
            .handle_websocket(websocket, resolved_principal, BoundarySurface::UnixSocket)
            .await
    });

    wait_for_identity_sql_count(
        &store_path,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE closed_at_ms IS NULL",
        1,
    )
    .await;
    task.abort();
    let _ = task.await;
    wait_for_identity_sql_count(
        &store_path,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE closed_at_ms IS NOT NULL",
        1,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn pre_upgrade_reads_and_upgrade_are_bounded_when_no_data_arrives() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connect = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (mut server, _) = listener.accept().await.unwrap();
    let _client = connect.await.unwrap();

    assert!(peek_http_request(&server).await.unwrap().is_none());
    consume_http_request_headers(&mut server).await.unwrap();

    #[cfg(unix)]
    {
        let (_client, mut server) = tokio::net::UnixStream::pair().unwrap();
        assert!(peek_unix_http_request(&server).await.unwrap().is_none());
        consume_http_request_headers(&mut server).await.unwrap();
    }

    let (_client_io, server_io) = tokio::io::duplex(1024);
    let error = accept_authenticated_websocket(server_io).await.unwrap_err();
    assert!(error.to_string().contains("timed out"));
}

#[tokio::test]
async fn oversized_pre_upgrade_headers_fail_closed_with_one_witness() {
    let app = test_app().await;
    let store_path = app.session_store_path().to_path_buf();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let connect = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (mut server, _) = listener.accept().await.unwrap();
    let mut client = connect.await.unwrap();
    let request = format!(
        "GET /rpc HTTP/1.1\r\nHost: {addr}\r\nX-Oversized: {}\r\n\r\n",
        "x".repeat(MAX_HTTP_REQUEST_HEADER_BYTES)
    );
    client.write_all(request.as_bytes()).await.unwrap();

    assert!(
        app.authenticate_tcp_websocket(&mut server)
            .await
            .unwrap()
            .is_none()
    );
    let mut response = vec![0_u8; 256];
    let len = tokio::time::timeout(Duration::from_secs(2), client.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response[..len].starts_with(b"HTTP/1.1 401 Unauthorized"));
    assert_eq!(
        identity_sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'websocket'",
        )
        .await,
        1
    );
    response.fill(0);
}

async fn identity_sql_count(path: &Path, query: &str) -> i64 {
    let store = SqliteSessionStore::open(path).await.unwrap();
    let connection = store.sqlite_database().connect().await.unwrap();
    let mut rows = connection.query(query, ()).await.unwrap();
    rows.next().await.unwrap().unwrap().get(0).unwrap()
}

async fn wait_for_identity_sql_count(path: &Path, query: &str, expected: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if identity_sql_count(path, query).await >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn app_server_listen_addr_parses_websocket_urls() {
    let addr: std::net::SocketAddr = "127.0.0.1:8765".parse().unwrap();
    assert_eq!(
        AppServerListenAddr::parse("ws://127.0.0.1:8765/rpc").unwrap(),
        AppServerListenAddr::WebSocket(addr)
    );
    assert_eq!(
        AppServerListenAddr::parse("ws://127.0.0.1:8765")
            .unwrap()
            .display(),
        "ws://127.0.0.1:8765/rpc"
    );
    assert!(
        AppServerListenAddr::parse("ws://127.0.0.1:8765/not-rpc")
            .unwrap_err()
            .to_string()
            .contains("expected /rpc")
    );
    assert!(
        AppServerListenAddr::parse("tcp://127.0.0.1:8765")
            .unwrap_err()
            .to_string()
            .contains("unix://PATH or ws://HOST:PORT")
    );
}

#[tokio::test]
async fn app_server_websocket_listen_rejects_non_loopback_without_auth() {
    let root = unique_test_root("app-server-websocket-non-loopback");
    let listen = AppServerListenAddr::parse("ws://0.0.0.0:0/rpc").unwrap();
    // lexicon-allow: capsule - existing test client name
    let first_client = Arc::new(InspectingCapsuleClient::default());
    let provider_client: Arc<dyn ProviderClient> = first_client;
    let app = test_app_with_provider_root_and_listen(
        &root,
        &root,
        listen.clone(),
        provider_client,
        // lexicon-allow: capsule - existing app-server config type
        CapsuleBindingsConfig::default(),
    )
    .await;

    let err = app.serve(listen).await.unwrap_err();

    assert!(
        err.to_string().contains("not loopback"),
        "unexpected error: {err}"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fs_methods_cover_basic_host_file_operations() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let root = std::env::temp_dir().join(format!("cooldis-vfs-test-{}", Uuid::now_v7()));
    let nested = root.join("nested");
    let file = nested.join("hello.txt");
    let copied = nested.join("copy.txt");

    let mkdir = app
        .dispatch_request(
            &connection,
            "fs/createDirectory",
            Some(json!({ "path": cwd_string(&nested), "recursive": true })),
        )
        .await
        .unwrap();
    assert_eq!(mkdir, json!({}));

    let write = app
        .dispatch_request(
            &connection,
            "fs/writeFile",
            Some(json!({
                "path": cwd_string(&file),
                "dataBase64": "aGVsbG8=",
            })),
        )
        .await
        .unwrap();
    assert_eq!(write, json!({}));

    let read = app
        .dispatch_request(
            &connection,
            "fs/readFile",
            Some(json!({ "path": cwd_string(&file) })),
        )
        .await
        .unwrap();
    assert_eq!(read["dataBase64"].as_str(), Some("aGVsbG8="));

    let metadata = app
        .dispatch_request(
            &connection,
            "fs/getMetadata",
            Some(json!({ "path": cwd_string(&file) })),
        )
        .await
        .unwrap();
    assert_eq!(metadata["isFile"].as_bool(), Some(true));
    assert_eq!(metadata["isDirectory"].as_bool(), Some(false));

    let entries = app
        .dispatch_request(
            &connection,
            "fs/readDirectory",
            Some(json!({ "path": cwd_string(&nested) })),
        )
        .await
        .unwrap();
    assert_eq!(
        entries["entries"][0]["fileName"].as_str(),
        Some("hello.txt")
    );

    let watch = app
        .dispatch_request(
            &connection,
            "fs/watch",
            Some(json!({ "watchId": "watch-1", "path": cwd_string(&nested) })),
        )
        .await
        .unwrap();
    let canonical_nested = std::fs::canonicalize(&nested).unwrap();
    assert_eq!(
        watch["path"].as_str(),
        Some(cwd_string(&canonical_nested).as_str())
    );

    let copy = app
        .dispatch_request(
            &connection,
            "fs/copy",
            Some(json!({
                "sourcePath": cwd_string(&file),
                "destinationPath": cwd_string(&copied),
            })),
        )
        .await
        .unwrap();
    assert_eq!(copy, json!({}));
    assert_eq!(std::fs::read_to_string(&copied).unwrap(), "hello");

    let unwatch = app
        .dispatch_request(
            &connection,
            "fs/unwatch",
            Some(json!({ "watchId": "watch-1" })),
        )
        .await
        .unwrap();
    assert_eq!(unwatch, json!({}));

    let remove = app
        .dispatch_request(
            &connection,
            "fs/remove",
            Some(json!({ "path": cwd_string(&root), "recursive": true })),
        )
        .await
        .unwrap();
    assert_eq!(remove, json!({}));
    assert!(!root.exists());
}

#[tokio::test]
async fn command_exec_returns_buffered_output() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let root = std::env::temp_dir().join(format!("cooldis-command-test-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();

    let response = app
            .dispatch_request(
                &connection,
                "command/exec",
                Some(json!({
                    "command": ["/bin/sh", "-c", "printf \"$COOLDIS_TEST:$PWD\"; printf err >&2; exit 7"],
                    "cwd": cwd_string(&root),
                    "env": { "COOLDIS_TEST": "ok" },
                    "disableTimeout": true,
                })),
            )
            .await
            .unwrap();
    assert_eq!(response["exitCode"].as_i64(), Some(7));
    let canonical_root = std::fs::canonicalize(&root).unwrap();
    assert_eq!(
        response["stdout"].as_str(),
        Some(format!("ok:{}", cwd_string(&canonical_root)).as_str())
    );
    assert_eq!(response["stderr"].as_str(), Some("err"));

    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn command_exec_streaming_session_can_poll_write_and_terminate() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();

    let started = app
        .dispatch_request(
            &connection,
            "command/exec",
            Some(json!({
                "command": ["/bin/sh", "-c", "cat"],
                "streamStdin": true,
                "streamStdoutStderr": true,
                "yieldTimeMs": 5,
                "timeoutMs": 2000,
                "threadId": thread_id,
            })),
        )
        .await
        .unwrap();
    assert_eq!(started["status"].as_str(), Some("running"));
    let process_id = started["processId"].as_str().unwrap().to_string();

    let written = app
        .dispatch_request(
            &connection,
            "command/exec/write",
            Some(json!({
                "processId": process_id,
                "deltaBase64": STANDARD.encode("hello\n"),
                "yieldTimeMs": 100,
            })),
        )
        .await
        .unwrap();
    assert_eq!(written["status"].as_str(), Some("running"));
    assert!(written["stdout"].as_str().unwrap().contains("hello"));

    let terminated = app
        .dispatch_request(
            &connection,
            "command/exec/terminate",
            Some(json!({
                "processId": process_id,
                "reason": "test complete",
                "yieldTimeMs": 1000,
            })),
        )
        .await
        .unwrap();
    assert_eq!(terminated["status"].as_str(), Some("cancelled"));
    assert_eq!(terminated["exitCode"].as_i64(), Some(130));

    let coordinates = app.coordinates_for_thread(&thread_id).await.unwrap();
    let store = app
        .inner
        .supervisor
        .runtime_store(&app.inner.tenant_id)
        .await
        .unwrap();
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let context = store.build_context(&coordinates).await.unwrap();
            let text = context
                .messages
                .iter()
                .filter_map(|message| match message {
                    CanonicalMessage::User { content, .. } => Some(
                        content
                            .iter()
                            .filter_map(|content| match content {
                                CanonicalContent::Text { text, .. } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join(""),
                    ),
                    _ => None,
                })
                .find(|text| text.contains(cooldis_runtime_contracts::HANDLE_OUTCOME_CONTENT_KIND));
            if let Some(text) = text {
                break text;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cancelled process outcome should reach its consumer");
    assert!(delivered.contains("cancelled"), "{delivered}");
    assert!(delivered.contains("test complete"), "{delivered}");

    let resize_err = app
        .dispatch_request(
            &connection,
            "command/exec/resize",
            Some(json!({ "processId": process_id })),
        )
        .await
        .unwrap_err();
    assert_eq!(resize_err.code, -32602);
    assert!(resize_err.message.contains("resize"));
}

#[tokio::test]
async fn command_exec_streaming_start_returns_running_process_id_then_poll_completes() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();

    let started = app
        .dispatch_request(
            &connection,
            "command/exec",
            Some(json!({
                "command": ["/bin/sh", "-c", "sleep 0.05; printf done"],
                "streamStdoutStderr": true,
                "yieldTimeMs": 5,
                "timeoutMs": 2000,
                "threadId": thread_id,
            })),
        )
        .await
        .unwrap();
    assert_eq!(started["status"].as_str(), Some("running"));
    let process_id = started["processId"].as_str().unwrap().to_string();

    let completed = app
        .dispatch_request(
            &connection,
            "command/exec",
            Some(json!({
                "processId": process_id,
                "yieldTimeMs": 1000,
            })),
        )
        .await
        .unwrap();
    assert_eq!(completed["status"].as_str(), Some("completed"));
    assert_eq!(completed["processId"].as_str(), Some(process_id.as_str()));
    assert_eq!(completed["stdout"].as_str(), Some("done"));
    assert_eq!(completed["exitCode"].as_i64(), Some(0));
}

#[tokio::test]
async fn process_dispatch_retry_and_duplicate_terminal_deliver_once() {
    use cooldis_io_core::{
        ConversationKind, IngressContent, IngressEnvelope, IoConversation, IoDedupeKey, IoDelivery,
        IoPrincipal, IoSource,
    };
    use cooldis_runtime_contracts::{
        DispatchId, HANDLE_DISPATCH_CONTENT_KIND, HANDLE_OUTCOME_CONTENT_KIND, HandleId,
        HandleTerminalEnvelope, HandleTerminalOutcome,
    };

    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let marker = std::env::temp_dir().join(format!("cooldis-process-dispatch-{}", Uuid::now_v7()));
    let dispatch_id = format!("process-dispatch-{}", Uuid::now_v7());
    let params = json!({
        "command": [
            "/bin/sh",
            "-c",
            format!("sleep 0.15; printf x >> '{}'; printf done", marker.display()),
        ],
        "streamStdoutStderr": true,
        "yieldTimeMs": 1,
        "timeoutMs": 2000,
        "threadId": thread_id,
        "dispatchId": dispatch_id,
    });

    let first = app
        .dispatch_request(&connection, "command/exec", Some(params.clone()))
        .await
        .unwrap();
    let second = app
        .dispatch_request(&connection, "command/exec", Some(params))
        .await
        .unwrap();
    assert_eq!(first["processId"], second["processId"]);
    assert_eq!(first["dispatchId"], second["dispatchId"]);
    let process_id = first["processId"].as_str().unwrap().to_string();

    let coordinates = app.coordinates_for_thread(&thread_id).await.unwrap();
    let store = app
        .inner
        .supervisor
        .runtime_store(&app.inner.tenant_id)
        .await
        .unwrap();
    let (events, thread_events) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let events = store
                .read_events(&crate::control_stream_id(&coordinates), None)
                .await
                .unwrap();
            let outcomes = events
                .iter()
                .filter(|event| {
                    event.kind == EventKind::IoIngressReceived
                        && event.payload.get("route_id").and_then(Value::as_str)
                            == Some(HANDLE_OUTCOME_CONTENT_KIND)
                })
                .count();
            let thread_events = store
                .read_events(&crate::EventStreamId::for_thread(&coordinates), None)
                .await
                .unwrap();
            let turns = thread_events
                .iter()
                .filter(|event| event.kind == EventKind::TurnSubmitted)
                .count();
            if outcomes == 1 && turns == 1 {
                break (events, thread_events);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("process outcome ingress should settle");
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "x");
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == EventKind::IoIngressReceived
                    && event.payload.get("route_id").and_then(Value::as_str)
                        == Some(HANDLE_DISPATCH_CONTENT_KIND)
            })
            .count(),
        1
    );
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == EventKind::TurnSubmitted)
            .count(),
        1
    );

    let terminal = HandleTerminalEnvelope {
        dispatch_id: DispatchId::new(dispatch_id.clone()),
        handle: HandleId::process(process_id),
        outcome: HandleTerminalOutcome::Completed,
        outcome_reason: Some("exit status 0".to_string()),
        result: None,
        result_schema_id: None,
        artifact_refs: Vec::new(),
        usage: None,
        retryable: false,
    };
    let mut duplicate = IngressEnvelope::new(
        IoSource::new("cooldis.handle", "process"),
        IoConversation::new(format!("thread:{thread_id}"), ConversationKind::System),
        IngressContent::Event {
            kind: HANDLE_OUTCOME_CONTENT_KIND.to_string(),
            payload: serde_json::to_value(terminal).unwrap(),
        },
        1,
    )
    .with_dedupe_key(IoDedupeKey::new(
        HANDLE_OUTCOME_CONTENT_KIND,
        dispatch_id.clone(),
    ))
    .with_delivery(IoDelivery::new(dispatch_id.clone()))
    .with_principal(IoPrincipal::new(
        app.tenant_id(),
        app.user_id(),
        format!("handle:{dispatch_id}"),
    ))
    .with_metadata("cooldis_route_id", HANDLE_OUTCOME_CONTENT_KIND)
    .with_metadata("cooldis_route_policy", "queue_per_conversation");
    duplicate.id = events
        .iter()
        .find(|event| {
            event.kind == EventKind::IoIngressReceived
                && event.payload.get("route_id").and_then(Value::as_str)
                    == Some(HANDLE_OUTCOME_CONTENT_KIND)
        })
        .and_then(|event| event.payload.get("ingress_message_id"))
        .and_then(Value::as_str)
        .unwrap()
        .to_string();
    let bridge = CooldisDaemonIoBridge::from_app_server(&app);
    bridge
        .submit_durable_handle_envelope(duplicate.clone())
        .await
        .unwrap();
    bridge
        .submit_durable_handle_envelope(duplicate)
        .await
        .unwrap();

    let events = store
        .read_events(&crate::control_stream_id(&coordinates), None)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == EventKind::IoIngressReceived
                    && event.payload.get("route_id").and_then(Value::as_str)
                        == Some(HANDLE_OUTCOME_CONTENT_KIND)
            })
            .count(),
        1
    );
    assert_eq!(
        store
            .read_events(&crate::EventStreamId::for_thread(&coordinates), None)
            .await
            .unwrap()
            .iter()
            .filter(|event| event.kind == EventKind::TurnSubmitted)
            .count(),
        1
    );
    std::fs::remove_file(marker).unwrap();
}

#[tokio::test]
async fn local_ui_affordance_methods_return_safe_shapes() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    assert_eq!(
        app.dispatch_request(&connection, "app/list", None)
            .await
            .unwrap(),
        json!({ "data": [], "nextCursor": null })
    );
    assert_eq!(
        app.dispatch_request(&connection, "experimentalFeature/list", None)
            .await
            .unwrap(),
        json!({ "data": [], "nextCursor": null })
    );
    assert_eq!(
        app.dispatch_request(&connection, "hooks/list", None)
            .await
            .unwrap(),
        json!({ "data": [], "witnessing": true })
    );
    assert_eq!(
        app.dispatch_request(
            &connection,
            "experimentalFeature/enablement/set",
            Some(json!({ "enablement": { "example": true } })),
        )
        .await
        .unwrap(),
        json!({ "enablement": { "example": true } })
    );
    assert_eq!(
        app.dispatch_request(
            &connection,
            "getAuthStatus",
            Some(json!({ "includeToken": true, "refreshToken": false })),
        )
        .await
        .unwrap(),
        json!({
            "authMethod": null,
            "authToken": null,
            "requiresOpenaiAuth": false,
        })
    );

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap();
    let summary = app
        .dispatch_request(
            &connection,
            "getConversationSummary",
            Some(json!({ "conversationId": thread_id })),
        )
        .await
        .unwrap();
    assert_eq!(
        summary["summary"]["conversationId"].as_str(),
        Some(thread_id)
    );
    assert_eq!(summary["summary"]["source"].as_str(), Some("unknown"));
}

#[tokio::test]
async fn thread_shell_command_emits_command_execution_item() {
    let app = test_app().await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let response = app
        .dispatch_request(
            &connection,
            "thread/shellCommand",
            Some(json!({
                "threadId": thread_id,
                "command": "printf shell-output",
            })),
        )
        .await
        .unwrap();
    assert_eq!(response, json!({}));

    let mut saw_delta = false;
    let mut saw_completed = false;
    let mut observed = Vec::new();
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(2));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            message = outbound_rx.recv() => {
                let Some(JsonRpcMessage::Notification(notification)) = message else {
                    continue;
                };
                observed.push((notification.method.clone(), notification.params.clone()));
                if notification.method == "item/commandExecution/outputDelta"
                    && notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("delta"))
                        .and_then(Value::as_str)
                        .is_some_and(|delta| delta.contains("shell-output"))
                {
                    saw_delta = true;
                }
                if notification.method == "item/completed"
                    && notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("item"))
                        .and_then(|item| item.get("type"))
                        .and_then(Value::as_str)
                        == Some("commandExecution")
                {
                    saw_completed = true;
                }
                if saw_delta && saw_completed {
                    break;
                }
            }
        }
    }
    assert!(saw_delta, "observed notifications: {observed:?}");
    assert!(saw_completed, "observed notifications: {observed:?}");
}

#[tokio::test]
async fn bridge_flow_uses_local_offline_provider() {
    let app = test_app().await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();

    let turn_start = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "hello", "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    let turn_id = turn_start["turn"]["id"].as_str().unwrap().to_string();

    let mut saw_delta = false;
    let mut saw_completed = false;
    let mut methods = Vec::new();
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(2));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            message = outbound_rx.recv() => {
                let Some(message) = message else {
                    break;
                };
                if let JsonRpcMessage::Notification(notification) = message {
                    methods.push(notification.method.clone());
                    if notification.method == "item/agentMessage/delta" {
                        saw_delta = true;
                    }
                    if notification.method == "turn/completed"
                        && notification
                            .params
                            .as_ref()
                            .and_then(|params| params.get("turn"))
                            .and_then(|turn| turn.get("id"))
                            .and_then(Value::as_str)
                            == Some(turn_id.as_str())
                    {
                        saw_completed = true;
                        break;
                    }
                }
            }
        }
    }
    let snapshot = app.inner.supervisor.snapshot().await;
    let session_messages = latest_assistant_text(&app, &thread_id).await;
    assert!(
        saw_delta,
        "notifications: {methods:?}; latest assistant: {session_messages:?}; snapshot: {:?}",
        snapshot
    );
    assert!(
        saw_completed,
        "notifications: {methods:?}; latest assistant: {session_messages:?}; snapshot: {:?}",
        snapshot
    );
}

#[tokio::test]
async fn thinking_precedence_flows_to_provider_requests() {
    let root = unique_test_root("app-server-thinking-precedence");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let client = Arc::new(ThinkingRecorderClient::new());
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-app-server-test-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, &workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let mut runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    runtime_config.thinking = Some(ThinkingConfig::Budget { budget_tokens: 99 });
    let runtime_factory =
        runtime_factory_from_provider_parts(runtime_config, provider_client, Default::default());
    let app = CooldisAppServer::with_runtime_factory(config, runtime_factory)
        .await
        .unwrap();
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let default_thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let default_thread_id = default_thread["thread"]["id"].as_str().unwrap().to_string();
    let default_turn_id = start_text_turn(&app, &connection, &default_thread_id, "default").await;
    wait_for_provider_requests(&client, 1).await;
    wait_for_turn_completed_notification(&mut outbound_rx, &default_thread_id, &default_turn_id)
        .await;
    assert_eq!(
        client.requests()[0].thinking,
        Some(ThinkingConfig::Budget { budget_tokens: 99 })
    );

    let thread_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "thinking": { "type": "effort", "effort": "high" },
            })),
        )
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    let inherited_turn_id = start_text_turn(&app, &connection, &thread_id, "thread-level").await;
    wait_for_provider_requests(&client, 2).await;
    wait_for_turn_completed_notification(&mut outbound_rx, &thread_id, &inherited_turn_id).await;
    assert_eq!(
        client.requests()[1].thinking,
        Some(ThinkingConfig::Effort {
            effort: ThinkingEffort::High
        })
    );

    let override_turn = app
        .dispatch_request(
            &connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "override", "text_elements": [] }],
                "thinking": { "type": "disabled" },
            })),
        )
        .await
        .unwrap();
    let override_turn_id = override_turn["turn"]["id"].as_str().unwrap().to_string();
    wait_for_provider_requests(&client, 3).await;
    wait_for_turn_completed_notification(&mut outbound_rx, &thread_id, &override_turn_id).await;
    assert_eq!(
        client.requests()[2].thinking,
        Some(ThinkingConfig::Disabled)
    );

    let next_turn_id = start_text_turn(&app, &connection, &thread_id, "thread-level-again").await;
    wait_for_provider_requests(&client, 4).await;
    wait_for_turn_completed_notification(&mut outbound_rx, &thread_id, &next_turn_id).await;
    assert_eq!(
        client.requests()[3].thinking,
        Some(ThinkingConfig::Effort {
            effort: ThinkingEffort::High
        })
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn thinking_stream_projects_as_distinct_items() {
    let root = unique_test_root("app-server-thinking-stream");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let client = Arc::new(ThinkingRecorderClient::with_stream(vec![
        crate::ProviderStreamEvent::ThinkingDelta {
            text: "plan ".to_string(),
        },
        crate::ProviderStreamEvent::TextDelta {
            text: "answer".to_string(),
        },
        crate::ProviderStreamEvent::ThinkingDelta {
            text: "check".to_string(),
        },
        crate::ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::EndTurn,
        },
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let app = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        Default::default(),
        true,
    )
    .await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn_id = start_text_turn(&app, &connection, &thread_id, "split stream").await;

    let (message_delta, thinking_delta, completed) =
        collect_message_and_thinking_deltas_until_turn_completed(
            &mut outbound_rx,
            &thread_id,
            &turn_id,
        )
        .await;
    assert_eq!(message_delta, "answer");
    assert_eq!(thinking_delta, "plan check");

    let items = completed["turn"]["items"].as_array().unwrap();
    assert_eq!(
        item_text_by_type(items, "agentMessage").as_deref(),
        Some("answer")
    );
    assert_eq!(
        item_text_by_type(items, "agentThinking").as_deref(),
        Some("plan check")
    );

    let read = app
        .dispatch_request(
            &connection,
            "thread/read",
            Some(json!({ "threadId": thread_id, "includeTurns": true })),
        )
        .await
        .unwrap();
    let read_items = read["thread"]["turns"][0]["items"].as_array().unwrap();
    assert_eq!(
        item_text_by_type(read_items, "agentMessage").as_deref(),
        Some("answer")
    );
    assert_eq!(
        item_text_by_type(read_items, "agentThinking").as_deref(),
        Some("plan check")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn non_stream_thinking_delta_precedes_text_when_content_does() {
    let root = unique_test_root("app-server-thinking-non-stream-order");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let client = Arc::new(ThinkingRecorderClient::with_complete_content(vec![
        CanonicalContent::Thinking {
            text: "plan".to_string(),
            provider: crate::ThinkingProvider::Other("unit".to_string()),
            metadata: crate::ThinkingMetadata::None,
        },
        CanonicalContent::text("answer"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client;
    let app = test_app_with_provider_root_and_stream(
        &root,
        &workspace,
        provider_client,
        Default::default(),
        false,
    )
    .await;
    let (connection, mut outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread = app
        .dispatch_request(&connection, "thread/start", Some(json!({})))
        .await
        .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let turn_id = start_text_turn(&app, &connection, &thread_id, "ordered non-stream").await;

    let delta_methods =
        collect_delta_methods_until_turn_completed(&mut outbound_rx, &thread_id, &turn_id).await;
    assert_eq!(
        delta_methods,
        vec![
            "item/agentThinking/delta".to_string(),
            "item/agentMessage/delta".to_string()
        ]
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn local_thread_read_echoes_thread_thinking_config() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;

    let thread_start = app
        .dispatch_request(
            &connection,
            "thread/start",
            Some(json!({
                "thinking": { "type": "effort", "effort": "low" },
            })),
        )
        .await
        .unwrap();
    let thread_id = thread_start["thread"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        thread_start["thread"]["thinking"],
        json!({ "type": "effort", "effort": "low" })
    );

    let read = app
        .dispatch_request(
            &connection,
            "thread/read",
            Some(json!({ "threadId": thread_id, "includeTurns": false })),
        )
        .await
        .unwrap();
    assert_eq!(
        read["thread"]["thinking"],
        json!({ "type": "effort", "effort": "low" })
    );

    let list = app
        .dispatch_request(&connection, "thread/list", None)
        .await
        .unwrap();
    assert_eq!(
        list["data"][0]["thinking"],
        json!({ "type": "effort", "effort": "low" })
    );

    app.dispatch_request(
        &connection,
        "thread/name/set",
        Some(json!({ "threadId": thread_id, "name": "keeps thinking" })),
    )
    .await
    .unwrap();
    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(parsed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        thread_metadata_thinking(&lifecycle.metadata).unwrap(),
        Some(ThinkingConfig::Effort {
            effort: ThinkingEffort::Low
        })
    );

    app.dispatch_request(
        &connection,
        "thread/resume",
        Some(json!({ "threadId": thread_id, "excludeTurns": true })),
    )
    .await
    .unwrap();
    let lifecycle = app
        .inner
        .metadata_store
        .get_thread_lifecycle(parsed)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        thread_metadata_thinking(&lifecycle.metadata).unwrap(),
        Some(ThinkingConfig::Effort {
            effort: ThinkingEffort::Low
        })
    );
}

#[tokio::test]
async fn unsupported_methods_return_method_not_found() {
    let app = test_app().await;
    let (connection, _outbound_rx) = test_connection(app.clone());
    initialize_for_test(&connection).await;
    let err = app
        .dispatch_request(&connection, "not/a-method", None)
        .await
        .unwrap_err();
    assert_eq!(err.code, -32601);
    assert!(err.message.contains("not/a-method"));
}

async fn test_app() -> CooldisAppServer {
    let root = std::env::temp_dir().join(format!("cooldis-app-server-test-{}", Uuid::now_v7()));
    test_app_at_root(&root).await
}

async fn test_app_at_root(root: &Path) -> CooldisAppServer {
    let listen = AppServerListenAddr::Unix(root.join("app-server.sock"));
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    CooldisAppServer::new_local(config).await.unwrap()
}

fn test_connection(
    app: CooldisAppServer,
) -> (ConnectionState, mpsc::UnboundedReceiver<JsonRpcMessage>) {
    let (outbound, rx) = mpsc::unbounded_channel::<JsonRpcMessage>();
    let resolved_principal = ResolvedPrincipal {
        principal_id: PrincipalId::new(app.user_id()),
        kind: PrincipalKind::Operator,
        auth: AuthenticationPath::PeerUid {
            uid: current_effective_uid(),
        },
    };
    (
        ConnectionState {
            app,
            resolved_principal,
            outbound,
            handshake: Arc::new(Mutex::new(HandshakeState::default())),
            opt_out_notifications: Arc::new(RwLock::new(HashSet::new())),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            fs_watches: Arc::new(Mutex::new(HashMap::new())),
        },
        rx,
    )
}

async fn initialize_for_test(connection: &ConnectionState) {
    connection
        .handle_initialize(Some(json!({
            "clientInfo": {
                "name": "test",
                "title": null,
                "version": "0",
            },
            "capabilities": null,
        })))
        .await
        .unwrap();
}

// lexicon-allow: capsule - existing app-server test helper name
async fn test_app_with_provider_and_capsule_bindings(
    provider_client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing app-server config type and parameter
    capsule_bindings: CapsuleBindingsConfig,
) -> CooldisAppServer {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-app-server-test-{}.sock", Uuid::now_v7())),
    );
    let root = std::env::temp_dir().join(format!("cooldis-app-server-test-{}", Uuid::now_v7()));
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    // lexicon-allow: capsule - existing app-server config method and parameter
    let mut config = CooldisAppServerConfig::local(listen, &workspace)
        .with_capsule_bindings(capsule_bindings.clone()); // lexicon-allow: capsule - existing app-server config method and parameter
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let mut runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    runtime_config.max_tokens = 128;
    // lexicon-allow: capsule - existing app-server config parameter
    let runtime_factory =
        runtime_factory_from_provider_parts(runtime_config, provider_client, capsule_bindings); // lexicon-allow: capsule - existing app-server config parameter
    CooldisAppServer::with_runtime_factory(config, runtime_factory)
        .await
        .unwrap()
}

async fn test_app_with_provider_root(
    root: &Path,
    cwd: &Path,
    provider_client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing operation binding config type
    operation_bindings: CapsuleBindingsConfig,
) -> CooldisAppServer {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-app-server-test-{}.sock", Uuid::now_v7())),
    );
    test_app_with_provider_root_and_listen(root, cwd, listen, provider_client, operation_bindings)
        .await
}

async fn test_app_with_provider_root_and_stream(
    root: &Path,
    cwd: &Path,
    provider_client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing operation binding config type
    operation_bindings: CapsuleBindingsConfig,
    stream: bool,
) -> CooldisAppServer {
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-app-server-test-{}.sock", Uuid::now_v7())),
    );
    test_app_with_provider_root_listen_and_stream(
        root,
        cwd,
        listen,
        provider_client,
        operation_bindings,
        stream,
    )
    .await
}

async fn test_app_with_provider_root_and_listen(
    root: &Path,
    cwd: &Path,
    listen: AppServerListenAddr,
    provider_client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing operation binding config type
    operation_bindings: CapsuleBindingsConfig,
) -> CooldisAppServer {
    test_app_with_provider_root_listen_and_stream(
        root,
        cwd,
        listen,
        provider_client,
        operation_bindings,
        false,
    )
    .await
}

async fn test_app_with_provider_root_listen_and_stream(
    root: &Path,
    cwd: &Path,
    listen: AppServerListenAddr,
    provider_client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing operation binding config type
    operation_bindings: CapsuleBindingsConfig,
    stream: bool,
) -> CooldisAppServer {
    // lexicon-allow: capsule - existing app-server test helper
    let mut config = CooldisAppServerConfig::local(listen, cwd)
        .with_capsule_bindings(operation_bindings.clone()); // lexicon-allow: capsule - existing app-server test helper
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    if stream {
        config = config.with_openai_chat_completions(
            "openai",
            "https://example.invalid/v1",
            "test-api-key",
            "gpt-test",
        );
    }
    let mut runtime_config =
        AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    runtime_config.max_tokens = 128;
    runtime_config.stream = stream;
    // lexicon-allow: capsule - existing app-server test helper
    let runtime_factory =
        runtime_factory_from_provider_parts(runtime_config, provider_client, operation_bindings); // lexicon-allow: capsule - existing app-server test helper
    let metadata_store = SqliteMetadataStore::open(config.metadata_store_path())
        .await
        .unwrap();
    CooldisAppServer::with_runtime_factory_and_metadata_store(
        config,
        runtime_factory,
        metadata_store,
    )
    .await
    .unwrap()
}

async fn submit_provider_turn_without_subscription(
    app: &CooldisAppServer,
    thread_id: &str,
    input_values: Vec<Value>,
) -> String {
    let handle = app.handle_for_thread(thread_id).await.unwrap();
    let coordinates = handle.context().coordinates.clone();
    let turn_id = format!("turn-{}", Uuid::now_v7());
    let input = turn_input_from_values(&input_values)
        .with_provider(app.inner.model_provider.clone())
        .with_model(app.inner.model.clone());
    {
        let mut state = app.inner.state.write().await;
        let thread = state.threads.get_mut(thread_id).unwrap();
        let turn = AppServerTurnState::new(turn_id.clone(), input_values.clone());
        if thread.preview.is_empty() {
            thread.preview = user_input_preview(&input_values);
        }
        thread.updated_at_ms = now_ms();
        thread.active_turn_id = Some(turn_id.clone());
        thread.turns.insert(turn_id.clone(), turn);
    }
    app.inner
        .supervisor
        .submit_turn_to_with_mode(
            &coordinates,
            turn_id.clone(),
            input,
            TurnSubmissionMode::Queue,
        )
        .await
        .unwrap();
    turn_id
}

async fn start_text_turn(
    app: &CooldisAppServer,
    connection: &ConnectionState,
    thread_id: &str,
    text: &str,
) -> String {
    let turn = app
        .dispatch_request(
            connection,
            "turn/start",
            Some(json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": text, "text_elements": [] }],
            })),
        )
        .await
        .unwrap();
    turn["turn"]["id"].as_str().unwrap().to_string()
}

fn unique_test_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{name}-{}", Uuid::now_v7()))
}

fn default_agent_version_count(agent_registry_root: &Path) -> usize {
    let version_dir = agent_registry_root.join("versions").join("default");
    if !version_dir.exists() {
        return 0;
    }
    std::fs::read_dir(version_dir)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .ok()
                .and_then(|entry| {
                    entry
                        .path()
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(str::to_string)
                })
                .as_deref()
                == Some("json")
        })
        .count()
}

async fn publish_echo_operation(
    registry_root: &Path,
    record_name: &str,
    operation_name: &str,
    prefix: &str,
) -> crate::PublishedOperationRecord {
    std::fs::create_dir_all(registry_root).unwrap();
    let wasm = wat::parse_str(echo_operation_guest(prefix, operation_name))
        .expect("echo operation fixture should compile");
    let artifact_path = registry_root.join(format!("{record_name}.wasm"));
    std::fs::write(&artifact_path, wasm).unwrap();
    crate::LocalOperationRegistry::new(registry_root)
        .publish_artifact(crate::PublishOperationRequest {
            name: record_name.to_string(),
            artifact_path: artifact_path.clone(),
            source: crate::PublishedOperationSource::Wasm {
                bin_path: artifact_path,
            },
            interface: None,
            capability_grants: Default::default(),
            metadata: Default::default(),
        })
        .await
        .unwrap()
}

async fn publish_multi_echo_operation(
    registry_root: &Path,
    record_name: &str,
    operations: &[(&str, &str)],
) -> crate::PublishedOperationRecord {
    std::fs::create_dir_all(registry_root).unwrap();
    let wasm = wat::parse_str(multi_echo_operation_guest(operations))
        .expect("multi-operation fixture should compile");
    let artifact_path = registry_root.join(format!("{record_name}.wasm"));
    std::fs::write(&artifact_path, wasm).unwrap();
    crate::LocalOperationRegistry::new(registry_root)
        .publish_artifact(crate::PublishOperationRequest {
            name: record_name.to_string(),
            artifact_path: artifact_path.clone(),
            source: crate::PublishedOperationSource::Wasm {
                bin_path: artifact_path,
            },
            interface: None,
            capability_grants: Default::default(),
            metadata: Default::default(),
        })
        .await
        .unwrap()
}

fn publish_agent_manifest(
    root: &Path,
    agent_registry_root: &Path,
    name: &str,
    title: &str,
    summary: &str,
    tool_blocks: &[String],
) -> crate::PublishedAgentRecord {
    let manifest_path = root.join(format!("{name}.cooldis.agent.toml"));
    let tools = if tool_blocks.is_empty() {
        String::new()
    } else {
        format!("\n{}\n", tool_blocks.join("\n"))
    };
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "{name}"
version = "0.1.0"
display_name = "{title}"
description = "{summary}"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"
{tools}
[runtime]
default_cwd = "."
streaming = false
"#
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap()
}

fn write_skill_fixture(package_dir: &Path, name: &str, body: &str) {
    let dir = package_dir.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("SKILL.md"), body).unwrap();
}

async fn app_server_with_tool_client<T>(
    root: &Path,
    workspace: &Path,
    agent_registry_root: &Path,
    client: Arc<T>,
) -> CooldisAppServer
where
    T: ProviderClient + 'static,
{
    let listen = AppServerListenAddr::Unix(
        std::env::temp_dir().join(format!("cooldis-tool-universe-{}.sock", Uuid::now_v7())),
    );
    let mut config = CooldisAppServerConfig::local(listen, workspace);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.to_path_buf();
    let runtime_config = AgentLoopConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let factory = runtime_factory_from_provider_parts_with_store_paths(
        runtime_config,
        client,
        // lexicon-allow: capsule - existing app-server test helper
        CapsuleBindingsConfig::default(),
        None,
        Some(config.metadata_store_path()),
        None,
        Some(config.state_home.join("session_history.sqlite3")),
        None,
        None,
        None,
        None,
        config.default_placement.clone(),
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    );
    CooldisAppServer::with_runtime_factory(config, factory)
        .await
        .unwrap()
}

#[derive(Default)]
struct UniverseCallingClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
    step: std::sync::Mutex<usize>,
}

#[async_trait::async_trait]
impl ProviderClient for UniverseCallingClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let mut step = self.step.lock().unwrap();
        let response = match *step {
            0 => {
                let names = tool_names(request);
                assert!(names.contains(&TOOL_SEARCH_TOOL.to_string()));
                assert!(names.contains(&TOOL_DESCRIBE_TOOL.to_string()));
                assert!(names.contains(&TOOL_CALL_TOOL.to_string()));
                assert!(!names.contains(&"cooldis_mcp_echo".to_string()));
                ProviderResponse {
                    content: vec![CanonicalContent::tool_call(
                        "call_search",
                        TOOL_SEARCH_TOOL,
                        json!({"query": "echo"}),
                    )],
                    usage: CanonicalUsage::default(),
                    stop_reason: CanonicalStopReason::ToolUse,
                }
            }
            1 => {
                let text = text_from_canonical_messages(&request.messages);
                assert!(text.contains("cooldis_mcp_echo"));
                ProviderResponse {
                    content: vec![CanonicalContent::tool_call(
                        "call_describe",
                        TOOL_DESCRIBE_TOOL,
                        json!({"tool": "cooldis_mcp_echo"}),
                    )],
                    usage: CanonicalUsage::default(),
                    stop_reason: CanonicalStopReason::ToolUse,
                }
            }
            2 => {
                let text = text_from_canonical_messages(&request.messages);
                assert!(text.contains("SCHEMA HASH"));
                assert!(text.contains("mcp://arcade"));
                ProviderResponse {
                    content: vec![CanonicalContent::tool_call(
                        "call_universe",
                        TOOL_CALL_TOOL,
                        json!({
                            "tool": "cooldis_mcp_echo",
                            "arguments": {"message": "hello"}
                        }),
                    )],
                    usage: CanonicalUsage::default(),
                    stop_reason: CanonicalStopReason::ToolUse,
                }
            }
            _ => {
                let text = text_from_canonical_messages(&request.messages);
                assert!(text.contains("REMOTE_MCP_OK hello"));
                ProviderResponse {
                    content: vec![CanonicalContent::text("universe completed")],
                    usage: CanonicalUsage::default(),
                    stop_reason: CanonicalStopReason::EndTurn,
                }
            }
        };
        *step += 1;
        Ok(response)
    }
}

#[derive(Default)]
struct PinnedDirectCallingClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
    step: std::sync::Mutex<usize>,
}

#[async_trait::async_trait]
impl ProviderClient for PinnedDirectCallingClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let mut step = self.step.lock().unwrap();
        let response = if *step == 0 {
            let names = tool_names(request);
            assert!(names.contains(&TOOL_SEARCH_TOOL.to_string()));
            assert!(names.contains(&"cooldis_mcp_echo".to_string()));
            ProviderResponse {
                content: vec![CanonicalContent::tool_call(
                    "call_direct",
                    "cooldis_mcp_echo",
                    json!({"message": "hello"}),
                )],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::ToolUse,
            }
        } else {
            let text = text_from_canonical_messages(&request.messages);
            assert!(text.contains("REMOTE_MCP_OK hello"));
            ProviderResponse {
                content: vec![CanonicalContent::text("pinned completed")],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::EndTurn,
            }
        };
        *step += 1;
        Ok(response)
    }
}

async fn spawn_app_mcp_http_fixture(
    message_type: &'static str,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}/mcp");
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let header_end = loop {
                let mut chunk = [0; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    return;
                }
                buffer.extend_from_slice(&chunk[..read]);
                if let Some(index) = find_http_header_end(&buffer) {
                    break index;
                }
            };
            let header_text = String::from_utf8_lossy(&buffer[..header_end]);
            let content_length = header_text
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            let body_start = header_end + 4;
            while buffer.len() - body_start < content_length {
                let mut chunk = [0; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    return;
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            let request: Value =
                serde_json::from_slice(&buffer[body_start..body_start + content_length]).unwrap();
            let body = app_mcp_fixture_response(&request, message_type);
            let raw = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(raw.as_bytes()).await.unwrap();
        }
    });
    (url, task)
}

fn find_http_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn app_mcp_fixture_response(request: &Value, message_type: &str) -> String {
    let id = request.get("id").cloned();
    match request.get("method").and_then(Value::as_str) {
        Some("initialize") => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "app-mcp-fixture", "version": "1"}
            }
        }),
        Some("notifications/initialized") => json!({
            "jsonrpc": "2.0",
            "result": {}
        }),
        Some("tools/list") => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "tools": [{
                    "name": "cooldis_mcp_echo",
                    "description": "Echo a message through app-server MCP.",
                    "inputSchema": app_mcp_echo_schema(message_type)
                }]
            }
        }),
        Some("tools/call") => {
            let message = request
                .pointer("/params/arguments/message")
                .and_then(Value::as_str)
                .unwrap_or("");
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [{"type": "text", "text": format!("REMOTE_MCP_OK {message}")}],
                    "isError": false
                }
            })
        }
        _ => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": "unknown method"}
        }),
    }
    .to_string()
}

fn app_mcp_echo_schema(message_type: &str) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "message": {
                "type": message_type,
                "description": "Message to echo."
            }
        },
        "required": ["message"]
    })
}

async fn wait_for_event_kind(
    app: &CooldisAppServer,
    connection: &ConnectionState,
    thread_id: &str,
    kind: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let page = app
            .dispatch_request(
                connection,
                "thread/events/list",
                Some(json!({
                    "threadId": thread_id,
                    "kinds": [kind],
                })),
            )
            .await
            .unwrap();
        if page["data"]
            .as_array()
            .is_some_and(|events| !events.is_empty())
        {
            return page;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for event kind {kind}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
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

fn multi_echo_operation_guest(operations: &[(&str, &str)]) -> String {
    let manifest_operations = operations
        .iter()
        .enumerate()
        .map(|(index, (operation_name, _prefix))| {
            serde_json::json!({
                "id": index + 1,
                "name": operation_name,
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            })
        })
        .collect::<Vec<_>>();
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": manifest_operations
    })
    .to_string();
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write
                drop
                i32.const 0)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
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

#[derive(Default)]
struct ThinkingRecorderClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
    complete_content: std::sync::Mutex<Option<Vec<CanonicalContent>>>,
    stream_events: std::sync::Mutex<Option<Vec<crate::ProviderStreamEvent>>>,
}

impl ThinkingRecorderClient {
    fn new() -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            complete_content: std::sync::Mutex::new(None),
            stream_events: std::sync::Mutex::new(None),
        }
    }

    fn with_complete_content(complete_content: Vec<CanonicalContent>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            complete_content: std::sync::Mutex::new(Some(complete_content)),
            stream_events: std::sync::Mutex::new(None),
        }
    }

    fn with_stream(stream_events: Vec<crate::ProviderStreamEvent>) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            complete_content: std::sync::Mutex::new(None),
            stream_events: std::sync::Mutex::new(Some(stream_events)),
        }
    }

    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl ProviderClient for ThinkingRecorderClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(ProviderResponse {
            content: self
                .complete_content
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| vec![CanonicalContent::text("thinking recorded")]),
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }

    async fn stream(
        &self,
        request: &ProviderRequest,
    ) -> ProviderResult<Vec<crate::ProviderStreamEvent>> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(self
            .stream_events
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| {
                vec![
                    crate::ProviderStreamEvent::TextDelta {
                        text: "thinking recorded".to_string(),
                    },
                    crate::ProviderStreamEvent::Done {
                        stop_reason: CanonicalStopReason::EndTurn,
                    },
                ]
            }))
    }
}

#[derive(Default)]
// lexicon-allow: capsule - existing test client name
struct InspectingCapsuleClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
}

// lexicon-allow: capsule - existing test client name
impl InspectingCapsuleClient {
    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

// lexicon-allow: capsule - existing test client name
#[async_trait::async_trait]
// lexicon-allow: capsule - existing test client name
impl ProviderClient for InspectingCapsuleClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("inspected")],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

struct ThreadSpawnAgentRefClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
    agent_ref: String,
    cancel_calls: std::sync::Mutex<usize>,
}

impl ThreadSpawnAgentRefClient {
    fn new(agent_ref: &str) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            agent_ref: agent_ref.to_string(),
            cancel_calls: std::sync::Mutex::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ProviderClient for ThreadSpawnAgentRefClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let has_tool_result = request
            .messages
            .iter()
            .any(|message| matches!(message, CanonicalMessage::ToolResult { .. }));
        if has_tool_result {
            if latest_user_text(request).as_deref() == Some("cancel worker")
                && *self.cancel_calls.lock().unwrap() == 0
                && tool_names(request).contains(&THREAD_CANCEL_OPERATION.to_string())
            {
                *self.cancel_calls.lock().unwrap() += 1;
                return Ok(ProviderResponse {
                    content: vec![CanonicalContent::tool_call(
                        "call_thread_cancel_1",
                        THREAD_CANCEL_OPERATION,
                        json!({ "task_name": "worker" }),
                    )],
                    usage: CanonicalUsage::default(),
                    stop_reason: CanonicalStopReason::ToolUse,
                });
            }
            return Ok(ProviderResponse {
                content: vec![CanonicalContent::text("root observed child spawn")],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::EndTurn,
            });
        }
        if tool_names(request).contains(&THREAD_SPAWN_OPERATION.to_string()) {
            return Ok(ProviderResponse {
                content: vec![CanonicalContent::tool_call(
                    "call_thread_spawn_1",
                    THREAD_SPAWN_OPERATION,
                    json!({
                        "task_name": "worker",
                        "message": "hello bound child",
                        "agent_ref": self.agent_ref,
                    }),
                )],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::ToolUse,
            });
        }
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("child agent replied")],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

impl ProviderRequestRecorder for ThreadSpawnAgentRefClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

#[derive(Default)]
struct ScheduleMandateStartClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
}

#[async_trait::async_trait]
impl ProviderClient for ScheduleMandateStartClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let has_tool_result = request
            .messages
            .iter()
            .any(|message| matches!(message, CanonicalMessage::ToolResult { .. }));
        if has_tool_result {
            let text = text_from_canonical_messages(&request.messages);
            assert!(
                text.contains("cooldis.mandate_start"),
                "expected mandate_start tool result in provider context: {text}"
            );
            assert!(
                text.contains("mandate_event_id"),
                "expected mandate event id in provider context: {text}"
            );
            return Ok(ProviderResponse {
                content: vec![CanonicalContent::text("schedule mandate started")],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::EndTurn,
            });
        }

        let names = tool_names(request);
        assert!(
            names.contains(&MANDATE_START_OPERATION.to_string()),
            "expected mandate_start direct tool in {names:?}"
        );
        Ok(ProviderResponse {
            content: vec![CanonicalContent::tool_call(
                "call_mandate_start_1",
                MANDATE_START_OPERATION,
                json!({
                    "schedule": { "interval": { "every_ms": 60_000 } },
                    "input_template": "remind me in a minute"
                }),
            )],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::ToolUse,
        })
    }
}

impl ProviderRequestRecorder for ScheduleMandateStartClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

#[derive(Clone)]
enum SequencedStreamResponse {
    TextDelta(String),
    Content(String),
}

impl SequencedStreamResponse {
    fn text_delta(text: &str) -> Self {
        Self::TextDelta(text.to_string())
    }

    fn content(text: &str) -> Self {
        Self::Content(text.to_string())
    }

    fn text(&self) -> &str {
        match self {
            Self::TextDelta(text) | Self::Content(text) => text,
        }
    }
}

// lexicon-allow: capsule - existing test client name
struct SequencedStreamCapsuleClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
    responses: std::sync::Mutex<Vec<SequencedStreamResponse>>,
}

struct BurstStreamClient {
    deltas: Vec<String>,
}

#[derive(Default)]
struct LagThenBlockStreamClient {
    request_count: std::sync::atomic::AtomicUsize,
    second_request_started: tokio::sync::Notify,
    release_second_request: tokio::sync::Notify,
}

impl LagThenBlockStreamClient {
    async fn wait_for_second_request(&self) {
        self.second_request_started.notified().await;
    }

    fn release_second_request(&self) {
        self.release_second_request.notify_one();
    }
}

#[async_trait::async_trait]
impl ProviderClient for BurstStreamClient {
    async fn complete(&self, _request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text(self.deltas.concat())],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }

    async fn stream(
        &self,
        _request: &ProviderRequest,
    ) -> ProviderResult<Vec<crate::ProviderStreamEvent>> {
        let mut events = self
            .deltas
            .iter()
            .cloned()
            .map(|text| crate::ProviderStreamEvent::TextDelta { text })
            .collect::<Vec<_>>();
        events.push(crate::ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::EndTurn,
        });
        Ok(events)
    }
}

#[async_trait::async_trait]
impl ProviderClient for LagThenBlockStreamClient {
    async fn complete(&self, _request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("lag race completion")],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }

    async fn stream(
        &self,
        _request: &ProviderRequest,
    ) -> ProviderResult<Vec<crate::ProviderStreamEvent>> {
        let request_index = self
            .request_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if request_index == 0 {
            let mut events = (0..1_100)
                .map(|index| crate::ProviderStreamEvent::TextDelta {
                    text: format!("{index:04}|"),
                })
                .collect::<Vec<_>>();
            events.push(crate::ProviderStreamEvent::Done {
                stop_reason: CanonicalStopReason::EndTurn,
            });
            return Ok(events);
        }

        self.second_request_started.notify_one();
        self.release_second_request.notified().await;
        Ok(vec![
            crate::ProviderStreamEvent::TextDelta {
                text: "second turn complete".to_string(),
            },
            crate::ProviderStreamEvent::Done {
                stop_reason: CanonicalStopReason::EndTurn,
            },
        ])
    }
}

// lexicon-allow: capsule - existing test client name
impl SequencedStreamCapsuleClient {
    fn new<const N: usize>(responses: [&str; N]) -> Self {
        Self::new_modes(responses.map(SequencedStreamResponse::content))
    }

    fn new_modes<const N: usize>(responses: [SequencedStreamResponse; N]) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            responses: std::sync::Mutex::new(responses.into_iter().collect()),
        }
    }

    fn next_response(&self, request: &ProviderRequest) -> SequencedStreamResponse {
        self.requests.lock().unwrap().push(request.clone());
        let mut responses = self.responses.lock().unwrap();
        if responses.is_empty() {
            return SequencedStreamResponse::content("sequenced fallback completion");
        }
        responses.remove(0)
    }
}

// lexicon-allow: capsule - existing test client name
#[async_trait::async_trait]
// lexicon-allow: capsule - existing test client name
impl ProviderClient for SequencedStreamCapsuleClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        let text = self.next_response(request).text().to_string();
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text(text)],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }

    async fn stream(
        &self,
        request: &ProviderRequest,
    ) -> ProviderResult<Vec<crate::ProviderStreamEvent>> {
        let response = self.next_response(request);
        let mut events = match response {
            SequencedStreamResponse::TextDelta(text) => {
                vec![crate::ProviderStreamEvent::TextDelta { text }]
            }
            SequencedStreamResponse::Content(text) => {
                vec![crate::ProviderStreamEvent::Content {
                    content: CanonicalContent::text(text),
                }]
            }
        };
        events.push(crate::ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::EndTurn,
        });
        Ok(events)
    }
}

struct FailingProviderClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
    message: String,
}

impl FailingProviderClient {
    fn new(message: &str) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            message: message.to_string(),
        }
    }

    fn record(&self, request: &ProviderRequest) -> ProviderError {
        self.requests.lock().unwrap().push(request.clone());
        ProviderError::Decode(self.message.clone())
    }
}

#[async_trait::async_trait]
impl ProviderClient for FailingProviderClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        Err(self.record(request))
    }

    async fn stream(
        &self,
        request: &ProviderRequest,
    ) -> ProviderResult<Vec<crate::ProviderStreamEvent>> {
        Err(self.record(request))
    }
}

#[derive(Default)]
struct SkillResourceClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
}

#[async_trait::async_trait]
impl ProviderClient for SkillResourceClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let text = text_from_canonical_messages(&request.messages);
        let has_tool_result = request
            .messages
            .iter()
            .any(|message| matches!(message, CanonicalMessage::ToolResult { .. }));
        if !has_tool_result {
            assert!(
                text.contains("alpha — Alpha description."),
                "provider request did not include skill index: {text}"
            );
            let names = tool_names(request);
            assert!(names.contains(&"bash".to_string()));
            return Ok(ProviderResponse {
                content: vec![CanonicalContent::tool_call(
                    "call_bash_skill",
                    "bash",
                    json!({
                        "command": "cat /skills/alpha.md; printf '\\nWRITE:\\n'; echo nope > /skills/alpha.md"
                    }),
                )],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::ToolUse,
            });
        }

        assert!(
            text.contains("Alpha body marker."),
            "bash result did not include skill body: {text}"
        );
        assert!(
            text.contains("read-only") || text.contains("denied"),
            "bash result did not include read-only denial: {text}"
        );
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("skill read completed")],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

#[derive(Default)]
struct WorkspaceSkillDiscoveryClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
}

#[async_trait::async_trait]
impl ProviderClient for WorkspaceSkillDiscoveryClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let text = text_from_canonical_messages(&request.messages);
        let has_tool_result = request
            .messages
            .iter()
            .any(|message| matches!(message, CanonicalMessage::ToolResult { .. }));
        if !has_tool_result {
            assert!(
                text.contains(
                    "alpha — Original discovery description. — .agents/skills/alpha/SKILL.md"
                ),
                "provider request did not include the witnessed workspace skill index: {text}"
            );
            assert!(tool_names(request).contains(&"bash".to_string()));
            return Ok(ProviderResponse {
                content: vec![CanonicalContent::tool_call(
                    "call_bash_workspace_skill",
                    "bash",
                    json!({
                        "command": "cat /work/.agents/skills/alpha/SKILL.md"
                    }),
                )],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::ToolUse,
            });
        }
        assert!(
            text.contains("Changed discovery body marker."),
            "workspace bash did not read the live edited skill body: {text}"
        );
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("workspace skill read completed")],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

impl ProviderRequestRecorder for WorkspaceSkillDiscoveryClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

#[derive(Default)]
struct WorkspaceBindingClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
}

#[async_trait::async_trait]
impl ProviderClient for WorkspaceBindingClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let has_tool_result = request
            .messages
            .iter()
            .any(|message| matches!(message, CanonicalMessage::ToolResult { .. }));
        if !has_tool_result {
            assert!(tool_names(request).contains(&"bash".to_string()));
            return Ok(ProviderResponse {
                content: vec![CanonicalContent::tool_call(
                    "call_workspace_bash",
                    "bash",
                    json!({
                        "command": r#"ls /work
cat /work/note.txt
apply_patch <<'PATCH'
*** Begin Patch
*** Update File: /work/note.txt
@@
-seed
+updated
*** End Patch
PATCH
cat /work/outside-link || true
printf hacked > /work/outside-link || true
printf traversal > /work/../outside.txt
printf absolute > /absolute-outside.txt"#
                    }),
                )],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::ToolUse,
            });
        }

        let text = text_from_canonical_messages(&request.messages);
        assert!(
            text.contains("note.txt"),
            "workspace ls was not returned: {text}"
        );
        assert!(
            text.contains("seed"),
            "workspace cat was not returned: {text}"
        );
        assert!(
            text.contains("path escapes realfs root")
                || text.contains("symlink")
                || text.contains("Permission denied"),
            "symlink escape denial was not returned: {text}"
        );
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("workspace edit completed")],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

impl ProviderRequestRecorder for WorkspaceBindingClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

// lexicon-allow: capsule - existing test client name
struct BashCallingCapsuleClient {
    requests: std::sync::Mutex<Vec<ProviderRequest>>,
    direct_tool_name: String,
    shell_command_name: String,
    command: String,
    expected_output: String,
}

// lexicon-allow: capsule - existing test client name
impl BashCallingCapsuleClient {
    fn new(
        direct_tool_name: &str,
        shell_command_name: &str,
        command: &str,
        expected_output: &str,
    ) -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            direct_tool_name: direct_tool_name.to_string(),
            shell_command_name: shell_command_name.to_string(),
            command: command.to_string(),
            expected_output: expected_output.to_string(),
        }
    }

    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
// lexicon-allow: capsule - existing test client name
impl ProviderClient for BashCallingCapsuleClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let has_tool_result = request
            .messages
            .iter()
            .any(|message| matches!(message, CanonicalMessage::ToolResult { .. }));
        if !has_tool_result {
            let names = tool_names(request);
            assert!(
                names.contains(&self.direct_tool_name),
                "expected direct tool {:?} in {:?}",
                self.direct_tool_name,
                names
            );
            assert_bash_tool_describes(request, &self.shell_command_name);
            return Ok(ProviderResponse {
                content: vec![CanonicalContent::tool_call(
                    "call_bash_1",
                    "bash",
                    json!({ "command": self.command }),
                )],
                usage: CanonicalUsage::default(),
                stop_reason: CanonicalStopReason::ToolUse,
            });
        }

        let text = text_from_canonical_messages(&request.messages);
        assert!(
            text.contains(&self.expected_output),
            "expected bash result to contain {:?}, got: {text}",
            self.expected_output
        );
        Ok(ProviderResponse {
            // lexicon-allow: capsule - existing fixture response text
            content: vec![CanonicalContent::text("capsule command completed")],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

async fn wait_for_provider_requests<T>(client: &Arc<T>, count: usize)
where
    T: ProviderRequestRecorder + ?Sized,
{
    for _ in 0..100 {
        if client.recorded_request_count() >= count {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for {count} provider request(s), saw {}",
        client.recorded_request_count()
    );
}

async fn wait_for_lifecycle_status(
    store: &SqliteMetadataStore,
    thread_id: ThreadId,
    status: crate::ThreadLifecycleStatus,
) -> ThreadLifecycleRecord {
    for _ in 0..100 {
        if let Some(record) = store.get_thread_lifecycle(thread_id).await.unwrap()
            && record.status == status
        {
            return record;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for lifecycle status {status:?} on {thread_id}");
}

async fn wait_for_session_text(app: &CooldisAppServer, thread_id: &str, expected: &str) {
    let parsed = ThreadId::parse_str(thread_id).unwrap();
    for _ in 0..100 {
        if let Ok(handle) = app
            .inner
            .supervisor
            .get_thread(&app.inner.tenant_id, parsed)
            .await
            && let Ok(context) = handle.session_context().await
        {
            let text = text_from_canonical_messages(&context.messages);
            if text.contains(expected) {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for session text {expected:?} in thread {thread_id}");
}

async fn wait_for_turn_completed_notification(
    outbound_rx: &mut mpsc::UnboundedReceiver<JsonRpcMessage>,
    thread_id: &str,
    turn_id: &str,
) -> Value {
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut observed = Vec::new();
    loop {
        tokio::select! {
            _ = &mut deadline => {
                panic!(
                    "timed out waiting for turn/completed for {thread_id}/{turn_id}; observed {observed:?}"
                );
            }
            message = outbound_rx.recv() => {
                let Some(JsonRpcMessage::Notification(notification)) = message else {
                    continue;
                };
                observed.push(notification.method.clone());
                if notification.method == "turn/completed"
                    && notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("threadId"))
                        .and_then(Value::as_str)
                        == Some(thread_id)
                    && notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("turn"))
                        .and_then(|turn| turn.get("id"))
                        .and_then(Value::as_str)
                        == Some(turn_id)
                {
                    return notification.params.unwrap_or(Value::Null);
                }
            }
        }
    }
}

async fn wait_for_failed_turn_and_closed_thread(
    outbound_rx: &mut mpsc::UnboundedReceiver<JsonRpcMessage>,
    thread_id: &str,
    turn_id: &str,
) -> Value {
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut observed = Vec::new();
    let mut completed = None;
    let mut saw_failed_status = false;
    let mut saw_closed = false;
    loop {
        if let Some(completed) = completed.clone()
            && saw_failed_status
            && saw_closed
        {
            return completed;
        }
        tokio::select! {
            _ = &mut deadline => {
                panic!(
                    "timed out waiting for failed turn and closed thread for {thread_id}/{turn_id}; observed {observed:?}; completed {completed:?}; failed status {saw_failed_status}; closed {saw_closed}"
                );
            }
            message = outbound_rx.recv() => {
                let Some(JsonRpcMessage::Notification(notification)) = message else {
                    continue;
                };
                observed.push(notification.method.clone());
                let Some(params) = notification.params.as_ref() else {
                    continue;
                };
                if params.get("threadId").and_then(Value::as_str) != Some(thread_id) {
                    continue;
                }
                match notification.method.as_str() {
                    "thread/status/changed"
                        if params
                            .get("status")
                            .and_then(|status| status.get("type"))
                            .and_then(Value::as_str)
                            == Some("systemError") =>
                    {
                        saw_failed_status = true;
                    }
                    "thread/closed" => {
                        saw_closed = true;
                    }
                    "turn/completed"
                        if params
                            .get("turn")
                            .and_then(|turn| turn.get("id"))
                            .and_then(Value::as_str)
                            == Some(turn_id) =>
                    {
                        completed = Some(params.clone());
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn collect_agent_deltas_until_turn_completed(
    outbound_rx: &mut mpsc::UnboundedReceiver<JsonRpcMessage>,
    thread_id: &str,
    turn_id: &str,
) -> (String, Value) {
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut observed = Vec::new();
    let mut deltas = String::new();
    loop {
        tokio::select! {
            _ = &mut deadline => {
                panic!(
                    "timed out waiting for restored turn stream for {thread_id}/{turn_id}; observed {observed:?}; deltas {deltas:?}"
                );
            }
            message = outbound_rx.recv() => {
                let Some(JsonRpcMessage::Notification(notification)) = message else {
                    continue;
                };
                observed.push(notification.method.clone());
                if notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("threadId"))
                    .and_then(Value::as_str)
                    != Some(thread_id)
                {
                    continue;
                }
                if notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("turnId"))
                    .and_then(Value::as_str)
                    == Some(turn_id)
                    && notification.method == "item/agentMessage/delta"
                {
                    if let Some(delta) = notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("delta"))
                        .and_then(Value::as_str)
                    {
                        deltas.push_str(delta);
                    }
                }
                if notification.method == "turn/completed"
                    && notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("turn"))
                        .and_then(|turn| turn.get("id"))
                        .and_then(Value::as_str)
                        == Some(turn_id)
                {
                    return (deltas, notification.params.unwrap_or(Value::Null));
                }
            }
        }
    }
}

async fn collect_message_and_thinking_deltas_until_turn_completed(
    outbound_rx: &mut mpsc::UnboundedReceiver<JsonRpcMessage>,
    thread_id: &str,
    turn_id: &str,
) -> (String, String, Value) {
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut observed = Vec::new();
    let mut message_deltas = String::new();
    let mut thinking_deltas = String::new();
    loop {
        tokio::select! {
            _ = &mut deadline => {
                panic!(
                    "timed out waiting for split stream for {thread_id}/{turn_id}; observed {observed:?}; message {message_deltas:?}; thinking {thinking_deltas:?}"
                );
            }
            message = outbound_rx.recv() => {
                let Some(JsonRpcMessage::Notification(notification)) = message else {
                    continue;
                };
                observed.push(notification.method.clone());
                if notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("threadId"))
                    .and_then(Value::as_str)
                    != Some(thread_id)
                {
                    continue;
                }
                if notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("turnId"))
                    .and_then(Value::as_str)
                    == Some(turn_id)
                {
                    match notification.method.as_str() {
                        "item/agentMessage/delta" => {
                            if let Some(delta) = notification
                                .params
                                .as_ref()
                                .and_then(|params| params.get("delta"))
                                .and_then(Value::as_str)
                            {
                                message_deltas.push_str(delta);
                            }
                        }
                        "item/agentThinking/delta" => {
                            if let Some(delta) = notification
                                .params
                                .as_ref()
                                .and_then(|params| params.get("delta"))
                                .and_then(Value::as_str)
                            {
                                thinking_deltas.push_str(delta);
                            }
                        }
                        _ => {}
                    }
                }
                if notification.method == "turn/completed"
                    && notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("turn"))
                        .and_then(|turn| turn.get("id"))
                        .and_then(Value::as_str)
                        == Some(turn_id)
                {
                    return (
                        message_deltas,
                        thinking_deltas,
                        notification.params.unwrap_or(Value::Null),
                    );
                }
            }
        }
    }
}

async fn collect_delta_methods_until_turn_completed(
    outbound_rx: &mut mpsc::UnboundedReceiver<JsonRpcMessage>,
    thread_id: &str,
    turn_id: &str,
) -> Vec<String> {
    let deadline = tokio::time::sleep(std::time::Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut observed = Vec::new();
    loop {
        tokio::select! {
            _ = &mut deadline => {
                panic!(
                    "timed out waiting for delta methods for {thread_id}/{turn_id}; observed {observed:?}"
                );
            }
            message = outbound_rx.recv() => {
                let Some(JsonRpcMessage::Notification(notification)) = message else {
                    continue;
                };
                if notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("threadId"))
                    .and_then(Value::as_str)
                    != Some(thread_id)
                {
                    continue;
                }
                if notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("turnId"))
                    .and_then(Value::as_str)
                    == Some(turn_id)
                {
                    match notification.method.as_str() {
                        "item/agentMessage/delta" | "item/agentThinking/delta" => {
                            observed.push(notification.method.clone());
                        }
                        _ => {}
                    }
                }
                if notification.method == "turn/completed"
                    && notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("turn"))
                        .and_then(|turn| turn.get("id"))
                        .and_then(Value::as_str)
                        == Some(turn_id)
                {
                    return observed;
                }
            }
        }
    }
}

async fn assert_no_extra_turn_delta_or_completed(
    outbound_rx: &mut mpsc::UnboundedReceiver<JsonRpcMessage>,
    thread_id: &str,
    turn_id: &str,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(50);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let message = tokio::time::timeout(remaining, outbound_rx.recv()).await;
        let Ok(Some(JsonRpcMessage::Notification(notification))) = message else {
            continue;
        };
        let Some(params) = notification.params.as_ref() else {
            continue;
        };
        if params.get("threadId").and_then(Value::as_str) != Some(thread_id) {
            continue;
        }
        let matches_turn_id = params.get("turnId").and_then(Value::as_str) == Some(turn_id)
            || params
                .get("turn")
                .and_then(|turn| turn.get("id"))
                .and_then(Value::as_str)
                == Some(turn_id);
        if matches_turn_id
            && matches!(
                notification.method.as_str(),
                "item/agentMessage/delta" | "turn/completed"
            )
        {
            panic!(
                "saw extra {} for {thread_id}/{turn_id}: {:?}",
                notification.method, params
            );
        }
    }
}

async fn wait_for_assistant_texts(
    app: &CooldisAppServer,
    thread_id: &str,
    expected_count: usize,
) -> Vec<String> {
    let parsed = ThreadId::parse_str(thread_id).unwrap();
    for _ in 0..100 {
        if let Ok(handle) = app
            .inner
            .supervisor
            .get_thread(&app.inner.tenant_id, parsed)
            .await
            && let Ok(context) = handle.session_context().await
        {
            let texts = assistant_texts(&context.messages);
            if texts.len() == expected_count {
                return texts;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {expected_count} assistant message(s) in {thread_id}");
}

fn assistant_texts(messages: &[CanonicalMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| match message {
            CanonicalMessage::Assistant { content, .. } => {
                Some(text_from_canonical_content(content))
            }
            CanonicalMessage::User { .. } | CanonicalMessage::ToolResult { .. } => None,
        })
        .filter(|text| !text.is_empty())
        .collect()
}

fn turn_item_texts(turns: &[Value]) -> Vec<Vec<String>> {
    turns
        .iter()
        .map(|turn| {
            turn["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(item_text)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn turn_has_agent_delta(turn_completed_params: &Value, expected: &str) -> bool {
    turn_completed_params["turn"]["items"]
        .as_array()
        .is_some_and(|items| {
            items
                .iter()
                .filter_map(item_text)
                .any(|text| text == expected)
        })
}

fn completed_turn_agent_text(turn_completed_params: &Value) -> Option<String> {
    turn_completed_params["turn"]["items"]
        .as_array()?
        .iter()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("agentMessage"))
        .and_then(item_text)
}

fn item_text_by_type(items: &[Value], item_type: &str) -> Option<String> {
    items
        .iter()
        .find(|item| item.get("type").and_then(Value::as_str) == Some(item_type))
        .and_then(item_text)
}

fn last_user_message_text(request: &ProviderRequest) -> Option<String> {
    request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            CanonicalMessage::User { content, .. } => Some(text_from_canonical_content(content)),
            _ => None,
        })
        .filter(|text| !text.is_empty())
}

fn item_text(item: &Value) -> Option<String> {
    if let Some(text) = item.get("text").and_then(Value::as_str) {
        return Some(text.to_string());
    }
    item.get("content")
        .and_then(Value::as_array)
        .map(|content| {
            content
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|text| !text.is_empty())
}

#[cfg(unix)]
async fn connect_tui_test_client(
    socket: &Path,
    client_name: &str,
) -> crate::CodexTuiTestClient<tokio::net::UnixStream> {
    let mut last_error = None;
    for _ in 0..100 {
        match crate::CodexTuiTestClient::connect_unix(
            socket,
            crate::CodexTuiConnectConfig {
                client_name: client_name.to_string(),
                ..crate::CodexTuiConnectConfig::default()
            },
        )
        .await
        {
            Ok(client) => return client,
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    panic!(
        "timed out connecting Codex TUI test client to {}; last error: {}",
        socket.display(),
        last_error.unwrap_or_else(|| "none".to_string())
    );
}

async fn connect_ws_tui_test_client(
    url: &str,
    token: &str,
) -> crate::CodexTuiTestClient<tokio::net::TcpStream> {
    let mut last_error = None;
    for _ in 0..100 {
        match crate::CodexTuiTestClient::connect_websocket(
            url,
            crate::CodexTuiConnectConfig {
                client_name: "websocket-listen-test".to_string(),
                bearer_token: Some(token.to_string()),
                ..crate::CodexTuiConnectConfig::default()
            },
        )
        .await
        {
            Ok(client) => return client,
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    panic!(
        "timed out connecting Codex TUI test client to {url}; last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    );
}

async fn mint_app_server_test_token(app: &CooldisAppServer) -> String {
    let store = SqliteSessionStore::open(app.session_store_path())
        .await
        .unwrap();
    let authority = SqliteIdentityAuthority::new(store, Arc::new(SystemDaemonClock), None)
        .await
        .unwrap();
    let principal = PrincipalId::new(app.user_id());
    authority
        .mint_credential(&principal, &principal, None)
        .await
        .unwrap()
        .1
}

fn console_token_from_response(response: &str) -> String {
    let marker = "sessionToken:";
    let start = response.find(marker).unwrap() + marker.len();
    let value = response[start..].split('}').next().unwrap();
    serde_json::from_str(value).unwrap()
}

fn unused_loopback_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn get_tcp_health_response(addr: std::net::SocketAddr, path: &str) -> String {
    get_tcp_response(addr, path).await
}

async fn get_tcp_response(addr: std::net::SocketAddr, path: &str) -> String {
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    get_tcp_raw_response(addr, &request).await
}

async fn get_tcp_raw_response(addr: std::net::SocketAddr, request: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut last_error = None;
    for _ in 0..100 {
        match TcpStream::connect(addr).await {
            Ok(mut stream) => {
                stream.write_all(request.as_bytes()).await.unwrap();
                let mut response = String::new();
                stream.read_to_string(&mut response).await.unwrap();
                return response;
            }
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    panic!(
        "timed out connecting TCP probe to {addr}; last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    );
}

trait ProviderRequestRecorder {
    fn recorded_request_count(&self) -> usize;
}

impl ProviderRequestRecorder for ThinkingRecorderClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

// lexicon-allow: capsule - existing test client name
impl ProviderRequestRecorder for InspectingCapsuleClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

// lexicon-allow: capsule - existing test client name
impl ProviderRequestRecorder for SequencedStreamCapsuleClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl ProviderRequestRecorder for FailingProviderClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl ProviderRequestRecorder for SkillResourceClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

// lexicon-allow: capsule - existing test client name
impl ProviderRequestRecorder for BashCallingCapsuleClient {
    fn recorded_request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

fn tool_names(request: &ProviderRequest) -> Vec<String> {
    request
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>()
}

fn latest_user_text(request: &ProviderRequest) -> Option<String> {
    request.messages.iter().rev().find_map(|message| {
        let CanonicalMessage::User { content, .. } = message else {
            return None;
        };
        Some(text_from_canonical_content(content))
    })
}

fn thread_operation_names() -> Vec<&'static str> {
    vec![
        THREAD_SPAWN_OPERATION,
        THREAD_SUBMIT_OPERATION,
        THREAD_WAIT_OPERATION,
        THREAD_STATUS_OPERATION,
        THREAD_CANCEL_OPERATION,
    ]
}

fn thread_operation_capability(operation: &str) -> &'static str {
    match operation {
        THREAD_SPAWN_OPERATION => THREADS_SPAWN_CAPABILITY,
        THREAD_SUBMIT_OPERATION | THREAD_CANCEL_OPERATION => THREADS_CONTROL_CAPABILITY,
        THREAD_WAIT_OPERATION | THREAD_STATUS_OPERATION => THREADS_READ_CAPABILITY,
        other => panic!("unknown thread operation {other}"),
    }
}

fn json_array_string_set(value: &Value) -> BTreeSet<String> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {value}"))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("expected string item, got {item}"))
                .to_string()
        })
        .collect()
}

fn assert_bash_tool_describes(request: &ProviderRequest, command: &str) {
    let description = bash_tool_description(request);
    assert!(
        description.contains(command),
        "expected bash description to mention {command:?}: {description}"
    );
}

fn assert_bash_tool_omits(request: &ProviderRequest, command: &str) {
    let description = bash_tool_description(request);
    assert!(
        !description.contains(command),
        "expected bash description to omit {command:?}: {description}"
    );
}

fn assert_bash_tool_absent_or_omits(request: &ProviderRequest, command: &str) {
    let Some(description) = request
        .tools
        .iter()
        .find(|tool| tool.name == "bash")
        .map(|tool| tool.description.clone())
    else {
        return;
    };
    assert!(
        !description.contains(command),
        "expected bash description to omit {command:?}: {description}"
    );
}

fn bash_tool_description(request: &ProviderRequest) -> String {
    request
        .tools
        .iter()
        .find(|tool| tool.name == "bash")
        .map(|tool| tool.description.clone())
        .expect("bash tool should be advertised")
}

fn text_from_canonical_messages(messages: &[CanonicalMessage]) -> String {
    messages
        .iter()
        .map(|message| match message {
            CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => text_from_canonical_content(content),
        })
        .collect::<Vec<_>>()
        .join("\n")
}
