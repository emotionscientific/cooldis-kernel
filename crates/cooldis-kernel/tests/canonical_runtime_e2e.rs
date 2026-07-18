use cooldis::{
    AgentLoopConfig, AgentLoopFactory, AnthropicMessagesAdapter, CanonicalContent,
    CanonicalMessage, CanonicalStopReason, CooldisSupervisor, OpenAIChatCompletionsAdapter,
    OpenAIReasoningSummary, OpenAIResponsesAdapter, ProviderApi, ProviderAuth, ProviderEndpoint,
    ProviderHttpClient, ProviderWireAdapter, RuntimeEventKind, RuntimeHost, SessionEntryKind,
    SessionStore, SqliteSessionStore, TenantRegistration, TenantRuntimeContext, ThreadCoordinates,
    ThreadEvent, ThreadStartRequest, ThreadTopology,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn openai_runtime_replays_persisted_sqlite_history_after_restart() {
    let db_path = temp_db_path("cooldis-e2e-openai");
    let server = MockHttpServer::start(vec![
        json!({
            "status": "completed",
            "output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": "first reply"}]
            }],
            "usage": {"input_tokens": 11, "output_tokens": 2}
        }),
        json!({
            "status": "completed",
            "output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": "second reply"}]
            }],
            "usage": {"input_tokens": 22, "output_tokens": 3}
        }),
    ])
    .await;
    let mut coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");

    {
        let host = openai_host(&server, &db_path).await;
        let thread = host
            .start_thread(coordinates.clone(), ThreadTopology::root())
            .await
            .unwrap();
        coordinates = thread.context().coordinates.clone();
        let mut events = thread.subscribe_events();

        host.submit(coordinates.thread_id, "turn-1", "first prompt")
            .await
            .unwrap();
        assert_eq!(next_output(&mut events).await, "first reply");
        host.shutdown_thread(coordinates.thread_id).await.unwrap();
    }

    {
        let host = openai_host(&server, &db_path).await;
        let thread = host
            .start_thread(coordinates.clone(), ThreadTopology::root())
            .await
            .unwrap();
        let mut events = thread.subscribe_events();

        host.submit(coordinates.thread_id, "turn-2", "second prompt")
            .await
            .unwrap();
        assert_eq!(next_output(&mut events).await, "second reply");

        let context = thread.session_context().await.unwrap();
        assert_eq!(
            text_messages(&context.messages),
            vec![
                "first prompt",
                "first reply",
                "second prompt",
                "second reply"
            ]
        );
        assert!(
            context
                .entries
                .iter()
                .all(|entry| { matches!(entry.kind, SessionEntryKind::Message { .. }) })
        );
        host.shutdown_thread(coordinates.thread_id).await.unwrap();
    }

    let requests = server.requests(2).await;
    assert_eq!(requests[0].path, "/v1/responses");
    assert_eq!(requests[0].header("authorization"), Some("Bearer e2e-key"));
    assert_eq!(requests[0].body["model"], "gpt-e2e");
    assert_eq!(openai_input_texts(&requests[0].body), vec!["first prompt"]);
    assert_eq!(
        openai_input_texts(&requests[1].body),
        vec!["first prompt", "first reply", "second prompt"]
    );

    let stored_payloads = sqlite_entry_payloads(&db_path).await;
    assert_eq!(stored_payloads.len(), 4);
    assert!(
        stored_payloads
            .iter()
            .any(|payload| payload.contains("openai"))
    );
    assert!(stored_payloads.iter().all(|payload| {
        !payload.contains("max_output_tokens") && !payload.contains("tool_choice")
    }));

    server.join().await;
    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn anthropic_runtime_stores_tool_use_as_canonical_tool_call() {
    let db_path = temp_db_path("cooldis-e2e-anthropic");
    let server = MockHttpServer::start(vec![json!({
        "stop_reason": "tool_use",
        "content": [{
            "type": "tool_use",
            "id": "toolu_123",
            "name": "bash",
            "input": {"command": "pwd"}
        }],
        "usage": {"input_tokens": 7, "output_tokens": 8}
    })])
    .await;
    let host = anthropic_host(&server, &db_path).await;
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let mut events = thread.subscribe_events();

    host.submit(thread_id, "turn-1", "run pwd").await.unwrap();
    let assistant = next_assistant_message(&mut events).await;

    match assistant {
        CanonicalMessage::Assistant {
            provider,
            api,
            model,
            content,
            stop_reason,
            ..
        } => {
            assert_eq!(provider, "anthropic");
            assert_eq!(api, ProviderApi::AnthropicMessages);
            assert_eq!(model, "claude-e2e");
            assert_eq!(stop_reason, CanonicalStopReason::ToolUse);
            assert!(matches!(
                content.first(),
                Some(CanonicalContent::ToolCall { id, name, arguments })
                    if id == "toolu_123"
                        && name == "bash"
                        && arguments["command"] == "pwd"
            ));
        }
        other => panic!("expected assistant message, got {other:?}"),
    }

    let requests = server.requests(1).await;
    assert_eq!(requests[0].path, "/anthropic/v1/messages");
    assert_eq!(requests[0].header("x-api-key"), Some("anthropic-key"));
    assert_eq!(requests[0].header("anthropic-version"), Some("2023-06-01"));
    assert_eq!(requests[0].body["messages"][0]["role"], "user");
    assert_eq!(
        requests[0].body["messages"][0]["content"][0]["text"],
        "run pwd"
    );

    let stored_payloads = sqlite_entry_payloads(&db_path).await;
    assert!(
        stored_payloads
            .iter()
            .any(|payload| payload.contains("tool_call"))
    );
    assert!(
        stored_payloads
            .iter()
            .all(|payload| !payload.contains(r#""type":"tool_use""#))
    );

    host.shutdown_thread(thread_id).await.unwrap();
    server.join().await;
    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn chat_completions_runtime_replays_same_canonical_sqlite_history() {
    let db_path = temp_db_path("cooldis-e2e-chat");
    let server = MockHttpServer::start(vec![
        json!({
            "choices": [{
                "finish_reason": "stop",
                "message": {"content": "first chat reply"}
            }],
            "usage": {"prompt_tokens": 3, "completion_tokens": 4}
        }),
        json!({
            "choices": [{
                "finish_reason": "stop",
                "message": {"content": "second chat reply"}
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 6}
        }),
    ])
    .await;
    let mut coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");

    {
        let host = chat_host(&server, &db_path).await;
        let thread = host
            .start_thread(coordinates.clone(), ThreadTopology::root())
            .await
            .unwrap();
        coordinates = thread.context().coordinates.clone();
        let mut events = thread.subscribe_events();

        host.submit(coordinates.thread_id, "turn-1", "first chat prompt")
            .await
            .unwrap();
        assert_eq!(next_output(&mut events).await, "first chat reply");
        host.shutdown_thread(coordinates.thread_id).await.unwrap();
    }

    {
        let host = chat_host(&server, &db_path).await;
        let thread = host
            .start_thread(coordinates.clone(), ThreadTopology::root())
            .await
            .unwrap();
        let mut events = thread.subscribe_events();

        host.submit(coordinates.thread_id, "turn-2", "second chat prompt")
            .await
            .unwrap();
        assert_eq!(next_output(&mut events).await, "second chat reply");
        host.shutdown_thread(coordinates.thread_id).await.unwrap();
    }

    let requests = server.requests(2).await;
    assert_eq!(requests[0].path, "/v1/chat/completions");
    assert_eq!(
        requests[0].body["messages"][0]["content"],
        "first chat prompt"
    );
    assert_eq!(
        requests[1].body["messages"][0]["content"],
        "first chat prompt"
    );
    assert_eq!(
        requests[1].body["messages"][1]["content"],
        "first chat reply"
    );
    assert_eq!(
        requests[1].body["messages"][2]["content"],
        "second chat prompt"
    );

    let stored_payloads = sqlite_entry_payloads(&db_path).await;
    assert_eq!(stored_payloads.len(), 4);
    assert!(stored_payloads.iter().all(|payload| {
        !payload.contains(r#""choices""#) && !payload.contains(r#""tool_choice""#)
    }));

    server.join().await;
    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn supervisor_resume_and_fork_from_checkpoint_after_restart_keep_branches_isolated() {
    let db_path = temp_db_path("cooldis-e2e-supervisor-branch");
    let runtime_root = temp_dir_path("cooldis-e2e-supervisor-branch");
    let server = MockHttpServer::start(vec![
        openai_text_response("first reply"),
        openai_text_response("after checkpoint reply"),
        openai_text_response("resumed reply"),
        openai_text_response("fork reply"),
        openai_text_response("parent reply"),
    ])
    .await;

    let checkpoint = {
        let supervisor = openai_supervisor(&server, &db_path, &runtime_root).await;
        let thread = supervisor
            .start_thread(ThreadStartRequest {
                tenant_id: "tenant_a".to_string(),
                user_id: "user_1".to_string(),
                session_id: "session_1".to_string(),
                topology: ThreadTopology::root(),
                metadata: Default::default(),
            })
            .await
            .unwrap();
        let coordinates = thread.context().coordinates.clone();
        let mut events = thread.subscribe_events();

        supervisor
            .submit_to(&coordinates, "turn-1", "first prompt")
            .await
            .unwrap();
        assert_eq!(next_output(&mut events).await, "first reply");
        let checkpoint = supervisor
            .create_checkpoint_at(
                &coordinates,
                None,
                Some("branch-point".to_string()),
                BTreeMap::from([("opaque_product_id".to_string(), "product-ckpt".to_string())]),
            )
            .await
            .unwrap();

        supervisor
            .submit_to(&coordinates, "turn-after", "after checkpoint prompt")
            .await
            .unwrap();
        assert_eq!(next_output(&mut events).await, "after checkpoint reply");
        supervisor.shutdown_thread_at(&coordinates).await.unwrap();
        checkpoint
    };

    let supervisor = openai_supervisor(&server, &db_path, &runtime_root).await;
    let resumed = supervisor
        .resume_thread_from_checkpoint_at(checkpoint.clone())
        .await
        .unwrap();
    let mut resumed_events = resumed.subscribe_events();
    supervisor
        .submit_to(
            &resumed.context().coordinates,
            "turn-resumed",
            "resumed prompt",
        )
        .await
        .unwrap();
    assert_eq!(next_output(&mut resumed_events).await, "resumed reply");

    let fork = supervisor
        .fork_thread_from_checkpoint_at(checkpoint.clone())
        .await
        .unwrap();
    let mut fork_events = fork.subscribe_events();
    supervisor
        .submit_to(&fork.context().coordinates, "turn-fork", "fork prompt")
        .await
        .unwrap();
    assert_eq!(next_output(&mut fork_events).await, "fork reply");

    supervisor
        .submit_to(
            &resumed.context().coordinates,
            "turn-parent",
            "parent after fork",
        )
        .await
        .unwrap();
    assert_eq!(next_output(&mut resumed_events).await, "parent reply");

    assert_eq!(
        text_messages(&resumed.session_context().await.unwrap().messages),
        vec![
            "first prompt",
            "first reply",
            "resumed prompt",
            "resumed reply",
            "parent after fork",
            "parent reply"
        ]
    );
    assert_eq!(
        text_messages(&fork.session_context().await.unwrap().messages),
        vec!["first prompt", "first reply", "fork prompt", "fork reply"]
    );

    let requests = server.requests(5).await;
    assert_eq!(
        openai_input_texts(&requests[1].body),
        vec!["first prompt", "first reply", "after checkpoint prompt"]
    );
    assert_eq!(
        openai_input_texts(&requests[2].body),
        vec!["first prompt", "first reply", "resumed prompt"]
    );
    assert_eq!(
        openai_input_texts(&requests[3].body),
        vec!["first prompt", "first reply", "fork prompt"]
    );
    assert_eq!(
        openai_input_texts(&requests[4].body),
        vec![
            "first prompt",
            "first reply",
            "resumed prompt",
            "resumed reply",
            "parent after fork"
        ]
    );

    server.join().await;
    remove_sqlite_files(&db_path);
    let _ = std::fs::remove_dir_all(runtime_root);
}

#[tokio::test]
async fn anthropic_runtime_replays_openai_tool_history_from_sqlite() {
    let db_path = temp_db_path("cooldis-e2e-provider-switch");
    let server = MockHttpServer::start(vec![json!({
        "stop_reason": "end_turn",
        "content": [{"type": "text", "text": "done"}],
        "usage": {"input_tokens": 9, "output_tokens": 2}
    })])
    .await;
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let store = SqliteSessionStore::open(&db_path).await.unwrap();
    store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("spawn worker"),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::assistant(
                    "openai",
                    ProviderApi::OpenAIResponses,
                    "gpt-e2e",
                    vec![CanonicalContent::tool_call(
                        "call_1|fc_1",
                        "cooldis_spawn_subagent",
                        json!({"task_name": "worker", "message": "echo historical child"}),
                    )],
                    CanonicalStopReason::ToolUse,
                ),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::tool_result(
                    "call_1|fc_1",
                    "cooldis_spawn_subagent",
                    r#"{"operation":"cooldis.spawn_subagent","thread_id":"historical-child"}"#,
                    false,
                ),
            },
        )
        .await
        .unwrap();

    let host = anthropic_host(&server, &db_path).await;
    let thread = host
        .start_thread(coordinates.clone(), ThreadTopology::root())
        .await
        .unwrap();
    let mut events = thread.subscribe_events();
    host.submit(coordinates.thread_id, "turn-2", "continue")
        .await
        .unwrap();
    assert_eq!(next_output(&mut events).await, "done");

    let requests = server.requests(1).await;
    let messages = requests[0].body["messages"].as_array().unwrap();
    assert_eq!(messages[0]["content"][0]["text"], "spawn worker");
    assert_eq!(messages[1]["content"][0]["type"], "tool_use");
    assert_eq!(messages[1]["content"][0]["id"], "call_1_fc_1");
    assert_eq!(messages[1]["content"][0]["name"], "cooldis_spawn_subagent");
    assert_eq!(
        messages[1]["content"][0]["input"]["message"],
        "echo historical child"
    );
    assert_eq!(messages[2]["content"][0]["type"], "tool_result");
    assert_eq!(messages[2]["content"][0]["tool_use_id"], "call_1_fc_1");
    assert_eq!(
        messages[2]["content"][0]["content"],
        r#"{"operation":"cooldis.spawn_subagent","thread_id":"historical-child"}"#
    );
    assert_eq!(messages[3]["content"][0]["text"], "continue");

    let stored_payloads = sqlite_entry_payloads(&db_path).await;
    assert!(stored_payloads.iter().all(|payload| {
        !payload.contains(r#""type":"tool_use""#) && !payload.contains(r#""messages""#)
    }));

    host.shutdown_thread(coordinates.thread_id).await.unwrap();
    server.join().await;
    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn openai_responses_http_sse_runtime_stores_canonical_stream_without_raw_payloads() {
    let db_path = temp_db_path("cooldis-e2e-openai-sse");
    let server = MockHttpServer::start_text(vec![openai_responses_sse()]).await;
    let host = openai_streaming_host(&server, &db_path).await;
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
        "stream with tool",
    )
    .await
    .unwrap();
    let (assistant, runtime_events) = next_assistant_with_runtime_events(&mut events).await;
    assert!(runtime_events.iter().any(|event| {
        matches!(event, RuntimeEventKind::TextDelta { text } if text == "streamed")
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallStarted { call_id, name, .. }
                if call_id == "call_1|fc_1" && name == "bash"
        )
    }));
    assert_streamed_tool_assistant(assistant, "call_1|fc_1");
    assert_raw_stream_payloads_not_stored(&db_path).await;

    let requests = server.requests(1).await;
    assert_eq!(requests[0].body["stream"], true);
    assert_eq!(requests[0].body["input"][0]["content"], "stream with tool");

    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();
    server.join().await;
    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn chat_http_sse_runtime_stores_canonical_stream_without_raw_payloads() {
    let db_path = temp_db_path("cooldis-e2e-chat-sse");
    let server = MockHttpServer::start_text(vec![chat_completions_sse()]).await;
    let host = chat_streaming_host(&server, &db_path).await;
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
        "stream chat",
    )
    .await
    .unwrap();
    let (assistant, runtime_events) = next_assistant_with_runtime_events(&mut events).await;
    assert!(runtime_events.iter().any(|event| {
        matches!(event, RuntimeEventKind::TextDelta { text } if text == "chat-streamed")
    }));
    assert_streamed_tool_assistant(assistant, "call_1");
    assert_raw_stream_payloads_not_stored(&db_path).await;

    let requests = server.requests(1).await;
    assert_eq!(requests[0].body["stream"], true);
    assert_eq!(requests[0].body["messages"][0]["content"], "stream chat");

    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();
    server.join().await;
    remove_sqlite_files(&db_path);
}

#[tokio::test]
async fn anthropic_http_sse_runtime_stores_canonical_stream_without_raw_payloads() {
    let db_path = temp_db_path("cooldis-e2e-anthropic-sse");
    let server = MockHttpServer::start_text(vec![anthropic_messages_sse()]).await;
    let host = anthropic_streaming_host(&server, &db_path).await;
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
        "stream anthropic",
    )
    .await
    .unwrap();
    let (assistant, runtime_events) = next_assistant_with_runtime_events(&mut events).await;
    assert!(runtime_events.iter().any(|event| {
        matches!(event, RuntimeEventKind::TextDelta { text } if text == "anthropic-streamed")
    }));
    assert_streamed_tool_assistant(assistant, "toolu_1");
    assert_raw_stream_payloads_not_stored(&db_path).await;

    let requests = server.requests(1).await;
    assert_eq!(
        requests[0].body["messages"][0]["content"][0]["text"],
        "stream anthropic"
    );

    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();
    server.join().await;
    remove_sqlite_files(&db_path);
}

async fn openai_host(server: &MockHttpServer, db_path: &Path) -> RuntimeHost {
    let factory = openai_factory(server, false);
    RuntimeHost::with_session_store(
        factory,
        Arc::new(SqliteSessionStore::open(db_path).await.unwrap()),
    )
}

async fn openai_streaming_host(server: &MockHttpServer, db_path: &Path) -> RuntimeHost {
    let factory = openai_factory(server, true);
    RuntimeHost::with_session_store(
        factory,
        Arc::new(SqliteSessionStore::open(db_path).await.unwrap()),
    )
}

async fn openai_supervisor(
    server: &MockHttpServer,
    db_path: &Path,
    runtime_root: &Path,
) -> CooldisSupervisor {
    let supervisor = CooldisSupervisor::new();
    supervisor
        .register_tenant(TenantRegistration {
            context: TenantRuntimeContext::local("tenant_a", runtime_root, runtime_root)
                .with_session_store(Arc::new(SqliteSessionStore::open(db_path).await.unwrap())),
            runtime_factory: openai_factory(server, false),
        })
        .await
        .unwrap();
    supervisor
}

fn openai_factory(server: &MockHttpServer, stream: bool) -> Arc<AgentLoopFactory> {
    let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(OpenAIResponsesAdapter {
        include_encrypted_reasoning: false,
        reasoning_summary: OpenAIReasoningSummary::Auto,
    });
    let client = Arc::new(
        ProviderHttpClient::new(
            ProviderEndpoint {
                url: format!("{}/v1/responses", server.base_url()),
                auth: ProviderAuth::Bearer {
                    token: "e2e-key".to_string(),
                },
                headers: Vec::new(),
            },
            adapter,
        )
        .unwrap(),
    );
    let mut config = AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-e2e");
    config.max_tokens = 64;
    config.stream = stream;
    Arc::new(AgentLoopFactory::new(config, client))
}

async fn chat_host(server: &MockHttpServer, db_path: &Path) -> RuntimeHost {
    let factory = chat_factory(server, false);
    RuntimeHost::with_session_store(
        factory,
        Arc::new(SqliteSessionStore::open(db_path).await.unwrap()),
    )
}

async fn chat_streaming_host(server: &MockHttpServer, db_path: &Path) -> RuntimeHost {
    let factory = chat_factory(server, true);
    RuntimeHost::with_session_store(
        factory,
        Arc::new(SqliteSessionStore::open(db_path).await.unwrap()),
    )
}

fn chat_factory(server: &MockHttpServer, stream: bool) -> Arc<AgentLoopFactory> {
    let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(OpenAIChatCompletionsAdapter);
    let client = Arc::new(
        ProviderHttpClient::new(
            ProviderEndpoint {
                url: format!("{}/v1/chat/completions", server.base_url()),
                auth: ProviderAuth::Bearer {
                    token: "chat-key".to_string(),
                },
                headers: Vec::new(),
            },
            adapter,
        )
        .unwrap(),
    );
    let mut config = AgentLoopConfig::new(
        ProviderApi::OpenAIChatCompletions,
        "openai-compatible",
        "chat-e2e",
    );
    config.max_tokens = 64;
    config.stream = stream;
    Arc::new(AgentLoopFactory::new(config, client))
}

async fn anthropic_host(server: &MockHttpServer, db_path: &Path) -> RuntimeHost {
    let factory = anthropic_factory(server, false);
    RuntimeHost::with_session_store(
        factory,
        Arc::new(SqliteSessionStore::open(db_path).await.unwrap()),
    )
}

async fn anthropic_streaming_host(server: &MockHttpServer, db_path: &Path) -> RuntimeHost {
    let factory = anthropic_factory(server, true);
    RuntimeHost::with_session_store(
        factory,
        Arc::new(SqliteSessionStore::open(db_path).await.unwrap()),
    )
}

fn anthropic_factory(server: &MockHttpServer, stream: bool) -> Arc<AgentLoopFactory> {
    let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(AnthropicMessagesAdapter);
    let client = Arc::new(
        ProviderHttpClient::new(
            ProviderEndpoint {
                url: format!("{}/anthropic/v1/messages", server.base_url()),
                auth: ProviderAuth::AnthropicApiKey {
                    key: "anthropic-key".to_string(),
                },
                headers: vec![("anthropic-version".to_string(), "2023-06-01".to_string())],
            },
            adapter,
        )
        .unwrap(),
    );
    let mut config =
        AgentLoopConfig::new(ProviderApi::AnthropicMessages, "anthropic", "claude-e2e");
    config.max_tokens = 64;
    config.stream = stream;
    Arc::new(AgentLoopFactory::new(config, client))
}

async fn next_output(events: &mut tokio::sync::broadcast::Receiver<ThreadEvent>) -> String {
    loop {
        let event = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            ThreadEvent::Output { text, .. } => return text,
            ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
            _ => {}
        }
    }
}

async fn next_assistant_message(
    events: &mut tokio::sync::broadcast::Receiver<ThreadEvent>,
) -> CanonicalMessage {
    loop {
        let event = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            ThreadEvent::CanonicalMirror { entry, .. } => {
                if let SessionEntryKind::Message {
                    message: CanonicalMessage::Assistant { .. },
                } = entry.kind
                {
                    if let SessionEntryKind::Message { message } = entry.kind {
                        return message;
                    }
                }
            }
            ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
            _ => {}
        }
    }
}

async fn next_assistant_with_runtime_events(
    events: &mut tokio::sync::broadcast::Receiver<ThreadEvent>,
) -> (CanonicalMessage, Vec<RuntimeEventKind>) {
    let mut runtime_events = Vec::new();
    loop {
        let event = timeout(Duration::from_secs(5), events.recv())
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

fn assert_streamed_tool_assistant(assistant: CanonicalMessage, expected_tool_id: &str) {
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
                CanonicalContent::Text { text, .. }
                    if text.ends_with("streamed") || text == "chat-streamed"
            ));
            assert!(content.iter().any(|content| matches!(
                content,
                CanonicalContent::ToolCall { id, name, arguments }
                    if id == expected_tool_id && name == "bash" && arguments["command"] == "pwd"
            )));
        }
        other => panic!("expected streamed assistant, got {other:?}"),
    }
}

async fn assert_raw_stream_payloads_not_stored(db_path: &Path) {
    let stored_payloads = sqlite_entry_payloads(db_path).await;
    assert!(
        stored_payloads
            .iter()
            .any(|payload| payload.contains("tool_call"))
    );
    assert!(stored_payloads.iter().all(|payload| {
        !payload.contains("response.output_text.delta")
            && !payload.contains("content_block_delta")
            && !payload.contains(r#""choices""#)
            && !payload.contains("partial_json")
    }));
}

fn text_messages(messages: &[CanonicalMessage]) -> Vec<&str> {
    messages
        .iter()
        .map(|message| match message {
            CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or(""),
        })
        .collect()
}

fn openai_input_texts(body: &Value) -> Vec<&str> {
    body["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item.get("content").and_then(Value::as_str))
        .collect()
}

fn openai_text_response(text: &str) -> Value {
    json!({
        "status": "completed",
        "output": [{
            "type": "message",
            "content": [{"type": "output_text", "text": text}]
        }],
        "usage": {"input_tokens": 1, "output_tokens": 1}
    })
}

fn openai_responses_sse() -> &'static str {
    concat!(
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"streamed\"}\n\n",
        "event: response.function_call_arguments.delta\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"call_id\":\"call_1\",\"delta\":\"{\\\"command\\\"\"}\n\n",
        "event: response.function_call_arguments.delta\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"call_id\":\"call_1\",\"delta\":\":\\\"pwd\\\"}\"}\n\n",
        "event: response.output_item.done\n",
        "data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"bash\",\"arguments\":\"{\\\"command\\\":\\\"pwd\\\"}\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"streamed\"}]},{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"bash\",\"arguments\":\"{\\\"command\\\":\\\"pwd\\\"}\"}],\"usage\":{\"input_tokens\":5,\"output_tokens\":6}}}\n\n",
    )
}

fn chat_completions_sse() -> &'static str {
    concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"chat-streamed\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"bash\",\"arguments\":\"{\\\"command\\\"\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"pwd\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":6}}\n\n",
        "data: [DONE]\n\n",
    )
}

fn anthropic_messages_sse() -> &'static str {
    concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":5}}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"anthropic-streamed\"}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"bash\",\"input\":{}}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"command\\\"\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\":\\\"pwd\\\"}\"}}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":6}}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    )
}

async fn sqlite_entry_payloads(path: &Path) -> Vec<String> {
    let db = cooldis_sqlite::Db::open(path, cooldis_sqlite::DbConfig::default())
        .await
        .unwrap();
    let connection = db.connect().await.unwrap();
    let mut rows = connection
        .query(
            "SELECT entry_json FROM session_entries ORDER BY created_at_ms",
            (),
        )
        .await
        .unwrap();
    let mut entries = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        entries.push(row.get::<String>(0).unwrap());
    }
    entries
        .into_iter()
        .filter(|entry| {
            let value: serde_json::Value = serde_json::from_str(entry).unwrap();
            value["kind"]["kind"].as_str() != Some("thread_started")
        })
        .collect()
}

fn temp_db_path(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}.sqlite3"))
}

fn temp_dir_path(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}"))
}

fn remove_sqlite_files(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
}

struct MockHttpServer {
    addr: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    handle: JoinHandle<()>,
}

#[derive(Clone)]
struct MockResponse {
    body: String,
    content_type: &'static str,
}

impl MockResponse {
    fn json(value: Value) -> Self {
        Self {
            body: value.to_string(),
            content_type: "application/json",
        }
    }

    fn text(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            content_type: "text/event-stream",
        }
    }
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    path: String,
    headers: Vec<(String, String)>,
    body: Value,
}

impl CapturedRequest {
    fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
            .map(|(_, value)| value.as_str())
    }
}

impl MockHttpServer {
    async fn start(responses: Vec<Value>) -> Self {
        Self::start_responses(responses.into_iter().map(MockResponse::json).collect()).await
    }

    async fn start_text(responses: Vec<&str>) -> Self {
        Self::start_responses(responses.into_iter().map(MockResponse::text).collect()).await
    }

    async fn start_responses(responses: Vec<MockResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let handle = tokio::spawn(async move {
            let mut responses = responses.into_iter().collect::<VecDeque<_>>();
            while let Some(response) = responses.pop_front() {
                let (socket, _) = listener.accept().await.unwrap();
                let request = handle_connection(socket, &response).await;
                captured_requests.lock().unwrap().push(request);
            }
        });
        Self {
            addr,
            requests,
            handle,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn requests(&self, expected: usize) -> Vec<CapturedRequest> {
        timeout(Duration::from_secs(5), async {
            loop {
                let requests = self.requests.lock().unwrap().clone();
                if requests.len() >= expected {
                    return requests;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for captured requests")
    }

    async fn join(self) {
        self.handle.await.unwrap();
    }
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    response_body: &MockResponse,
) -> CapturedRequest {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "connection closed before HTTP headers");
        buffer.extend_from_slice(&chunk[..read]);
        if header_end(&buffer).is_some() {
            break;
        }
    }

    let header_end = header_end(&buffer).unwrap();
    let headers_text = String::from_utf8(buffer[..header_end].to_vec()).unwrap();
    let mut lines = headers_text.split("\r\n");
    let request_line = lines.next().unwrap();
    let path = request_line.split_whitespace().nth(1).unwrap().to_string();
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_string(), value.trim().to_string()))
        })
        .collect::<Vec<_>>();
    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    let body_start = header_end + 4;
    while buffer.len() - body_start < content_length {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "connection closed before HTTP body");
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = serde_json::from_slice(&buffer[body_start..body_start + content_length]).unwrap();

    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.content_type,
        response_body.body.len(),
        response_body.body
    );
    socket.write_all(response.as_bytes()).await.unwrap();
    socket.shutdown().await.unwrap();

    CapturedRequest {
        path,
        headers,
        body,
    }
}

fn header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}
