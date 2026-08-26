use base64::Engine as _;
use futures_util::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[derive(Clone)]
struct MockRpcReply {
    method: &'static str,
    result: Result<serde_json::Value, &'static str>,
}

fn rpc_ok(method: &'static str, result: serde_json::Value) -> MockRpcReply {
    MockRpcReply {
        method,
        result: Ok(result),
    }
}

fn rpc_err(method: &'static str, message: &'static str) -> MockRpcReply {
    MockRpcReply {
        method,
        result: Err(message),
    }
}

async fn mock_operator_client(
    replies: Vec<MockRpcReply>,
) -> (
    crate::adapters::operator_client::OperatorClient<tokio::io::DuplexStream>,
    std::sync::Arc<std::sync::Mutex<Vec<crate::adapters::app_server::connection::JsonRpcRequest>>>,
    tokio::task::JoinHandle<()>,
) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client_websocket = tokio_tungstenite::WebSocketStream::from_raw_socket(
        client_io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let mut server_websocket = tokio_tungstenite::WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = std::sync::Arc::clone(&requests);
    let task = tokio::spawn(async move {
        let mut replies = std::collections::VecDeque::from(replies);
        while let Some(message) = server_websocket.next().await {
            let message = message.expect("mock app-server websocket read");
            let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
                if message.is_close() {
                    break;
                }
                continue;
            };
            let message = serde_json::from_str::<
                crate::adapters::app_server::connection::JsonRpcMessage,
            >(&text)
            .expect("mock app-server JSON-RPC request");
            let crate::adapters::app_server::connection::JsonRpcMessage::Request(request) = message
            else {
                continue;
            };
            let answer = if request.method == "initialize" {
                crate::adapters::app_server::connection::JsonRpcMessage::Response(
                    crate::adapters::app_server::connection::JsonRpcResponse {
                        id: request.id.clone(),
                        result: serde_json::json!({}),
                    },
                )
            } else {
                captured.lock().unwrap().push(request.clone());
                let reply = replies.pop_front().expect("unexpected JSON-RPC request");
                assert_eq!(request.method, reply.method);
                match reply.result {
                    Ok(result) => {
                        crate::adapters::app_server::connection::JsonRpcMessage::Response(
                            crate::adapters::app_server::connection::JsonRpcResponse {
                                id: request.id.clone(),
                                result,
                            },
                        )
                    }
                    Err(message) => crate::adapters::app_server::connection::JsonRpcMessage::Error(
                        crate::adapters::app_server::connection::JsonRpcError {
                            id: request.id.clone(),
                            error: crate::adapters::app_server::connection::JsonRpcErrorError {
                                code: -32602,
                                message: message.to_string(),
                                data: None,
                            },
                        },
                    ),
                }
            };
            let text = serde_json::to_string(&answer).unwrap();
            server_websocket
                .send(tokio_tungstenite::tungstenite::Message::Text(text.into()))
                .await
                .expect("mock app-server websocket write");
        }
        assert!(replies.is_empty(), "unused mock JSON-RPC replies");
    });
    let client = crate::adapters::operator_client::OperatorClient::connect_with_websocket(
        client_websocket,
        "memory://chat-test",
        crate::adapters::operator_client::OperatorConnectConfig::default(),
    )
    .await
    .unwrap();
    (client, requests, task)
}

async fn drive_actions(
    replies: Vec<MockRpcReply>,
    actions: Vec<verlet_chat::Action>,
) -> (
    Vec<verlet_chat::ChatEvent>,
    Vec<crate::adapters::app_server::connection::JsonRpcRequest>,
) {
    let (mut client, requests, server) = mock_operator_client(replies).await;
    let (action_tx, action_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    for action in actions {
        action_tx.send(action).unwrap();
    }
    drop(action_tx);
    let mut driver = crate::cli::chat::ChatDriver::new("thread-1".to_string(), true).unwrap();
    driver
        .drive(&mut client, action_rx, event_tx)
        .await
        .unwrap();
    let mut events = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }
    client.close().await.unwrap();
    server.await.unwrap();
    let requests = requests.lock().unwrap().clone();
    (events, requests)
}

struct FakeOAuthServer {
    base_url: String,
    requests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

async fn fake_oauth_server(responses: Vec<(u16, serde_json::Value)>) -> FakeOAuthServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = std::sync::Arc::clone(&requests);
    let task = tokio::spawn(async move {
        for (status, body) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            captured.lock().unwrap().push(request);
            let body = serde_json::to_string(&body).unwrap();
            let reason = if status < 300 { "OK" } else { "Bad Request" };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    FakeOAuthServer {
        base_url: format!("http://{address}"),
        requests,
        task,
    }
}

async fn pending_oauth_server() -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    std::sync::Arc<tokio::sync::Notify>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = std::sync::Arc::clone(&requests);
    let shutdown = std::sync::Arc::new(tokio::sync::Notify::new());
    let stop = std::sync::Arc::clone(&shutdown);
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_http_request(&mut socket).await;
        captured.lock().unwrap().push(request);
        let body = serde_json::json!({
            "device_auth_id": "pending-device",
            "user_code": "WAIT-CODE",
            "verification_uri": "https://example.test/device",
            "interval": 0
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        stop.notified().await;
    });
    (format!("http://{address}"), requests, shutdown, task)
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let read = socket.read(&mut buffer).await.unwrap();
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
        let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if bytes.len() >= header_end + 4 + content_length {
            break;
        }
    }
    String::from_utf8(bytes).unwrap()
}

fn oauth_access_token() -> String {
    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("{}");
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::json!({
            "https://api.openai.com/auth": {"chatgpt_account_id": "acct-device"},
            "email": "user@example.com"
        })
        .to_string(),
    );
    format!("{header}.{payload}.signature")
}

fn oauth_token_response(access: &str, refresh: &str) -> serde_json::Value {
    serde_json::json!({
        "access_token": access,
        "refresh_token": refresh,
        "expires_in": 3600
    })
}

async fn recv_event(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<verlet_chat::ChatEvent>,
) -> verlet_chat::ChatEvent {
    tokio::time::timeout(std::time::Duration::from_secs(30), events.recv())
        .await
        .expect("chat event timed out")
        .expect("chat event channel closed")
}

#[test]
fn parse_attach_target_accepts_unix_and_websocket() {
    assert_eq!(
        crate::cli::chat::parse_attach_target("unix:///tmp/sock").expect("unix target"),
        crate::cli::chat::ChatAttachTarget::Unix(std::path::PathBuf::from("/tmp/sock"))
    );
    assert_eq!(
        crate::cli::chat::parse_attach_target("ws://127.0.0.1:7000/rpc").expect("ws target"),
        crate::cli::chat::ChatAttachTarget::WebSocket("ws://127.0.0.1:7000/rpc".to_string())
    );
}

#[test]
fn parse_attach_target_rejects_empty_and_unknown_schemes() {
    assert!(crate::cli::chat::parse_attach_target("unix://").is_err());
    assert!(crate::cli::chat::parse_attach_target("wss://host/rpc").is_err());
    assert!(crate::cli::chat::parse_attach_target("http://host").is_err());
}

fn notification(
    method: &str,
    params: serde_json::Value,
) -> crate::adapters::app_server::connection::JsonRpcNotification {
    crate::adapters::app_server::connection::JsonRpcNotification {
        method: method.to_string(),
        params: Some(params),
    }
}

fn driver() -> crate::cli::chat::ChatDriver {
    let mut driver = crate::cli::chat::ChatDriver::new("thread-1".to_string(), true).unwrap();
    driver.active_turn_id = Some("turn-1".to_string());
    driver
}

#[test]
fn projects_answer_and_thinking_deltas_for_the_active_turn() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "item/agentMessage/delta",
        serde_json::json!({"threadId": "thread-1", "turnId": "turn-1", "delta": "hi"}),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::AnswerDelta("hi".into())]
    );

    let events = driver.project_notification(&notification(
        "item/agentThinking/delta",
        serde_json::json!({"threadId": "thread-1", "turnId": "turn-1", "delta": "hm"}),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ThinkingDelta("hm".into())]
    );

    // Another thread's stream must not leak into this transcript.
    let events = driver.project_notification(&notification(
        "item/agentMessage/delta",
        serde_json::json!({"threadId": "thread-2", "turnId": "turn-9", "delta": "no"}),
    ));
    assert!(events.is_empty());

    let events = driver.project_notification(&notification(
        "item/agentMessage/delta",
        serde_json::json!({"threadId": "thread-1", "turnId": "turn-1", "delta": ""}),
    ));
    assert!(
        events.is_empty(),
        "empty deltas must not open transcript cells"
    );
}

#[test]
fn projects_tool_call_lifecycle() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "item/started",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {
                "type": "dynamicToolCall",
                "id": "call-1",
                "tool": "web_search",
                "arguments": {"query": "verlet"},
            },
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ToolStarted {
            id: "call-1".into(),
            title: "web_search {\"query\":\"verlet\"}".into(),
        }]
    );

    let events = driver.project_notification(&notification(
        "item/completed",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {
                "type": "dynamicToolCall",
                "id": "call-1",
                "success": false,
                "contentItems": [{"type": "inputText", "text": "boom"}],
            },
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ToolCompleted {
            id: "call-1".into(),
            success: false,
            output: "boom".into(),
        }]
    );
}

#[test]
fn projects_command_execution_output_and_exit() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "item/commandExecution/outputDelta",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "exec-1",
            "delta": "line\n",
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ToolOutputDelta {
            id: "exec-1".into(),
            delta: "line\n".into(),
        }]
    );

    let events = driver.project_notification(&notification(
        "item/completed",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {
                "type": "commandExecution",
                "id": "exec-1",
                "exitCode": 2,
                "aggregatedOutput": "boom",
            },
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ToolCompleted {
            id: "exec-1".into(),
            success: false,
            output: "boom".into(),
        }]
    );
}

#[test]
fn ignores_tool_and_error_events_outside_the_active_turn() {
    let mut driver = driver();
    for (method, params) in [
        (
            "item/started",
            serde_json::json!({
                "threadId": "thread-1",
                "turnId": "turn-2",
                "item": {"type": "dynamicToolCall", "id": "stale", "tool": "read"},
            }),
        ),
        (
            "item/commandExecution/outputDelta",
            serde_json::json!({
                "threadId": "thread-1",
                "turnId": "turn-2",
                "itemId": "stale",
                "delta": "must not leak",
            }),
        ),
        (
            "item/completed",
            serde_json::json!({
                "threadId": "thread-1",
                "turnId": "turn-2",
                "item": {"type": "dynamicToolCall", "id": "stale", "success": true},
            }),
        ),
        (
            "error",
            serde_json::json!({
                "threadId": "thread-1",
                "turnId": "turn-2",
                "error": {"message": "stale failure"},
            }),
        ),
    ] {
        assert!(
            driver
                .project_notification(&notification(method, params))
                .is_empty(),
            "{method} from another turn must be ignored"
        );
    }
    assert_eq!(driver.active_turn_id.as_deref(), Some("turn-1"));

    let events = driver.project_notification(&notification(
        "turn/completed",
        serde_json::json!({
            "threadId": "thread-2",
            "turn": {"id": "turn-1", "status": "completed"},
        }),
    ));
    assert!(
        events.is_empty(),
        "another thread cannot complete this turn"
    );
    assert_eq!(driver.active_turn_id.as_deref(), Some("turn-1"));
}

#[test]
fn projects_error_only_for_the_active_thread_and_turn() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "error",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "error": {"message": "provider failed"},
        }),
    ));
    assert_eq!(
        events,
        vec![
            verlet_chat::ChatEvent::Error {
                title: "app-server error: provider failed".into(),
                body: Vec::new(),
            },
            verlet_chat::ChatEvent::TurnCompleted { error: None },
        ]
    );
    assert!(driver.active_turn_id.is_none());
}

#[test]
fn projects_resync_failure_for_the_current_thread_and_ends_the_turn() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "thread/resync/failed",
        serde_json::json!({
            "threadId": "thread-1",
            "reason": "broadcastLag",
            "laggedEvents": 8,
            "error": {"code": "resync_failed", "message": "durable history unavailable"},
        }),
    ));
    assert_eq!(
        events,
        vec![
            verlet_chat::ChatEvent::Error {
                title: "stream resync failed: durable history unavailable".into(),
                body: vec![
                    "the live subscription stopped; the transcript may be incomplete".into()
                ],
            },
            verlet_chat::ChatEvent::TurnCompleted { error: None },
        ]
    );
    assert!(driver.active_turn_id.is_none());

    assert!(
        driver
            .project_notification(&notification(
                "thread/resync/failed",
                serde_json::json!({
                    "threadId": "thread-2",
                    "error": {"message": "other thread"},
                }),
            ))
            .is_empty()
    );
}

#[test]
fn turn_completed_clears_the_active_turn_and_reports_errors() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "turn/completed",
        serde_json::json!({
            "threadId": "thread-1",
            "turn": {"id": "turn-1", "status": "failed", "error": {"message": "model unavailable"}},
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::TurnCompleted {
            error: Some("model unavailable".into()),
        }]
    );
    assert!(driver.active_turn_id.is_none());

    // Once idle, an unsolicited turn/started is adopted.
    let events = driver.project_notification(&notification(
        "turn/started",
        serde_json::json!({"threadId": "thread-1", "turn": {"id": "turn-2"}}),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::TurnStarted {
            turn_id: "turn-2".into(),
        }]
    );
    assert_eq!(driver.active_turn_id.as_deref(), Some("turn-2"));
}

#[test]
fn session_rows_mark_the_current_thread() {
    let rows = crate::cli::chat::session_rows(
        &serde_json::json!({
            "data": [
                {"id": "thread-1", "name": "alpha", "status": {"type": "idle"}, "preview": "  hello   world  "},
                {"id": "thread-2", "name": "", "status": {"type": "running"}},
            ],
        }),
        "thread-1",
    );
    assert_eq!(rows.len(), 2);
    assert!(rows[0].current);
    assert_eq!(rows[0].preview, "hello world");
    assert_eq!(rows[1].name, "unnamed");
    assert!(!rows[1].current);
}

#[test]
fn model_rows_preserve_coordinates_auth_and_active_selection() {
    let rows = crate::cli::chat::model_rows(&crate::adapters::operator_client::OperatorModelList {
        data: vec![crate::adapters::operator_client::OperatorModel {
            provider_id: "provider-id".to_string(),
            model: "server-model".to_string(),
            display_name: "Server Model".to_string(),
            auth_status: crate::adapters::operator_client::OperatorModelAuthStatus::Missing,
            active: true,
        }],
        next_cursor: None,
    });

    assert_eq!(
        rows,
        vec![verlet_chat::ModelRow {
            provider_id: "provider-id".to_string(),
            model: "server-model".to_string(),
            display_name: "Server Model".to_string(),
            auth_status: "missing".to_string(),
            active: true,
        }]
    );
}

#[test]
fn catalog_provider_rows_map_the_typed_rpc_response() {
    let catalog = serde_json::json!({
        "providers": [
            {
                "providerId": "anthropic",
                "displayName": "Anthropic",
                "baseUrl": "https://api.anthropic.com",
                "api": "anthropic_messages",
                "authKind": "api_key",
                "envVars": ["ANTHROPIC_API_KEY"],
                "configured": true,
                "authSource": "stored",
                "authLabel": "stored credential",
                "custom": false,
                "active": true,
                "modelCount": 4,
                "defaultModel": "claude-best"
            },
            {
                "providerId": "my-llm",
                "displayName": "My LLM",
                "baseUrl": "https://llm.example/v1",
                "api": "open_ai_chat_completions",
                "authKind": "api_key",
                "configured": false,
                "authLabel": null,
                "custom": true,
                "active": false,
                "modelCount": 0,
                "defaultModel": null
            }
        ],
        "nextCursor": null
    });
    let catalog = serde_json::from_value::<
        crate::adapters::operator_client::OperatorModelProviderCatalog,
    >(catalog)
    .expect("catalog response must deserialize into the typed client struct");
    assert_eq!(
        crate::cli::chat::catalog_provider_rows(&catalog),
        vec![
            verlet_chat::CatalogProviderRow {
                provider_id: "anthropic".to_string(),
                display_name: "Anthropic".to_string(),
                base_url: "https://api.anthropic.com".to_string(),
                api: "anthropic_messages".to_string(),
                auth_kind: "api_key".to_string(),
                env_vars: vec!["ANTHROPIC_API_KEY".to_string()],
                configured: true,
                auth_label: "stored credential".to_string(),
                custom: false,
                active: true,
                model_count: 4,
                default_model: Some("claude-best".to_string()),
            },
            verlet_chat::CatalogProviderRow {
                provider_id: "my-llm".to_string(),
                display_name: "My LLM".to_string(),
                base_url: "https://llm.example/v1".to_string(),
                api: "openai_chat_completions".to_string(),
                auth_kind: "api_key".to_string(),
                env_vars: Vec::new(),
                configured: false,
                auth_label: String::new(),
                custom: true,
                active: false,
                model_count: 0,
                default_model: None,
            },
        ]
    );
}

#[test]
fn custom_provider_upsert_params_carry_no_key_and_mark_the_default_model() {
    let params =
        crate::cli::chat::custom_provider_upsert_params(&verlet_chat::CustomProviderSpec {
            provider_id: "my-llm".to_string(),
            display_name: "My LLM".to_string(),
            api: "anthropic_messages".to_string(),
            base_url: "https://llm.example".to_string(),
            header: Some(("X-Team".to_string(), "research".to_string())),
            models: vec!["model-one".to_string(), "model-two".to_string()],
            keyless: false,
        });
    assert_eq!(
        serde_json::to_value(&params).unwrap(),
        serde_json::json!({
            "provider": {
                "providerId": "my-llm",
                "api": "anthropic_messages",
                "baseUrl": "https://llm.example",
                "displayName": "My LLM",
                "auth": { "type": "stored_or_environment" },
                "authHeader": true,
                "headers": { "X-Team": { "type": "literal", "value": "research" } },
                "models": [
                    { "modelId": "model-one", "metadata": { "default": "true" } },
                    { "modelId": "model-two" }
                ],
                "metadata": { "origin": "custom" },
            }
        })
    );
}

#[test]
fn keyless_custom_provider_upsert_params_declare_auth_none_without_auth_header() {
    let params =
        crate::cli::chat::custom_provider_upsert_params(&verlet_chat::CustomProviderSpec {
            provider_id: "local-llm".to_string(),
            display_name: "Local LLM".to_string(),
            api: "openai_chat_completions".to_string(),
            base_url: "http://127.0.0.1:11434/v1".to_string(),
            header: None,
            models: vec!["llama-local".to_string()],
            keyless: true,
        });
    assert_eq!(
        serde_json::to_value(&params).unwrap(),
        serde_json::json!({
            "provider": {
                "providerId": "local-llm",
                "api": "open_ai_chat_completions",
                "baseUrl": "http://127.0.0.1:11434/v1",
                "displayName": "Local LLM",
                "auth": { "type": "none" },
                "authHeader": false,
                "headers": {},
                "models": [
                    { "modelId": "llama-local", "metadata": { "default": "true" } }
                ],
                "metadata": { "origin": "custom" },
            }
        })
    );
}

#[tokio::test]
async fn fetch_provider_catalog_action_maps_the_catalog_rpc() {
    let (events, requests) = drive_actions(
        vec![rpc_ok(
            "modelProvider/catalog",
            serde_json::json!({
                "providers": [{
                    "providerId": "openai-codex",
                    "displayName": "OpenAI Codex",
                    "baseUrl": "https://chatgpt.com/backend-api/codex/responses",
                    "api": "open_ai_responses",
                    "authKind": "oauth",
                    "configured": false,
                    "authLabel": null,
                    "custom": false,
                    "active": false,
                    "modelCount": 1,
                    "defaultModel": "gpt-5.6-codex"
                }],
                "nextCursor": null
            }),
        )],
        vec![verlet_chat::Action::FetchProviderCatalog],
    )
    .await;

    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ProviderCatalog {
            providers: vec![verlet_chat::CatalogProviderRow {
                provider_id: "openai-codex".to_string(),
                display_name: "OpenAI Codex".to_string(),
                base_url: "https://chatgpt.com/backend-api/codex/responses".to_string(),
                api: "openai_responses".to_string(),
                auth_kind: "oauth".to_string(),
                env_vars: Vec::new(),
                configured: false,
                auth_label: String::new(),
                custom: false,
                active: false,
                model_count: 1,
                default_model: Some("gpt-5.6-codex".to_string()),
            }],
        }]
    );
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        ["modelProvider/catalog"]
    );
}

#[tokio::test]
async fn upsert_custom_provider_sends_one_upsert_and_reports_success() {
    let spec = verlet_chat::CustomProviderSpec {
        provider_id: "local-llm".to_string(),
        display_name: "Local LLM".to_string(),
        api: "openai_chat_completions".to_string(),
        base_url: "http://127.0.0.1:11434/v1".to_string(),
        header: None,
        models: vec!["llama-local".to_string()],
        keyless: true,
    };
    let expected_params =
        serde_json::to_value(crate::cli::chat::custom_provider_upsert_params(&spec)).unwrap();
    let (events, requests) = drive_actions(
        vec![rpc_ok(
            "modelProvider/upsert",
            serde_json::json!({ "provider": { "providerId": "local-llm" } }),
        )],
        vec![verlet_chat::Action::UpsertCustomProvider { spec }],
    )
    .await;

    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::CustomProviderResult {
            provider_id: "local-llm".to_string(),
            error: None,
        }]
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "modelProvider/upsert");
    assert_eq!(requests[0].params.as_ref().unwrap(), &expected_params);
}

#[tokio::test]
async fn upsert_custom_provider_rpc_error_maps_into_the_result_event() {
    let spec = verlet_chat::CustomProviderSpec {
        provider_id: "my-llm".to_string(),
        display_name: "My LLM".to_string(),
        api: "openai_chat_completions".to_string(),
        base_url: "https://llm.example/v1".to_string(),
        header: None,
        models: vec!["model-one".to_string()],
        keyless: false,
    };
    let (events, requests) = drive_actions(
        vec![rpc_err(
            "modelProvider/upsert",
            "cannot update active model provider \"my-llm\"; select a different provider first",
        )],
        vec![verlet_chat::Action::UpsertCustomProvider { spec }],
    )
    .await;

    // The RPC refusal lands in the result event, never as a transport error.
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::CustomProviderResult {
            provider_id: "my-llm".to_string(),
            error: Some(
                "request `modelProvider/upsert` was refused: cannot update active model provider \"my-llm\"; select a different provider first"
                    .to_string(),
            ),
        }]
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn upsert_rpc_error_text_passes_through_verbatim() {
    // The driver only redacts values it submitted itself (SetProviderKey);
    // upsert carries no secrets, so server text passes through untouched and
    // the UI stays responsible for redacting anything the user typed.
    let spec = verlet_chat::CustomProviderSpec {
        provider_id: "my-llm".to_string(),
        display_name: "My LLM".to_string(),
        api: "openai_chat_completions".to_string(),
        base_url: "https://llm.example/v1".to_string(),
        header: None,
        models: vec!["model-one".to_string()],
        keyless: false,
    };
    let (events, _) = drive_actions(
        vec![rpc_err(
            "modelProvider/upsert",
            "header value sk-fixture-lookalike-value was rejected",
        )],
        vec![verlet_chat::Action::UpsertCustomProvider { spec }],
    )
    .await;

    let verlet_chat::ChatEvent::CustomProviderResult {
        error: Some(error), ..
    } = &events[0]
    else {
        panic!("expected an upsert error result, got {events:?}");
    };
    assert!(error.contains("sk-fixture-lookalike-value"), "{error}");
    assert!(!error.contains("[redacted]"), "{error}");
}

#[tokio::test]
async fn delete_custom_provider_sends_one_delete_and_reports_success() {
    let (events, requests) = drive_actions(
        vec![rpc_ok(
            "modelProvider/delete",
            serde_json::json!({ "deleted": true, "providerId": "my-llm" }),
        )],
        vec![verlet_chat::Action::DeleteCustomProvider {
            provider_id: "my-llm".to_string(),
        }],
    )
    .await;

    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::CustomProviderResult {
            provider_id: "my-llm".to_string(),
            error: None,
        }]
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "modelProvider/delete");
    assert_eq!(
        requests[0].params.as_ref().unwrap(),
        &serde_json::json!({ "providerId": "my-llm" })
    );
}

#[tokio::test]
async fn delete_custom_provider_rpc_error_maps_into_the_result_event() {
    let (events, requests) = drive_actions(
        vec![rpc_err(
            "modelProvider/delete",
            "model provider \"my-llm\" was not found",
        )],
        vec![verlet_chat::Action::DeleteCustomProvider {
            provider_id: "my-llm".to_string(),
        }],
    )
    .await;

    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::CustomProviderResult {
            provider_id: "my-llm".to_string(),
            error: Some(
                "request `modelProvider/delete` was refused: model provider \"my-llm\" was not found"
                    .to_string(),
            ),
        }]
    );
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn set_provider_key_maps_success_to_credential_result() {
    let (events, requests) = drive_actions(
        vec![rpc_ok(
            "modelProvider/auth/set",
            serde_json::json!({
                "auth": {
                    "providerId": "anthropic",
                    "displayName": "Anthropic",
                    "configured": true,
                    "source": "stored",
                    "label": "stored credential"
                }
            }),
        )],
        vec![verlet_chat::Action::SetProviderKey {
            provider_id: "anthropic".to_string(),
            api_key: "paste-secret".to_string(),
        }],
    )
    .await;

    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::CredentialResult {
            provider_id: "anthropic".to_string(),
            error: None,
        }]
    );
    assert_eq!(requests[0].method, "modelProvider/auth/set");
    assert_eq!(
        requests[0].params.as_ref().unwrap()["apiKey"],
        "paste-secret"
    );
}

#[tokio::test]
async fn set_provider_key_maps_rpc_error_without_echoing_the_key() {
    let (events, _) = drive_actions(
        vec![rpc_err(
            "modelProvider/auth/set",
            "credential paste-secret rejected by policy",
        )],
        vec![verlet_chat::Action::SetProviderKey {
            provider_id: "anthropic".to_string(),
            api_key: "paste-secret".to_string(),
        }],
    )
    .await;

    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::CredentialResult {
            provider_id: "anthropic".to_string(),
            error: Some(
                "request `modelProvider/auth/set` was refused: credential [redacted] rejected by policy"
                    .to_string(),
            ),
        }]
    );
    assert!(!format!("{events:?}").contains("paste-secret"));
}

#[test]
fn oauth_rpc_error_redaction_removes_access_and_refresh_values() {
    assert_eq!(
        crate::cli::chat::redact_secret_values(
            "server echoed access-secret and refresh-secret".to_string(),
            [&"access-secret".to_string(), &"refresh-secret".to_string()],
        ),
        "server echoed [redacted] and [redacted]"
    );
}

#[tokio::test]
async fn pending_login_aborts_its_task_when_dropped() {
    struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for NotifyDrop {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _notify = NotifyDrop(Some(dropped_tx));
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    });
    started_rx.await.unwrap();
    drop(crate::cli::chat::PendingLogin { id: 1, task });
    tokio::time::timeout(std::time::Duration::from_secs(30), dropped_rx)
        .await
        .expect("aborted login task did not drop")
        .unwrap();
}

#[test]
fn provider_auth_display_name_defaults_for_older_servers() {
    let auth =
        serde_json::from_value::<crate::adapters::operator_client::OperatorModelProviderAuth>(
            serde_json::json!({
                "providerId": "older-provider",
                "configured": false
            }),
        )
        .unwrap();
    assert_eq!(auth.display_name, "");
}

#[tokio::test]
async fn clear_credential_calls_delete_and_reports_success() {
    let (events, requests) = drive_actions(
        vec![rpc_ok(
            "modelProvider/auth/delete",
            serde_json::json!({
                "auth": {
                    "providerId": "anthropic",
                    "configured": false,
                    "source": null,
                    "label": null
                }
            }),
        )],
        vec![verlet_chat::Action::ClearCredential {
            provider_id: "anthropic".to_string(),
        }],
    )
    .await;
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::CredentialCleared {
            provider_id: "anthropic".to_string()
        }]
    );
    assert_eq!(requests[0].method, "modelProvider/auth/delete");
}

#[tokio::test]
async fn device_login_emits_code_then_sends_oauth_credential_over_rpc() {
    let access_token = oauth_access_token();
    let oauth = fake_oauth_server(vec![
        (
            200,
            serde_json::json!({
                "device_auth_id": "device-123",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://example.test/device",
                "interval": 0
            }),
        ),
        (
            200,
            serde_json::json!({
                "authorization_code": "device-code",
                "code_verifier": "device-verifier"
            }),
        ),
        (200, oauth_token_response(&access_token, "refresh-secret")),
    ])
    .await;
    let (mut client, requests, server) = mock_operator_client(vec![rpc_ok(
        "modelProvider/auth/setOAuth",
        serde_json::json!({
            "auth": {
                "providerId": "openai-codex",
                "displayName": "OpenAI Codex",
                "configured": true,
                "source": "stored",
                "label": "signed in"
            }
        }),
    )])
    .await;
    let (action_tx, action_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut driver = crate::cli::chat::ChatDriver::new("thread-1".to_string(), true).unwrap();
    driver.oauth_client =
        crate::openai_codex::OpenAICodexOAuthClient::with_test_endpoints(&oauth.base_url).unwrap();

    let driven = driver.drive(&mut client, action_rx, event_tx);
    let interaction = async move {
        action_tx
            .send(verlet_chat::Action::StartLogin {
                provider_id: "openai-codex".to_string(),
                method: verlet_chat::LoginMethod::Device,
            })
            .unwrap();
        let first = recv_event(&mut event_rx).await;
        let second = recv_event(&mut event_rx).await;
        drop(action_tx);
        (first, second)
    };
    let (drive_result, (first, second)) = tokio::join!(driven, interaction);
    drive_result.unwrap();
    assert_eq!(
        first,
        verlet_chat::ChatEvent::LoginDeviceCode {
            verification_uri: "https://example.test/device".to_string(),
            user_code: "ABCD-EFGH".to_string(),
        }
    );
    assert_eq!(
        second,
        verlet_chat::ChatEvent::CredentialResult {
            provider_id: "openai-codex".to_string(),
            error: None,
        }
    );
    let rendered_events = format!("{first:?}{second:?}");
    assert!(!rendered_events.contains(&access_token));
    assert!(!rendered_events.contains("refresh-secret"));

    client.close().await.unwrap();
    server.await.unwrap();
    oauth.task.await.unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "modelProvider/auth/setOAuth");
    let params = requests[0].params.as_ref().unwrap();
    assert_eq!(params["access"], access_token);
    assert_eq!(params["refresh"], "refresh-secret");
    assert_eq!(oauth.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn second_start_login_while_pending_reports_error_without_spawning() {
    let (base_url, http_requests, shutdown, oauth_task) = pending_oauth_server().await;
    let (mut client, _, server) = mock_operator_client(Vec::new()).await;
    let (action_tx, action_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut driver = crate::cli::chat::ChatDriver::new("thread-1".to_string(), true).unwrap();
    driver.oauth_client =
        crate::openai_codex::OpenAICodexOAuthClient::with_test_endpoints(&base_url).unwrap();

    let driven = driver.drive(&mut client, action_rx, event_tx);
    let interaction = async move {
        action_tx
            .send(verlet_chat::Action::StartLogin {
                provider_id: "openai-codex".to_string(),
                method: verlet_chat::LoginMethod::Device,
            })
            .unwrap();
        assert!(matches!(
            recv_event(&mut event_rx).await,
            verlet_chat::ChatEvent::LoginDeviceCode { .. }
        ));
        action_tx
            .send(verlet_chat::Action::StartLogin {
                provider_id: "openai-codex".to_string(),
                method: verlet_chat::LoginMethod::Device,
            })
            .unwrap();
        let answer = recv_event(&mut event_rx).await;
        action_tx.send(verlet_chat::Action::CancelLogin).unwrap();
        drop(action_tx);
        answer
    };
    let (drive_result, answer) = tokio::join!(driven, interaction);
    drive_result.unwrap();
    assert_eq!(
        answer,
        verlet_chat::ChatEvent::CredentialResult {
            provider_id: "openai-codex".to_string(),
            error: Some("a sign-in is already in progress".to_string()),
        }
    );
    assert_eq!(driver.next_login_id, 1);
    assert!(driver.pending_login.is_none());
    assert_eq!(http_requests.lock().unwrap().len(), 1);

    shutdown.notify_one();
    oauth_task.await.unwrap();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn stale_login_completion_after_cancel_is_ignored() {
    let (mut client, requests, server) = mock_operator_client(Vec::new()).await;
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (login_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let mut driver = crate::cli::chat::ChatDriver::new("thread-1".to_string(), true).unwrap();
    driver.pending_login = Some(crate::cli::chat::PendingLogin {
        id: 7,
        task: tokio::spawn(std::future::pending()),
    });
    driver
        .execute(
            &mut client,
            &event_tx,
            &login_tx,
            verlet_chat::Action::CancelLogin,
        )
        .await
        .unwrap();
    driver
        .apply_login_event(
            &mut client,
            &event_tx,
            crate::cli::chat::LoginTaskEvent::Finished {
                id: 7,
                provider_id: "openai-codex".to_string(),
                result: Ok(
                    verlet_metadata::provider_store::LlmProviderCredential::OAuth {
                        access: "stale-access".to_string(),
                        refresh: "stale-refresh".to_string(),
                        expires_at_ms: 123,
                        account_id: None,
                        email: None,
                    },
                ),
            },
        )
        .await;
    assert!(event_rx.try_recv().is_err());
    assert!(requests.lock().unwrap().is_empty());

    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn set_provider_key_while_login_pending_is_rejected_without_rpc() {
    let (mut client, requests, server) = mock_operator_client(Vec::new()).await;
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (login_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let mut driver = crate::cli::chat::ChatDriver::new("thread-1".to_string(), true).unwrap();
    driver.pending_login = Some(crate::cli::chat::PendingLogin {
        id: 11,
        task: tokio::spawn(std::future::pending()),
    });

    driver
        .execute(
            &mut client,
            &event_tx,
            &login_tx,
            verlet_chat::Action::SetProviderKey {
                provider_id: "anthropic".to_string(),
                api_key: "must-not-send".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        event_rx.try_recv().unwrap(),
        verlet_chat::ChatEvent::CredentialResult {
            provider_id: "anthropic".to_string(),
            error: Some("a sign-in is already in progress".to_string()),
        }
    );
    assert!(requests.lock().unwrap().is_empty());

    driver.abort_pending_login();
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn clear_credential_rpc_failure_is_a_transcript_error() {
    let (events, _) = drive_actions(
        vec![rpc_err(
            "modelProvider/auth/delete",
            "cannot clear the active OAuth provider",
        )],
        vec![verlet_chat::Action::ClearCredential {
            provider_id: "openai-codex".to_string(),
        }],
    )
    .await;
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::Error {
            title: "request `modelProvider/auth/delete` was refused: cannot clear the active OAuth provider"
                .to_string(),
            body: Vec::new(),
        }]
    );
}

#[tokio::test]
async fn bootstrap_without_configured_providers_emits_the_first_run_gate() {
    let (mut client, requests, server) = mock_operator_client(vec![
        rpc_ok("account/read", serde_json::json!({})),
        rpc_ok(
            "config/read",
            serde_json::json!({"config": {"cwd": "/tmp/work"}}),
        ),
        // EMO-575 hides the offline echo launch pair: a fresh install
        // reports no model rows at all.
        rpc_ok(
            "model/list",
            serde_json::json!({ "data": [], "nextCursor": null }),
        ),
        rpc_ok(
            "modelProvider/auth/status",
            serde_json::json!({
                "auth": null,
                "data": [{
                    "providerId": "openai-codex",
                    "displayName": "OpenAI Codex",
                    "configured": false,
                    "source": null,
                    "label": null
                }],
                "nextCursor": null
            }),
        ),
    ])
    .await;

    let session =
        crate::cli::chat::bootstrap_chat_client(&mut client, "attach ws://test".to_string())
            .await
            .unwrap();
    assert_eq!(
        session.initial_events,
        vec![verlet_chat::ChatEvent::NoConfiguredProviders]
    );
    assert_eq!(session.model_label, "local/echo");
    assert_eq!(requests.lock().unwrap().len(), 4);

    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn bootstrap_with_a_configured_provider_skips_the_first_run_gate() {
    let (mut client, requests, server) = mock_operator_client(vec![
        rpc_ok("account/read", serde_json::json!({})),
        rpc_ok(
            "config/read",
            serde_json::json!({"config": {"cwd": "/tmp/work"}}),
        ),
        rpc_ok(
            "model/list",
            serde_json::json!({
                "data": [{
                    "providerId": "anthropic",
                    "model": "claude",
                    "displayName": "Claude",
                    "authStatus": "configured",
                    "active": true
                }],
                "nextCursor": null
            }),
        ),
    ])
    .await;

    let session =
        crate::cli::chat::bootstrap_chat_client(&mut client, "attach ws://test".to_string())
            .await
            .unwrap();
    assert_eq!(session.initial_events, Vec::new());
    assert_eq!(session.model_label, "anthropic/claude");
    assert_eq!(requests.lock().unwrap().len(), 3);

    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn kit_actions_on_attached_sessions_explain_instead_of_installing() {
    let (mut client, requests, server) = mock_operator_client(Vec::new()).await;
    let (action_tx, action_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    for action in [
        // The first-run offer stays silent; an explicit open explains.
        verlet_chat::Action::FetchKitStatus {
            intent: verlet_chat::KitStatusIntent::OfferIfMissing,
        },
        verlet_chat::Action::FetchKitStatus {
            intent: verlet_chat::KitStatusIntent::Open,
        },
        verlet_chat::Action::InstallKit {
            name: "pi".to_string(),
            source: "dist/pi-kit".to_string(),
        },
    ] {
        action_tx.send(action).unwrap();
    }
    drop(action_tx);
    let mut driver = crate::cli::chat::ChatDriver::new("thread-1".to_string(), false).unwrap();
    driver
        .drive(&mut client, action_rx, event_tx)
        .await
        .unwrap();

    let mut events = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }
    assert_eq!(events.len(), 2, "events: {events:?}");
    let verlet_chat::ChatEvent::Error { title, .. } = &events[0] else {
        panic!("expected an error event, got {:?}", events[0]);
    };
    assert_eq!(title, "kit install needs the instance host");
    let verlet_chat::ChatEvent::KitInstallResult { name, error, .. } = &events[1] else {
        panic!("expected an install result, got {:?}", events[1]);
    };
    assert_eq!(name, "pi");
    assert!(
        error
            .as_deref()
            .is_some_and(|message| message.contains("instance host")),
        "error: {error:?}"
    );
    // Kit actions never touch the app-server connection.
    assert!(requests.lock().unwrap().is_empty());

    client.close().await.unwrap();
    server.await.unwrap();
}
