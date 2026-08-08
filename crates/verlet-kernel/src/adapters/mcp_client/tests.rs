use crate::agent::agent_tool_router::AgentKernelToolProvider as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

#[tokio::test]
async fn mcp_stdio_provider_imports_and_invokes_tool() {
    let provider =
        crate::adapters::mcp_client::McpStdioToolProvider::connect(
            crate::adapters::mcp_client::McpStdioServerConfig::new("test-echo", "python3")
                .with_args(["-u", "-c", ECHO_SERVER]),
        )
        .await
        .expect("mcp provider should connect");
    let router = crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
        verlet_operations::operation_registry::OperationRegistry::new(),
    ))
    .with_kernel_tool_provider(std::sync::Arc::new(provider));

    let tools = router.tool_definitions().await;
    assert!(tools.iter().any(|tool| tool.name == "verlet_mcp_echo"));

    let result = router
        .invoke_tool_call(
            "call_1",
            "verlet_mcp_echo",
            serde_json::json!({"message": "hello from test"}),
        )
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if content.iter().any(|item| matches!(
            item,
            verlet_history::CanonicalContent::Text { text, .. }
                if text.contains("VERLET_MCP_ECHO_OK hello from test")
        ))
    ));
}

#[test]
fn sqlite_remote_mcp_registry_persists_and_redacts_source_records() {
    let registry = crate::adapters::mcp_client::SqliteMcpSourceRegistry::in_memory().unwrap();
    let config = crate::adapters::mcp_client::McpRemoteServerConfig::new(
        "arcade",
        crate::adapters::mcp_client::McpRemoteTransport::StreamableHttp,
        "https://example.com/mcp",
    )
    .unwrap()
    .with_bearer_secret("arcade.api_key")
    .unwrap()
    .with_header("x-provider", "fixture");

    let record = registry.upsert_source(config).unwrap();
    assert_eq!(record.name, "arcade");
    assert_eq!(record.bearer_secret.as_deref(), Some("arcade.api_key"));
    assert_eq!(registry.list_sources().unwrap().len(), 1);

    let redacted = record.redacted_json().to_string();
    assert!(redacted.contains("arcade.api_key"));
    assert!(redacted.contains("\"redacted\":true"));
    assert!(!redacted.contains("fixture"));
}

#[test]
fn sqlite_remote_mcp_registry_sync_boundary_is_reentrant_from_futures_executor() {
    futures_executor::block_on(async {
        let registry = crate::adapters::mcp_client::SqliteMcpSourceRegistry::in_memory().unwrap();
        registry
            .upsert_source(
                crate::adapters::mcp_client::McpRemoteServerConfig::new(
                    "nested-executor",
                    crate::adapters::mcp_client::McpRemoteTransport::StreamableHttp,
                    "https://nested.example.invalid/mcp",
                )
                .unwrap(),
            )
            .unwrap();

        assert!(registry.get_source("nested-executor").unwrap().is_some());
    });
}

#[tokio::test]
async fn remote_mcp_provider_fails_closed_when_bearer_secret_is_missing() {
    let config = crate::adapters::mcp_client::McpRemoteServerConfig::new(
        "missing-secret",
        crate::adapters::mcp_client::McpRemoteTransport::StreamableHttp,
        "http://127.0.0.1:9/mcp",
    )
    .unwrap()
    .with_bearer_secret("MISSING_MCP_TOKEN")
    .unwrap();
    let err = match crate::adapters::mcp_client::McpRemoteToolProvider::connect(
        config,
        Some(std::sync::Arc::new(
            verlet_metadata::secret_store::SqliteSecretStore::in_memory()
                .await
                .unwrap(),
        )),
    )
    .await
    {
        Ok(_) => panic!("remote MCP provider should fail when bearer secret is missing"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("bearer secret"));
    assert!(err.contains("not available"));
}

#[tokio::test]
async fn remote_mcp_provider_discovers_and_invokes_streamable_http_tool() {
    let seen_authorization = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (url, server) =
        spawn_mcp_http_fixture("application/json", seen_authorization.clone()).await;
    let secret_store = verlet_metadata::secret_store::SqliteSecretStore::in_memory()
        .await
        .unwrap();
    secret_store
        .set_secret(
            "ARCADE_API_KEY",
            "fixture-token",
            verlet_metadata::secret_store::SecretSourceKind::Local,
            None,
        )
        .await
        .unwrap();
    let config = crate::adapters::mcp_client::McpRemoteServerConfig::new(
        "arcade",
        crate::adapters::mcp_client::McpRemoteTransport::StreamableHttp,
        url,
    )
    .unwrap()
    .with_bearer_secret("ARCADE_API_KEY")
    .unwrap()
    .with_include_tools(["remote_echo"]);

    let provider = crate::adapters::mcp_client::McpRemoteToolProvider::connect(
        config,
        Some(std::sync::Arc::new(secret_store)),
    )
    .await
    .unwrap();
    let router = crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
        verlet_operations::operation_registry::OperationRegistry::new(),
    ))
    .with_kernel_tool_provider(std::sync::Arc::new(provider));

    assert_eq!(router.tool_definitions().await[0].name, "remote_echo");
    let result = router
        .invoke_tool_call(
            "call_remote",
            "remote_echo",
            serde_json::json!({"message": "hello"}),
        )
        .await;
    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if content.iter().any(|item| matches!(
            item,
            verlet_history::CanonicalContent::Text { text, .. }
                if text.contains("REMOTE_MCP_OK hello")
        ))
    ));
    server.await.unwrap();
    let observed = seen_authorization.lock().unwrap();
    assert_eq!(observed.as_slice(), ["Bearer fixture-token"; 4]);
}

#[tokio::test]
async fn remote_mcp_provider_accepts_sse_response_bodies() {
    let seen_authorization = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (url, server) = spawn_mcp_http_fixture("text/event-stream", seen_authorization).await;
    let config = crate::adapters::mcp_client::McpRemoteServerConfig::new(
        "sse",
        crate::adapters::mcp_client::McpRemoteTransport::HttpSse,
        url,
    )
    .unwrap();

    let provider = crate::adapters::mcp_client::McpRemoteToolProvider::connect(config, None)
        .await
        .unwrap();
    let result = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_sse".to_string(),
            tool_name: "remote_echo".to_string(),
            arguments: serde_json::json!({"message": "stream"}),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if content.iter().any(|item| matches!(
            item,
            verlet_history::CanonicalContent::Text { text, .. }
                if text.contains("REMOTE_MCP_OK stream")
        ))
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn router_imports_and_invokes_three_remote_mcp_sources() {
    let (search_url, search_server) = spawn_named_mcp_http_fixture(
        "application/json",
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        "search_docs",
        "SEARCH_OK",
    )
    .await;
    let (crm_url, crm_server) = spawn_named_mcp_http_fixture(
        "application/json",
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        "crm_lookup",
        "CRM_OK",
    )
    .await;
    let (ticket_url, ticket_server) = spawn_named_mcp_http_fixture(
        "text/event-stream",
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        "ticket_create",
        "TICKET_OK",
    )
    .await;

    let search = crate::adapters::mcp_client::McpRemoteToolProvider::connect(
        crate::adapters::mcp_client::McpRemoteServerConfig::new(
            "search",
            crate::adapters::mcp_client::McpRemoteTransport::StreamableHttp,
            search_url,
        )
        .unwrap(),
        None,
    )
    .await
    .unwrap();
    let crm = crate::adapters::mcp_client::McpRemoteToolProvider::connect(
        crate::adapters::mcp_client::McpRemoteServerConfig::new(
            "crm",
            crate::adapters::mcp_client::McpRemoteTransport::StreamableHttp,
            crm_url,
        )
        .unwrap(),
        None,
    )
    .await
    .unwrap();
    let ticket = crate::adapters::mcp_client::McpRemoteToolProvider::connect(
        crate::adapters::mcp_client::McpRemoteServerConfig::new(
            "ticket",
            crate::adapters::mcp_client::McpRemoteTransport::HttpSse,
            ticket_url,
        )
        .unwrap(),
        None,
    )
    .await
    .unwrap();
    let router = crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
        verlet_operations::operation_registry::OperationRegistry::new(),
    ))
    .with_kernel_tool_provider(std::sync::Arc::new(search))
    .with_kernel_tool_provider(std::sync::Arc::new(crm))
    .with_kernel_tool_provider(std::sync::Arc::new(ticket));

    let tool_names = router
        .tool_definitions()
        .await
        .into_iter()
        .map(|tool| tool.name)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        tool_names,
        std::collections::BTreeSet::from([
            "crm_lookup".to_string(),
            "search_docs".to_string(),
            "ticket_create".to_string(),
        ])
    );
    for (tool, marker) in [
        ("search_docs", "SEARCH_OK"),
        ("crm_lookup", "CRM_OK"),
        ("ticket_create", "TICKET_OK"),
    ] {
        let result = router
            .invoke_tool_call("call_multi", tool, serde_json::json!({"message": tool}))
            .await;
        assert!(matches!(
            result,
            verlet_history::CanonicalMessage::ToolResult {
                is_error: false,
                content,
                ..
            } if content.iter().any(|item| matches!(
                item,
                verlet_history::CanonicalContent::Text { text, .. }
                    if text.contains(marker) && text.contains(tool)
            ))
        ));
    }
    search_server.await.unwrap();
    crm_server.await.unwrap();
    ticket_server.await.unwrap();
}

async fn spawn_mcp_http_fixture(
    content_type: &'static str,
    seen_authorization: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
) -> (String, tokio::task::JoinHandle<()>) {
    spawn_named_mcp_http_fixture(
        content_type,
        seen_authorization,
        "remote_echo",
        "REMOTE_MCP_OK",
    )
    .await
}

async fn spawn_named_mcp_http_fixture(
    content_type: &'static str,
    seen_authorization: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    tool_name: &'static str,
    marker: &'static str,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}/mcp");
    let task = tokio::spawn(async move {
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let header_end = loop {
                let mut chunk = [0; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                buffer.extend_from_slice(&chunk[..read]);
                if let Some(index) = find_header_end(&buffer) {
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
            let auth = header_text
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_string())
                })
                .unwrap_or_default();
            if !auth.is_empty() {
                seen_authorization.lock().unwrap().push(auth);
            }
            let body_start = header_end + 4;
            while buffer.len() - body_start < content_length {
                let mut chunk = [0; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                buffer.extend_from_slice(&chunk[..read]);
            }
            let request: serde_json::Value =
                serde_json::from_slice(&buffer[body_start..body_start + content_length]).unwrap();
            let response = mcp_fixture_response(&request, tool_name, marker);
            let body = if content_type == "text/event-stream" {
                format!("event: message\ndata: {response}\n\n")
            } else {
                response
            };
            let raw = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(raw.as_bytes()).await.unwrap();
        }
    });
    (url, task)
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn mcp_fixture_response(request: &serde_json::Value, tool_name: &str, marker: &str) -> String {
    let id = request.get("id").cloned();
    match request.get("method").and_then(serde_json::Value::as_str) {
        Some("initialize") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "remote-fixture", "version": "1"}
            }
        }),
        Some("notifications/initialized") => serde_json::json!({
            "jsonrpc": "2.0",
            "result": {}
        }),
        Some("tools/list") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "tools": [{
                    "name": tool_name,
                    "description": "Echo a message through remote MCP.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {"message": {"type": "string"}},
                        "required": ["message"]
                    }
                }]
            }
        }),
        Some("tools/call") => {
            let message = request
                .pointer("/params/arguments/message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [{"type": "text", "text": format!("{marker} {message}")}],
                    "isError": false
                }
            })
        }
        _ => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": "unknown method"}
        }),
    }
    .to_string()
}

const ECHO_SERVER: &str = r#"
import json
import sys

for line in sys.stdin:
    if not line.strip():
        continue
    request = json.loads(line)
    method = request.get("method")
    if method == "initialize":
        print(json.dumps({"jsonrpc":"2.0","id":request["id"],"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"test-echo","version":"1"}}}), flush=True)
    elif method == "notifications/initialized":
        pass
    elif method == "tools/list":
        print(json.dumps({"jsonrpc":"2.0","id":request["id"],"result":{"tools":[{"name":"verlet_mcp_echo","description":"Echo a message through a real MCP stdio server.","inputSchema":{"type":"object","additionalProperties":False,"properties":{"message":{"type":"string"}},"required":["message"]}}]}}), flush=True)
    elif method == "tools/call":
        message = request.get("params", {}).get("arguments", {}).get("message", "")
        print(json.dumps({"jsonrpc":"2.0","id":request["id"],"result":{"content":[{"type":"text","text":"VERLET_MCP_ECHO_OK " + message}],"isError":False}}), flush=True)
    else:
        print(json.dumps({"jsonrpc":"2.0","id":request.get("id"),"error":{"code":-32601,"message":"unknown method"}}), flush=True)
"#;
