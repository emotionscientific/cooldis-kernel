use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

const SEARCH_FIXTURE_TEMPLATE: &str = include_str!("fixtures/search_operation.wat.tpl");

#[tokio::test]
async fn search_style_http_operation_registers_and_invokes_through_registry() {
    let (base_url, server) = spawn_http_server(
        200,
        r#"{"results":[{"title":"Verlet runtime","url":"https://verlet.local"}]}"#,
        vec![
            "POST /search HTTP/1.1",
            "content-type: application/json",
            "x-api-key: fixture-secret",
            r#"{"query":"verlet wasm"}"#,
        ],
    )
    .await;
    let url = format!("{base_url}/search");
    let http_grant = format!("net.http.private:POST:{base_url}");
    let wasm = wat::parse_str(render_search_fixture(&url, &http_grant))
        .expect("Example Search WAT fixture should compile to wasm");
    let registry = verlet_operations::operation_registry::OperationRegistry::new();

    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::from_config(
                "search",
                verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wasm))
                    .with_capability_grant(http_grant)
                    .with_capability_grant("secret:EXAMPLE_API_KEY")
                    .with_attachment_config(search_attachment_config(&base_url))
                    .with_secret("EXAMPLE_API_KEY", "fixture-secret"),
            )
            .with_metadata("provider", "search")
            .with_metadata("shape", "http-api-wrapper"),
        )
        .await
        .unwrap();

    let output = registry
        .invoke_bytes("search", "search", br#"{"query":"verlet wasm"}"#.to_vec())
        .await
        .unwrap();

    assert_eq!(output.operation.name, "search");
    assert_eq!(
        String::from_utf8_lossy(&output.output),
        r#"{"results":[{"title":"Verlet runtime","url":"https://verlet.local"}]}"#
    );
    assert!(String::from_utf8_lossy(&output.events).contains(r#""status":200"#));
    server.await.unwrap();
}

#[tokio::test]
async fn search_style_http_operation_runs_through_shell_command() {
    let (base_url, server) = spawn_http_server(
        200,
        r#"{"results":[{"title":"Verlet runtime","url":"https://verlet.local"}]}"#,
        vec![
            "POST /search HTTP/1.1",
            "content-type: application/json",
            "x-api-key: fixture-secret",
            r#"{"query":"verlet wasm"}"#,
        ],
    )
    .await;
    let url = format!("{base_url}/search");
    let http_grant = format!("net.http.private:POST:{base_url}");
    let wasm = wat::parse_str(render_search_fixture(&url, &http_grant))
        .expect("Example Search WAT fixture should compile to wasm");
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());

    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::from_config(
                "search",
                verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wasm))
                    .with_capability_grant(http_grant.clone())
                    .with_capability_grant("secret:EXAMPLE_API_KEY")
                    .with_attachment_config(search_attachment_config(&base_url))
                    .with_secret("EXAMPLE_API_KEY", "fixture-secret"),
            )
            .with_metadata("provider", "search")
            .with_metadata("shape", "http-api-wrapper"),
        )
        .await
        .unwrap();

    let config = verlet::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_capability_grant(http_grant)
        .with_capability_grant("secret:EXAMPLE_API_KEY");
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();
    let output = harness
        .execute(r#"command -v search && search '{"query":"verlet wasm"}'"#)
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("search\n"));
    assert!(output.stdout.contains("Verlet runtime"));
    assert!(output.stderr.contains(r#""status":200"#));
    server.await.unwrap();
}

#[tokio::test]
async fn published_search_operation_resolves_secret_store_and_invokes_through_agent_router() {
    let (base_url, server) = spawn_http_server(
        200,
        r#"{"results":[{"title":"Verlet runtime","url":"https://verlet.local"}]}"#,
        vec![
            "POST /search HTTP/1.1",
            "content-type: application/json",
            "x-api-key: fixture-secret",
            r#"{"query":"verlet wasm"}"#,
        ],
    )
    .await;
    let url = format!("{base_url}/search");
    let http_grant = format!("net.http.private:POST:{base_url}");
    let wasm = wat::parse_str(render_search_fixture(&url, &http_grant))
        .expect("Example Search WAT fixture should compile to wasm");
    let root = temp_dir("published-search-secret");
    let registry_root = root.join("operations");
    let artifact_path = root.join("search.wasm");
    std::fs::write(&artifact_path, wasm).unwrap();
    let registry = verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root);
    let record = registry
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: "search".to_string(),
                artifact_path: artifact_path.clone(),
                source: verlet_operations::operation_store::PublishedOperationSource::Wasm {
                    bin_path: artifact_path,
                },
                interface: None,
                capability_grants: std::collections::BTreeSet::from([
                    http_grant.clone(),
                    "secret:EXAMPLE_API_KEY".to_string(),
                ]),
                metadata: std::collections::BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    let secret_store =
        verlet_metadata::secret_store::SqliteSecretStore::open(root.join("state/metadata.turso"))
            .await
            .unwrap();
    secret_store
        .set_secret(
            "EXAMPLE_API_KEY",
            "fixture-secret",
            verlet_metadata::secret_store::SecretSourceKind::Env,
            Some("EXAMPLE_API_KEY".to_string()),
        )
        .await
        .unwrap();

    let catalog =
        verlet::operations::plugins::LocalPluginCatalog::load_records_with_secret_resolver(
            &registry_root,
            vec![record],
            Vec::new(),
            std::sync::Arc::new(secret_store),
        )
        .await
        .unwrap();
    let router =
        verlet::agent::agent_tool_router::AgentToolRouter::new(catalog.operation_registry());
    let definitions = router.tool_definitions().await;
    assert!(
        definitions
            .iter()
            .any(|definition| definition.name == "search"),
        "{definitions:?}"
    );

    let result = router
        .invoke_tool_call(
            "call_1",
            "search",
            serde_json::json!({"query":"verlet wasm"}),
        )
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if tool_result_text(&content).contains("Verlet runtime")
    ));
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

fn search_attachment_config(origin: &str) -> verlet_wasm::WasmAttachmentConfig {
    verlet_wasm::WasmAttachmentConfig {
        allowed_secrets: std::collections::BTreeSet::from(["EXAMPLE_API_KEY".to_string()]),
        allowed_private_network: std::collections::BTreeMap::from([(
            origin.to_string(),
            std::collections::BTreeSet::from(["POST".to_string()]),
        )]),
    }
}

fn render_search_fixture(url: &str, http_grant: &str) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "search",
            "input": "json",
            "output": "json",
            "events": "jsonl",
            "mode": "sync",
            "required_capabilities": [http_grant, "secret:EXAMPLE_API_KEY"]
        }]
    })
    .to_string();
    let request = serde_json::json!({
        "abi": "cooldis.net.http/0.1",
        "method": "POST",
        "url": url,
        "headers": [["content-type", "application/json"]],
        "secret_headers": [["x-api-key", "EXAMPLE_API_KEY"]],
        "timeout_ms": 5000,
        "max_response_bytes": 2048
    })
    .to_string();
    let body = br#"{"query":"verlet wasm"}"#;
    SEARCH_FIXTURE_TEMPLATE
        .replace("{{manifest}}", &wat_bytes(manifest.as_bytes()))
        .replace("{{manifest_len}}", &manifest.len().to_string())
        .replace("{{request}}", &wat_bytes(request.as_bytes()))
        .replace("{{request_len}}", &request.len().to_string())
        .replace("{{body}}", &wat_bytes(body))
        .replace("{{body_len}}", &body.len().to_string())
}

async fn spawn_http_server(
    status: u16,
    response_body: &'static str,
    request_contains: Vec<&'static str>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let read = socket.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            let request_text = String::from_utf8_lossy(&request);
            if let Some(header_end) = request_text.find("\r\n\r\n") {
                let content_length = request_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or_default();
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
        }
        let request_text = String::from_utf8_lossy(&request);
        for expected in request_contains {
            assert!(
                request_text.contains(expected),
                "request did not contain {expected:?}: {request_text}"
            );
        }
        let response = format!(
            "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    (base_url, handle)
}

fn tool_result_text(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            verlet_history::CanonicalContent::Thinking { .. }
            | verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("verlet-search-{label}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&path).unwrap();
    path
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
