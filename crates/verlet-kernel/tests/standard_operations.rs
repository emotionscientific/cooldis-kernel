use bashkit::FileSystem as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

#[tokio::test]
async fn json_query_reads_nested_value() {
    let factory = standard_operation_factory(json_query_wasm());
    let output = factory
        .invoke_operation_bytes(
            "json_query",
            serde_json::to_vec(&serde_json::json!({
                "json": {"items": [{"name": "Ada"}, {"name": "Linus"}]},
                "pointer": "/items/1/name"
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let value: serde_json::Value = serde_json::from_slice(&output.output).unwrap();
    assert_eq!(value, serde_json::json!({"found": true, "value": "Linus"}));
}

#[tokio::test]
async fn json_query_invalid_pointer_is_invalid_argument() {
    let factory = standard_operation_factory(json_query_wasm());
    let err = factory
        .invoke_operation_bytes(
            "json_query",
            serde_json::to_vec(&serde_json::json!({"json": {"name": "Ada"}, "pointer": "name"}))
                .unwrap(),
        )
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("returned status 1"));
}

#[tokio::test]
async fn json_query_empty_pointer_returns_whole_document() {
    let factory = standard_operation_factory(json_query_wasm());
    let document = serde_json::json!({"name": "Ada", "scores": [10, 11]});
    let output = factory
        .invoke_operation_bytes(
            "json_query",
            serde_json::to_vec(&serde_json::json!({"json": document, "pointer": ""})).unwrap(),
        )
        .await
        .unwrap();

    let value: serde_json::Value = serde_json::from_slice(&output.output).unwrap();
    assert_eq!(
        value,
        serde_json::json!({"found": true, "value": {"name": "Ada", "scores": [10, 11]}})
    );
}

#[tokio::test]
async fn file_read_reads_from_vfs() {
    let factory = verlet::WasmRuntimeFactory::new(
        verlet::WasmRuntimeConfig::new(verlet::WasmRuntimeArtifact::bytes(file_read_wasm()))
            .with_vfs(read_test_vfs().await),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes(
            "file_read",
            serde_json::to_vec(
                &serde_json::json!({"path": "/workspace/input.txt", "offsetBytes": 6, "maxBytes": 4}),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let value: serde_json::Value = serde_json::from_slice(&output.output).unwrap();
    assert_eq!(
        value,
        serde_json::json!({"content": "beta", "bytesRead": 4, "eof": false})
    );
}

#[tokio::test]
async fn file_read_missing_file_returns_structured_error() {
    let factory = verlet::WasmRuntimeFactory::new(
        verlet::WasmRuntimeConfig::new(verlet::WasmRuntimeArtifact::bytes(file_read_wasm()))
            .with_vfs(read_test_vfs().await),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes(
            "file_read",
            serde_json::to_vec(&serde_json::json!({"path": "/workspace/missing.txt"})).unwrap(),
        )
        .await
        .unwrap();

    let value: serde_json::Value = serde_json::from_slice(&output.output).unwrap();
    assert_eq!(value["content"], "");
    assert_eq!(value["bytesRead"], 0);
    assert_eq!(value["eof"], true);
    assert_eq!(value["error"]["code"], "not_found");
}

#[tokio::test]
async fn file_read_handles_zero_max_and_offset_past_eof() {
    let factory = verlet::WasmRuntimeFactory::new(
        verlet::WasmRuntimeConfig::new(verlet::WasmRuntimeArtifact::bytes(file_read_wasm()))
            .with_vfs(read_test_vfs().await),
    )
    .unwrap();

    let zero = factory
        .invoke_operation_bytes(
            "file_read",
            serde_json::to_vec(
                &serde_json::json!({"path": "/workspace/input.txt", "offsetBytes": 0, "maxBytes": 0}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let zero: serde_json::Value = serde_json::from_slice(&zero.output).unwrap();
    assert_eq!(
        zero,
        serde_json::json!({"content": "", "bytesRead": 0, "eof": false})
    );

    let past_eof = factory
        .invoke_operation_bytes(
            "file_read",
            serde_json::to_vec(
                &serde_json::json!({"path": "/workspace/input.txt", "offsetBytes": 999}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let past_eof: serde_json::Value = serde_json::from_slice(&past_eof.output).unwrap();
    assert_eq!(
        past_eof,
        serde_json::json!({"content": "", "bytesRead": 0, "eof": true})
    );
}

#[tokio::test]
async fn http_fetch_reads_from_local_server() {
    let (base_url, server) = spawn_http_server(
        "hello from standard operations",
        vec!["x-test: standard-op"],
    )
    .await;
    let url = format!("{base_url}/fetch");
    let grant = format!("net.http.private:GET:{base_url}");
    let factory = verlet::WasmRuntimeFactory::new(
        verlet::WasmRuntimeConfig::new(verlet::WasmRuntimeArtifact::bytes(http_fetch_wasm()))
            .with_capability_grant(grant),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes(
            "http_fetch",
            serde_json::to_vec(&serde_json::json!({
                "url": url,
                "headers": {"x-test": "standard-op"},
                "maxResponseBytes": 1024
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let value: serde_json::Value = serde_json::from_slice(&output.output).unwrap();
    assert_eq!(value["status"], 200);
    assert_eq!(value["bodyText"], "hello from standard operations");
    assert_eq!(value["truncated"], false);
    assert_eq!(value["headers"]["content-type"], "text/plain");
    assert!(value.get("error").is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn http_fetch_reports_cap_edge_truncation() {
    let (zero_base_url, zero_server) = spawn_http_server("abc", vec![]).await;
    let zero_grant = format!("net.http.private:GET:{zero_base_url}");
    let zero_factory = verlet::WasmRuntimeFactory::new(
        verlet::WasmRuntimeConfig::new(verlet::WasmRuntimeArtifact::bytes(http_fetch_wasm()))
            .with_capability_grant(zero_grant),
    )
    .unwrap();
    let zero = zero_factory
        .invoke_operation_bytes(
            "http_fetch",
            serde_json::to_vec(
                &serde_json::json!({"url": format!("{zero_base_url}/fetch"), "maxResponseBytes": 0}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let zero: serde_json::Value = serde_json::from_slice(&zero.output).unwrap();
    assert_eq!(zero["bodyText"], "");
    assert_eq!(zero["truncated"], true);
    zero_server.await.unwrap();

    let (exact_base_url, exact_server) = spawn_http_server("abcd", vec![]).await;
    let exact_grant = format!("net.http.private:GET:{exact_base_url}");
    let exact_factory = verlet::WasmRuntimeFactory::new(
        verlet::WasmRuntimeConfig::new(verlet::WasmRuntimeArtifact::bytes(http_fetch_wasm()))
            .with_capability_grant(exact_grant),
    )
    .unwrap();
    let exact = exact_factory
        .invoke_operation_bytes(
            "http_fetch",
            serde_json::to_vec(
                &serde_json::json!({"url": format!("{exact_base_url}/fetch"), "maxResponseBytes": 4}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let exact: serde_json::Value = serde_json::from_slice(&exact.output).unwrap();
    assert_eq!(exact["bodyText"], "abcd");
    assert_eq!(exact["truncated"], false);
    exact_server.await.unwrap();
}

#[tokio::test]
async fn http_fetch_denied_origin_fails_closed() {
    let factory = standard_operation_factory(http_fetch_wasm());
    let output = factory
        .invoke_operation_bytes(
            "http_fetch",
            serde_json::to_vec(&serde_json::json!({"url": "http://127.0.0.1:9/fetch"})).unwrap(),
        )
        .await
        .unwrap();

    let value: serde_json::Value = serde_json::from_slice(&output.output).unwrap();
    assert_eq!(value["status"], 0);
    assert_eq!(value["error"]["code"], "capability_denied");
}

fn standard_operation_factory(wasm: Vec<u8>) -> verlet::WasmRuntimeFactory {
    verlet::WasmRuntimeFactory::new(verlet::WasmRuntimeConfig::new(
        verlet::WasmRuntimeArtifact::bytes(wasm),
    ))
    .unwrap()
}

fn http_fetch_wasm() -> Vec<u8> {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| build_tool_wasm("http-fetch")).clone()
}

fn file_read_wasm() -> Vec<u8> {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| build_tool_wasm("file-read")).clone()
}

fn json_query_wasm() -> Vec<u8> {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| build_tool_wasm("json-query")).clone()
}

fn build_tool_wasm(name: &str) -> Vec<u8> {
    let root = workspace_root();
    let build = verlet::build_rust_wasm_module(verlet::RustWasmBuildOptions::new(
        root.join("tools").join(name),
    ))
    .unwrap_or_else(|err| panic!("failed to build {name} tool wasm: {err}"));
    std::fs::read(build.artifact_path)
        .unwrap_or_else(|err| panic!("failed to read {name} tool wasm artifact: {err}"))
}

fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("kernel crate should live under crates/verlet-kernel")
        .to_path_buf()
}

async fn read_test_vfs() -> std::sync::Arc<verlet::VerletVfs> {
    let workspace = std::sync::Arc::new(bashkit::InMemoryFs::new());
    workspace
        .write_file(std::path::Path::new("/input.txt"), b"alpha beta gamma")
        .await
        .unwrap();
    let vfs = std::sync::Arc::new(verlet::VerletVfs::new(std::sync::Arc::new(
        bashkit::InMemoryFs::new(),
    )));
    vfs.mount("/workspace", workspace).unwrap();
    vfs
}

async fn spawn_http_server(
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
            if String::from_utf8_lossy(&request).contains("\r\n\r\n") {
                break;
            }
        }
        let request_text = String::from_utf8_lossy(&request);
        assert!(request_text.contains("GET /fetch HTTP/1.1"));
        for expected in request_contains {
            assert!(
                request_text.contains(expected),
                "request did not contain {expected:?}: {request_text}"
            );
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    (base_url, handle)
}
