use bashkit::FileSystem as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

const WASM_VFS_PROBE_FIXTURE_TEMPLATE: &str =
    include_str!("../../../tests/fixtures/wasm_vfs_probe_operation.wat.tpl");

fn echo_guest() -> &'static str {
    r#"
        (module
          (import "verlet" "input_read" (func $input_read (param i32 i32) (result i32)))
          (import "verlet" "output_write" (func $output_write (param i32 i32)))
          (import "verlet" "log" (func $log (param i32 i32)))
          (memory (export "memory") 1)
          (data (i32.const 2048) "wasm:")
          (data (i32.const 2060) "saw input")
          (func (export "handle_turn") (result i32)
            (local $n i32)
            i32.const 0
            i32.const 1024
            call $input_read
            local.set $n
            i32.const 2060
            i32.const 9
            call $log
            i32.const 2048
            i32.const 5
            call $output_write
            i32.const 0
            local.get $n
            call $output_write
            i32.const 0))
        "#
}

fn loop_guest() -> &'static str {
    r#"
        (module
          (memory (export "memory") 1)
          (func (export "handle_turn") (result i32)
            (loop $again
              br $again)
            i32.const 0))
        "#
}

fn ambient_wasi_guest() -> &'static str {
    r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "handle_turn") (result i32)
            i32.const 0))
        "#
}

fn wat_bytes(bytes: &[u8]) -> String {
    let mut encoded = String::new();
    for byte in bytes {
        match byte {
            b'"' => encoded.push_str("\\22"),
            b'\\' => encoded.push_str("\\5c"),
            0x20..=0x7e => encoded.push(*byte as char),
            _ => encoded.push_str(&format!("\\{byte:02x}")),
        }
    }
    encoded
}

fn wat_guest(wat: impl AsRef<str>) -> Vec<u8> {
    wat::parse_str(wat.as_ref()).expect("test WAT fixture should compile to wasm")
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn wasm_runtime_factory_uses_signal_trap_handler_on_macos() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(
        operation_guest(),
    ))
    .unwrap();
    let manifest = factory.describe().await.unwrap().unwrap();
    assert!(
        manifest
            .operation(verlet_wasm::DEFAULT_OPERATION_NAME)
            .is_some()
    );
}

fn operation_guest() -> String {
    operation_guest_with_required_capabilities(Vec::<&str>::new())
}

fn operation_guest_with_required_capabilities(required_capabilities: Vec<&str>) -> String {
    let manifest = serde_json::json!({
        "abi": verlet_wasm::runner::OPERATION_ABI,
        "operations": [{
            "id": 1,
            "name": "handle_turn",
            "input": "bytes",
            "output": "bytes",
            "events": "jsonl",
            "mode": "streaming",
            "required_capabilities": required_capabilities
        }]
    })
    .to_string();
    let event = br#"{"type":"progress","value":1}
"#;
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "event_emit" (func $event_emit (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "op:")
              (data (i32.const 8200) "{event}")
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
                  i32.const {not_found}
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
                i32.const {event_len}
                i32.store
                local.get $invocation
                i32.const 8200
                i32.const 0
                call $event_emit
                drop
                i32.const 0
                i32.const 3
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
        event = wat_bytes(event),
        event_len = event.len(),
        not_found = verlet_wasm::runner::STATUS_NOT_FOUND,
    )
}

fn capability_manifest_guest() -> String {
    let manifest = serde_json::json!({
        "abi": verlet_wasm::runner::OPERATION_ABI,
        "operations": [{
            "id": 1,
            "name": "handle_turn",
            "input": "bytes",
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": ["verlet:secret/read"]
        }]
    })
    .to_string();
    format!(
        r#"
            (module
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__verlet_call_operation__")
                (param i32 i32 i32 i32 i32)
                (result i32)
                unreachable))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
    )
}

fn http_guest(url: &str, required_capabilities: Vec<String>) -> String {
    let manifest = serde_json::json!({
        "abi": verlet_wasm::runner::OPERATION_ABI,
        "operations": [{
            "id": 1,
            "name": "search",
            "input": "json",
            "output": "json",
            "events": "jsonl",
            "mode": "sync",
            "required_capabilities": required_capabilities
        }]
    })
    .to_string();
    let request = serde_json::json!({
        "abi": verlet_wasm::runner::HTTP_ABI,
        "method": "POST",
        "url": url,
        "headers": [["content-type", "application/json"]],
        "secret_headers": [["x-api-key", "EXAMPLE_API_KEY"]],
        "timeout_ms": 5000,
        "max_response_bytes": 2048
    })
    .to_string();
    let body = br#"{"query":"verlet"}"#;
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "event_emit" (func $event_emit (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "http_request" (func $http_request (param i32 i32 i32 i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "{request}")
              (data (i32.const 12288) "{body}")
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
                (local $status i32)
                (local $meta_source i32)
                (local $body_source i32)
                (local $n i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const {not_found}
                  return
                end
                local.get $invocation
                i32.const 8192
                i32.const {request_len}
                i32.const 12288
                i32.const {body_len}
                i32.const 64
                local.get $events
                call $http_request
                local.set $status
                local.get $status
                i32.const 0
                i32.ne
                if
                  local.get $status
                  return
                end
                i32.const 64
                i32.load
                local.set $meta_source
                i32.const 68
                i32.load
                local.set $body_source
                i32.const 0
                i32.const 2048
                i32.store
                local.get $meta_source
                i32.const 16384
                i32.const 0
                call $source_read
                drop
                i32.const 0
                i32.load
                local.set $n
                i32.const 0
                local.get $n
                i32.store
                local.get $invocation
                i32.const 16384
                i32.const 0
                call $event_emit
                drop
                i32.const 0
                i32.const 2048
                i32.store
                local.get $body_source
                i32.const 20480
                i32.const 0
                call $source_read
                drop
                i32.const 0
                i32.load
                local.set $n
                i32.const 0
                local.get $n
                i32.store
                local.get $output
                i32.const 20480
                i32.const 0
                call $sink_write
                drop
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
        request = wat_bytes(request.as_bytes()),
        request_len = request.len(),
        body = wat_bytes(body),
        body_len = body.len(),
        not_found = verlet_wasm::runner::STATUS_NOT_FOUND,
    )
}

fn wasm_vfs_tools_guest() -> Vec<u8> {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| {
        let fixture_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("wasm-vfs-tools");
        let build = crate::operations::operation_builder::build_rust_wasm_module(
            crate::operations::operation_builder::RustWasmBuildOptions::new(fixture_dir),
        )
        .expect("failed to build Rust Wasm VFS fixture");
        std::fs::read(build.artifact_path).expect("failed to read compiled Rust Wasm VFS fixture")
    })
    .clone()
}

fn wasm_vfs_probe_guest() -> String {
    let manifest = serde_json::json!({
        "abi": verlet_wasm::runner::OPERATION_ABI,
        "operations": [
            {
                "id": 1,
                "name": "invalid_mode",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 2,
                "name": "invalid_handle",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 3,
                "name": "close_twice",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            }
        ]
    })
    .to_string();
    let path = b"/workspace/input.txt";
    render_vfs_fixture(WASM_VFS_PROBE_FIXTURE_TEMPLATE, &manifest)
        .replace("{{path}}", &wat_bytes(path))
        .replace("{{path_len}}", &path.len().to_string())
}

fn render_vfs_fixture(template: &str, manifest: &str) -> String {
    template
        .replace("{{manifest}}", &wat_bytes(manifest.as_bytes()))
        .replace("{{manifest_len}}", &manifest.len().to_string())
        .replace(
            "{{not_found}}",
            &verlet_wasm::runner::STATUS_NOT_FOUND.to_string(),
        )
        .replace(
            "{{read_mode}}",
            &verlet_wasm::runner::FS_MODE_READ.to_string(),
        )
        .replace("{{eof}}", &verlet_wasm::runner::STATUS_EOF.to_string())
}

async fn wasm_cat_vfs() -> std::sync::Arc<verlet_vfs::VerletVfs> {
    let workspace = std::sync::Arc::new(bashkit::InMemoryFs::new());
    workspace
        .write_file(
            std::path::Path::new("/input.txt"),
            b"alpha\nbeta\ngamma from verlet vfs\n",
        )
        .await
        .unwrap();
    workspace
        .write_file(
            std::path::Path::new("/tail.txt"),
            b"one\ntwo\nthree\nfour\nfive\n",
        )
        .await
        .unwrap();
    let vfs = std::sync::Arc::new(verlet_vfs::VerletVfs::new(std::sync::Arc::new(
        bashkit::InMemoryFs::new(),
    )));
    vfs.mount("/workspace", workspace).unwrap();
    vfs
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

async fn spawn_http_redirect_server(
    location: &'static str,
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
        let response = format!(
            "HTTP/1.1 302 Found\r\nlocation: {location}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    (base_url, handle)
}

async fn spawn_http_bytes_server(response_body: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
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
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            response_body.len()
        );
        socket.write_all(headers.as_bytes()).await.unwrap();
        socket.write_all(&response_body).await.unwrap();
    });
    (base_url, handle)
}

fn http_request_bytes(
    url: &str,
    max_response_bytes: Option<usize>,
    secret_headers: Vec<(String, String)>,
) -> Vec<u8> {
    serde_json::to_vec(&verlet_wasm::WasmHttpRequest {
        abi: verlet_wasm::runner::HTTP_ABI.to_string(),
        method: "POST".to_string(),
        url: url.to_string(),
        headers: vec![("content-type".to_string(), "application/json".to_string())],
        secret_headers,
        secret_header_prefixes: Vec::new(),
        input_mapping: None,
        response_envelope: false,
        timeout_ms: Some(5000),
        max_response_bytes,
    })
    .unwrap()
}

async fn next_output(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> String {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("timed out waiting for event")
            .expect("event stream closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } = event {
            return text;
        }
    }
}

async fn next_cancelled(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> String {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("timed out waiting for event")
            .expect("event stream closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { reason, .. } =
            event
        {
            return reason;
        }
    }
}

async fn next_failed(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> String {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("timed out waiting for event")
            .expect("event stream closed");
        if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } = event
        {
            return message;
        }
    }
}

#[tokio::test]
async fn wasm_runtime_runs_guest_and_mirrors_output() {
    let factory = std::sync::Arc::new(
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(echo_guest()))
            .unwrap(),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory);
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "hello wasm",
    )
    .await
    .unwrap();

    let output = next_output(&mut events).await;
    assert!(output.contains("[wasm log] saw input"));
    assert!(output.contains("wasm:hello wasm"));

    let session = thread.session_context().await.unwrap();
    assert_eq!(session.entries.len(), 2);
}

#[tokio::test]
async fn wasm_operation_manifest_is_discovered_at_registration() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(
        operation_guest(),
    ))
    .unwrap();
    let manifest = factory
        .describe()
        .await
        .unwrap()
        .expect("operation manifest");

    assert_eq!(manifest.abi, verlet_wasm::runner::OPERATION_ABI);
    assert_eq!(manifest.operations.len(), 1);
    assert_eq!(manifest.operations[0].id, 1);
    assert_eq!(manifest.operations[0].name, "handle_turn");
    assert_eq!(
        manifest.operations[0].mode,
        verlet_abi::WasmOperationMode::Streaming
    );
}

#[tokio::test]
async fn wasm_operation_artifact_validation_accepts_operation_guest() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(
        operation_guest(),
    ))
    .unwrap();
    let manifest = factory.validate_operation_artifact().await.unwrap();

    assert_eq!(manifest.operations[0].name, "handle_turn");
}

#[tokio::test]
async fn wasm_operation_artifact_validation_rejects_missing_manifest() {
    let factory =
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(echo_guest()))
            .unwrap();

    let err = factory
        .validate_operation_artifact()
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("__verlet_describe_module__"), "{err}");
}

#[tokio::test]
async fn wasm_runtime_rejects_textual_wat_artifact() {
    let factory =
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(echo_guest()).unwrap();

    let err = factory.describe().await.unwrap_err().to_string();

    assert!(err.contains("compiled .wasm bytes"), "{err}");
}

#[tokio::test]
async fn wasm_operation_artifact_validation_rejects_unknown_import() {
    let guest = r#"
        (module
          (import "mystery" "call" (func $call))
          (memory (export "memory") 1)
          (func (export "__verlet_describe_module__") (param i32) (result i32)
            i32.const 0)
          (func (export "__verlet_call_operation__") (param i32 i32 i32 i32 i32) (result i32)
            i32.const 0))
        "#;
    let factory =
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(guest)).unwrap();

    let err = factory
        .validate_operation_artifact()
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("mystery::call"), "{err}");
}

#[tokio::test]
async fn wasm_operation_artifact_validation_rejects_raw_caller_identity_import() {
    let guest = r#"
        (module
          (import "cooldis_0.1" "caller_identity" (func $caller_identity (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "__verlet_describe_module__") (param i32) (result i32)
            i32.const 0)
          (func (export "__verlet_call_operation__") (param i32 i32 i32 i32 i32) (result i32)
            i32.const 0))
        "#;
    let factory =
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(guest)).unwrap();

    let err = factory
        .validate_operation_artifact()
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("caller_identity"), "{err}");
    assert!(err.contains("unsupported host functions"), "{err}");
}

#[tokio::test]
async fn wasm_operation_artifact_validation_explains_wasm_bindgen_imports() {
    let guest = r#"
        (module
          (import "__wbindgen_placeholder__" "__wbindgen_describe" (func $bindgen))
          (memory (export "memory") 1)
          (func (export "__verlet_describe_module__") (param i32) (result i32)
            i32.const 0)
          (func (export "__verlet_call_operation__") (param i32 i32 i32 i32 i32) (result i32)
            i32.const 0))
        "#;
    let factory =
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(guest)).unwrap();

    let err = factory
        .validate_operation_artifact()
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("wasm-bindgen"), "{err}");
}

#[tokio::test]
async fn wasm_operation_artifact_validation_explains_wasi_random_imports() {
    let guest = r#"
        (module
          (import "wasi_snapshot_preview1" "random_get" (func $random_get (param i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "__verlet_describe_module__") (param i32) (result i32)
            i32.const 0)
          (func (export "__verlet_call_operation__") (param i32 i32 i32 i32 i32) (result i32)
            i32.const 0))
        "#;
    let factory =
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(guest)).unwrap();

    let err = factory
        .validate_operation_artifact()
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("random_get"), "{err}");
    assert!(err.contains("deterministic operations"), "{err}");
}

#[tokio::test]
async fn wasm_operation_can_be_invoked_as_harness_tool() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(
        operation_guest(),
    ))
    .unwrap();

    let output = factory
        .invoke_operation_bytes("handle_turn", b"hello operation".to_vec())
        .await
        .unwrap();

    assert_eq!(output.operation.name, "handle_turn");
    assert_eq!(
        String::from_utf8_lossy(&output.output),
        "op:hello operation"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.events),
        "{\"type\":\"progress\",\"value\":1}\n"
    );
}

#[tokio::test]
async fn wasm_operation_can_be_invoked_as_process_handle() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(
        operation_guest(),
    ))
    .unwrap();

    let process = factory
        .invoke_operation_process("handle_turn", b"hello process".to_vec())
        .await
        .unwrap();
    let output = process.output();

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::WasmOperation
    );
    assert_eq!(output.stdout_text_lossy(), "op:hello process");
    assert_eq!(
        output.stderr_text_lossy(),
        "{\"type\":\"progress\",\"value\":1}\n"
    );
    assert_eq!(output.exit_code(), Some(0));
    assert!(output.success());
}

#[tokio::test]
async fn wasm_runtime_uses_described_operation_for_thread_submit() {
    let factory = std::sync::Arc::new(
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(
            operation_guest(),
        ))
        .unwrap(),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory);
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "hello thread",
    )
    .await
    .unwrap();

    let output = next_output(&mut events).await;
    assert!(output.contains("op:hello thread"));
    assert!(output.contains(r#"{"type":"progress","value":1}"#));
}

#[tokio::test]
async fn wasm_operation_required_capabilities_fail_before_guest_execution() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(
        capability_manifest_guest(),
    ))
    .unwrap();

    let err = factory
        .invoke_operation_bytes("handle_turn", b"secret".to_vec())
        .await
        .unwrap_err();

    assert!(err.to_string().contains("requires ungranted capabilities"));
}

#[tokio::test]
async fn wasm_operation_preserves_invocation_identity_without_guest_authority() {
    let context =
        verlet_abi::InvocationContext::new(verlet_abi::ExecutionPrincipal::system("tool-runner"))
            .with_caller(verlet_abi::Principal::user("user-123"))
            .with_audit_metadata("request_id", "req-123");
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            operation_guest(),
        )))
        .with_invocation_context(context.clone()),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes("handle_turn", b"hello identity".to_vec())
        .await
        .unwrap();

    assert_eq!(output.invocation_context, context);
    assert_eq!(
        output.invocation_context.caller,
        Some(verlet_abi::Principal::user("user-123"))
    );
    assert_eq!(
        output.invocation_context.execution,
        verlet_abi::ExecutionPrincipal::system("tool-runner")
    );
    assert_eq!(
        output.invocation_context.audit_metadata["request_id"],
        "req-123"
    );
    assert_eq!(String::from_utf8_lossy(&output.output), "op:hello identity");
}

#[tokio::test]
async fn wasm_operation_accepts_invocation_context_grants_before_guest_execution() {
    let context = verlet_abi::InvocationContext::anonymous().with_grant("verlet:secret/read");
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            operation_guest_with_required_capabilities(vec!["verlet:secret/read"]),
        )))
        .with_invocation_context(context),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes("handle_turn", b"delegated".to_vec())
        .await
        .unwrap();

    assert_eq!(String::from_utf8_lossy(&output.output), "op:delegated");
}

#[tokio::test]
async fn wasm_cat_reads_file_from_verlet_vfs() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_vfs_tools_guest(),
        ))
        .with_vfs(wasm_cat_vfs().await),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes("cat", b"/workspace/input.txt".to_vec())
        .await
        .unwrap();

    assert_eq!(output.operation.name, "cat");
    assert_eq!(
        String::from_utf8_lossy(&output.output),
        "alpha\nbeta\ngamma from verlet vfs\n"
    );
    assert!(output.events.is_empty());
}

#[tokio::test]
async fn wasm_tail_reads_last_two_lines_from_verlet_vfs() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_vfs_tools_guest(),
        ))
        .with_vfs(wasm_cat_vfs().await),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes("tail", b"/workspace/tail.txt".to_vec())
        .await
        .unwrap();

    assert_eq!(output.operation.name, "tail");
    assert_eq!(String::from_utf8_lossy(&output.output), "four\nfive\n");
    assert!(output.events.is_empty());
}

#[tokio::test]
async fn wasm_cat_missing_vfs_file_returns_not_found() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_vfs_tools_guest(),
        ))
        .with_vfs(wasm_cat_vfs().await),
    )
    .unwrap();

    let err = factory
        .invoke_operation_bytes("cat", b"/workspace/missing.txt".to_vec())
        .await
        .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("returned status 2"));
    assert!(!message.contains("/Users/"));
    assert!(!message.contains("s3://"));
}

#[tokio::test]
async fn wasm_vfs_read_imports_fail_closed_for_invalid_mode_and_handle() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            wasm_vfs_probe_guest(),
        )))
        .with_vfs(wasm_cat_vfs().await),
    )
    .unwrap();

    let invalid_mode = factory
        .invoke_operation_bytes("invalid_mode", Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(invalid_mode.contains("returned status 1"));

    let invalid_handle = factory
        .invoke_operation_bytes("invalid_handle", Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(invalid_handle.contains("returned status 2"));
}

#[tokio::test]
async fn wasm_vfs_close_drops_invocation_local_handles() {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            wasm_vfs_probe_guest(),
        )))
        .with_vfs(wasm_cat_vfs().await),
    )
    .unwrap();

    let err = factory
        .invoke_operation_bytes("close_twice", Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("returned status 2"));
}

#[tokio::test]
async fn wasm_http_operation_can_call_mock_exa_api() {
    let (base_url, server) = spawn_http_server(
        200,
        r#"{"results":[{"title":"Verlet"}]}"#,
        vec![
            "POST /search HTTP/1.1",
            "content-type: application/json",
            "x-api-key: test-secret",
            r#"{"query":"verlet"}"#,
        ],
    )
    .await;
    let url = format!("{base_url}/search");
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(&url).unwrap()).unwrap();
    let http_grant = format!("net.http.private:POST:{origin}");
    let guest = http_guest(
        &url,
        vec![http_grant.clone(), "secret:EXAMPLE_API_KEY".to_string()],
    );
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            &guest,
        )))
        .with_capability_grant(http_grant)
        .with_capability_grant("secret:EXAMPLE_API_KEY")
        .with_secret("EXAMPLE_API_KEY", "test-secret"),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes("search", br#"{"query":"verlet"}"#.to_vec())
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8_lossy(&output.output),
        r#"{"results":[{"title":"Verlet"}]}"#
    );
    assert!(String::from_utf8_lossy(&output.events).contains(r#""status":200"#));
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_http_import_uses_invocation_context_grants_for_privileged_work() {
    let (base_url, server) = spawn_http_server(
        200,
        r#"{"results":[{"title":"Delegated"}]}"#,
        vec![
            "POST /search HTTP/1.1",
            "x-api-key: delegated-secret",
            r#"{"query":"verlet"}"#,
        ],
    )
    .await;
    let url = format!("{base_url}/search");
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(&url).unwrap()).unwrap();
    let http_grant = format!("net.http.private:POST:{origin}");
    let guest = http_guest(
        &url,
        vec![http_grant.clone(), "secret:EXAMPLE_API_KEY".to_string()],
    );
    let context =
        verlet_abi::InvocationContext::new(verlet_abi::ExecutionPrincipal::system("http-broker"))
            .with_caller(verlet_abi::Principal::agent("agent-123"))
            .with_grant(http_grant)
            .with_grant("secret:EXAMPLE_API_KEY")
            .with_audit_metadata("request_id", "req-http-1");
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            &guest,
        )))
        .with_invocation_context(context)
        .with_secret("EXAMPLE_API_KEY", "delegated-secret"),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes("search", br#"{"query":"verlet"}"#.to_vec())
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8_lossy(&output.output),
        r#"{"results":[{"title":"Delegated"}]}"#
    );
    assert_eq!(
        output.invocation_context.execution,
        verlet_abi::ExecutionPrincipal::system("http-broker")
    );
    assert!(String::from_utf8_lossy(&output.events).contains(r#""status":200"#));
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_http_operation_treats_http_error_status_as_response() {
    let (base_url, server) = spawn_http_server(
        500,
        r#"{"error":"upstream"}"#,
        vec!["POST /search HTTP/1.1"],
    )
    .await;
    let url = format!("{base_url}/search");
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(&url).unwrap()).unwrap();
    let http_grant = format!("net.http.private:POST:{origin}");
    let guest = http_guest(
        &url,
        vec![http_grant.clone(), "secret:EXAMPLE_API_KEY".to_string()],
    );
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            &guest,
        )))
        .with_capability_grant(http_grant)
        .with_capability_grant("secret:EXAMPLE_API_KEY")
        .with_secret("EXAMPLE_API_KEY", "test-secret"),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes("search", br#"{"query":"verlet"}"#.to_vec())
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8_lossy(&output.output),
        r#"{"error":"upstream"}"#
    );
    assert!(String::from_utf8_lossy(&output.events).contains(r#""status":500"#));
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_http_operation_denies_ungranted_network_request() {
    let guest = http_guest("http://127.0.0.1:9/search", Vec::new());
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            &guest,
        ))),
    )
    .unwrap();

    let err = factory
        .invoke_operation_bytes("search", br#"{"query":"verlet"}"#.to_vec())
        .await
        .unwrap_err();

    assert!(err.to_string().contains("returned status 3"));
}

#[tokio::test]
async fn wasm_http_request_truncates_response_to_requested_cap() {
    let (base_url, server) = spawn_http_server(200, r#"{"abcdef":true}"#, Vec::new()).await;
    let url = format!("{base_url}/search");
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(&url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([format!("net.http.private:POST:{origin}")]);

    let exchange = verlet_wasm::runner::execute_http_request(
        http_request_bytes(&url, Some(4), Vec::new()),
        br#"{"query":"verlet"}"#.to_vec(),
        grants,
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap();

    assert_eq!(exchange.body, b"{\"ab");
    assert!(exchange.response.truncated);
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_http_response_envelope_stays_valid_and_within_the_requested_cap() {
    let (base_url, server) = spawn_http_bytes_server(vec![1_u8; 100_000]).await;
    let url = format!("{base_url}/binary");
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(&url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([format!("net.http.private:POST:{origin}")]);
    let request = verlet_wasm::WasmHttpRequest {
        abi: verlet_wasm::runner::HTTP_ABI.to_string(),
        method: "POST".to_string(),
        url,
        headers: Vec::new(),
        secret_headers: Vec::new(),
        secret_header_prefixes: Vec::new(),
        input_mapping: None,
        response_envelope: true,
        timeout_ms: Some(5000),
        max_response_bytes: Some(262_144),
    };

    let exchange = verlet_wasm::runner::execute_http_request(
        serde_json::to_vec(&request).unwrap(),
        Vec::new(),
        grants,
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&exchange.body).unwrap();

    assert!(exchange.body.len() <= 262_144, "{}", exchange.body.len());
    assert_eq!(envelope["status"], 200);
    assert_eq!(envelope["truncated"], true);
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_http_input_mapping_enforces_pinned_parameter_schemas() {
    let url = "http://127.0.0.1:9/items/{id}";
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([format!("net.http.private:GET:{origin}")]);
    let request = verlet_wasm::WasmHttpRequest {
        abi: verlet_wasm::runner::HTTP_ABI.to_string(),
        method: "GET".to_string(),
        url: url.to_string(),
        headers: Vec::new(),
        secret_headers: Vec::new(),
        secret_header_prefixes: Vec::new(),
        input_mapping: Some(serde_json::json!({
            "parameters": [{
                "name": "id",
                "input_property": "id",
                "location": "path",
                "required": true,
                "schema": {"type": "integer"}
            }]
        })),
        response_envelope: true,
        timeout_ms: Some(5000),
        max_response_bytes: Some(262_144),
    };

    let err = verlet_wasm::runner::execute_http_request(
        serde_json::to_vec(&request).unwrap(),
        br#"{"id":"not-an-integer"}"#.to_vec(),
        grants,
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.status, verlet_wasm::runner::STATUS_INVALID_ARGUMENT);
    assert!(err.message.contains("pinned schema"), "{}", err.message);
}

#[tokio::test]
async fn wasm_http_input_mapping_allows_an_omitted_optional_body() {
    let (base_url, server) = spawn_http_server(200, r#"{"ok":true}"#, Vec::new()).await;
    let url = format!("{base_url}/optional");
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(&url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([format!("net.http.private:POST:{origin}")]);
    let request = verlet_wasm::WasmHttpRequest {
        abi: verlet_wasm::runner::HTTP_ABI.to_string(),
        method: "POST".to_string(),
        url,
        headers: Vec::new(),
        secret_headers: Vec::new(),
        secret_header_prefixes: Vec::new(),
        input_mapping: Some(serde_json::json!({
            "request_body": {
                "required": false,
                "input_property": "body",
                "schema": {"type": "object", "additionalProperties": true}
            }
        })),
        response_envelope: false,
        timeout_ms: Some(5000),
        max_response_bytes: Some(262_144),
    };

    let exchange = verlet_wasm::runner::execute_http_request(
        serde_json::to_vec(&request).unwrap(),
        br#"{}"#.to_vec(),
        grants,
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap();

    assert_eq!(exchange.body, br#"{"ok":true}"#);
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_http_rejects_protected_secret_header_injection() {
    let url = "http://127.0.0.1:9/search";
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([
        format!("net.http.private:POST:{origin}"),
        "secret:HOST_OVERRIDE".to_string(),
    ]);
    let request = verlet_wasm::WasmHttpRequest {
        abi: verlet_wasm::runner::HTTP_ABI.to_string(),
        method: "POST".to_string(),
        url: url.to_string(),
        headers: Vec::new(),
        secret_headers: vec![("Host".to_string(), "HOST_OVERRIDE".to_string())],
        secret_header_prefixes: Vec::new(),
        input_mapping: None,
        response_envelope: false,
        timeout_ms: Some(5000),
        max_response_bytes: Some(262_144),
    };

    let err = verlet_wasm::runner::execute_http_request(
        serde_json::to_vec(&request).unwrap(),
        Vec::new(),
        grants,
        std::collections::BTreeMap::from([(
            "HOST_OVERRIDE".to_string(),
            "other.example".to_string(),
        )]),
    )
    .await
    .unwrap_err();

    assert_eq!(err.status, verlet_wasm::runner::STATUS_INVALID_ARGUMENT);
    assert!(
        err.message.contains("forbidden HTTP header"),
        "{}",
        err.message
    );
    assert!(!err.message.contains("HOST_OVERRIDE"), "{}", err.message);
}

#[tokio::test]
async fn wasm_http_request_does_not_follow_redirects() {
    let (base_url, server) = spawn_http_redirect_server("http://127.0.0.1:1/private").await;
    let url = format!("{base_url}/redirect");
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(&url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([format!("net.http.private:POST:{origin}")]);

    let exchange = verlet_wasm::runner::execute_http_request(
        http_request_bytes(&url, None, Vec::new()),
        Vec::new(),
        grants,
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap();

    assert_eq!(exchange.response.status, 302);
    assert!(exchange.body.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_http_request_requires_private_grant_for_loopback() {
    let url = "http://127.0.0.1:9/search";
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([format!("net.http:POST:{origin}")]);

    let err = verlet_wasm::runner::execute_http_request(
        http_request_bytes(url, None, Vec::new()),
        Vec::new(),
        grants,
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.status, verlet_wasm::runner::STATUS_CAPABILITY_DENIED);
    assert!(err.message.contains("net.http.private:POST"));
}

#[test]
fn wasm_http_capability_allows_public_origin_wildcards() {
    let grants = std::collections::BTreeSet::from([
        "net.http:GET:https://*".to_string(),
        "net.http:GET:http://*".to_string(),
    ]);

    verlet_wasm::runner::ensure_http_capability(
        &grants,
        &reqwest::Method::GET,
        "https://example.com",
        false,
    )
    .unwrap();
    verlet_wasm::runner::ensure_http_capability(
        &grants,
        &reqwest::Method::GET,
        "http://news.example:8080",
        false,
    )
    .unwrap();
    let err = verlet_wasm::runner::ensure_http_capability(
        &grants,
        &reqwest::Method::POST,
        "https://example.com",
        false,
    )
    .unwrap_err();
    assert_eq!(err.status, verlet_wasm::runner::STATUS_CAPABILITY_DENIED);
}

#[test]
fn wasm_http_capability_wildcards_do_not_cross_private_namespace() {
    let grants = std::collections::BTreeSet::from(["net.http:GET:http://*".to_string()]);

    let err = verlet_wasm::runner::ensure_http_capability(
        &grants,
        &reqwest::Method::GET,
        "http://127.0.0.1:9",
        true,
    )
    .unwrap_err();

    assert_eq!(err.status, verlet_wasm::runner::STATUS_CAPABILITY_DENIED);
    assert!(err.message.contains("net.http.private:GET"));
}

#[test]
fn wasm_http_capability_allows_method_wildcards() {
    let grants =
        std::collections::BTreeSet::from(["net.http:*:https://api.example.com".to_string()]);

    verlet_wasm::runner::ensure_http_capability(
        &grants,
        &reqwest::Method::GET,
        "https://api.example.com",
        false,
    )
    .unwrap();
    verlet_wasm::runner::ensure_http_capability(
        &grants,
        &reqwest::Method::POST,
        "https://api.example.com",
        false,
    )
    .unwrap();
}

#[tokio::test]
async fn wasm_http_secret_diagnostics_redact_secret_names() {
    let url = "http://127.0.0.1:9/search";
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(url).unwrap()).unwrap();
    let secret_headers = vec![("x-api-key".to_string(), "EXAMPLE_API_KEY".to_string())];
    let grants = std::collections::BTreeSet::from([format!("net.http.private:POST:{origin}")]);

    let missing_grant = verlet_wasm::runner::execute_http_request(
        http_request_bytes(url, None, secret_headers.clone()),
        Vec::new(),
        grants.clone(),
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        missing_grant.status,
        verlet_wasm::runner::STATUS_CAPABILITY_DENIED
    );
    assert!(!missing_grant.message.contains("EXAMPLE_API_KEY"));
    assert_eq!(missing_grant.message, "missing required secret capability");

    let missing_secret = verlet_wasm::runner::execute_http_request(
        http_request_bytes(url, None, secret_headers),
        Vec::new(),
        grants
            .into_iter()
            .chain(["secret:EXAMPLE_API_KEY".to_string()])
            .collect(),
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        missing_secret.status,
        verlet_wasm::runner::STATUS_CAPABILITY_DENIED
    );
    assert!(!missing_secret.message.contains("EXAMPLE_API_KEY"));
    assert_eq!(missing_secret.message, "required secret is not available");
}

#[tokio::test]
async fn wasm_runtime_accepts_cancel_during_guest_execution() {
    let factory = std::sync::Arc::new(
        crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
            verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
                wat_guest(loop_guest()),
            ))
            .with_fuel(Some(u64::MAX))
            .with_fuel_yield_interval(Some(1_000)),
        )
        .unwrap(),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory);
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "spin")
        .await
        .unwrap();
    host.cancel(thread.context().coordinates.thread_id, "stop wasm")
        .await
        .unwrap();

    assert_eq!(next_cancelled(&mut events).await, "stop wasm");
}

#[tokio::test]
async fn wasm_runtime_rejects_ungranted_wasi_imports() {
    let factory = std::sync::Arc::new(
        crate::capabilities::wasm_runner::WasmRuntimeFactory::from_bytes(wat_guest(
            ambient_wasi_guest(),
        ))
        .unwrap(),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory);
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "try ambient wasi",
    )
    .await
    .unwrap();

    let message = next_failed(&mut events).await;
    assert!(message.contains("wasi_snapshot_preview1"));
}
