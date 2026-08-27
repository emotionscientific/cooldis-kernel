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

fn wasm_agent_tool_guest(module: &str) -> Vec<u8> {
    let module_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("agent-tools")
        .join("wasm")
        .join(module);
    let build = crate::operations::operation_builder::build_rust_wasm_module(
        crate::operations::operation_builder::RustWasmBuildOptions::new(module_dir),
    )
    .unwrap_or_else(|error| panic!("failed to build {module} agent tool Wasm module: {error}"));
    std::fs::read(build.artifact_path)
        .unwrap_or_else(|error| panic!("failed to read {module} agent tool Wasm module: {error}"))
}

fn wasm_read_tool_guest() -> Vec<u8> {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| wasm_agent_tool_guest("read")).clone()
}

fn wasm_write_tool_guest() -> Vec<u8> {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| wasm_agent_tool_guest("write")).clone()
}

fn wasm_edit_tool_guest() -> Vec<u8> {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| wasm_agent_tool_guest("edit")).clone()
}

fn wasm_search_tool_guest() -> Vec<u8> {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| wasm_agent_tool_guest("search")).clone()
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
            },
            {
                "id": 4,
                "name": "unclosed_write",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 5,
                "name": "mkdir_recursive",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 6,
                "name": "mkdir_non_recursive",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 7,
                "name": "write_to_read",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 8,
                "name": "read_from_write",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 9,
                "name": "unknown_write",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 10,
                "name": "stat_record",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 11,
                "name": "mkdir_missing_parent",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 12,
                "name": "bad_write_pointer",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 13,
                "name": "failed_close_consumes_handle",
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": []
            },
            {
                "id": 14,
                "name": "mkdir_invalid_recursive",
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
    let pending_path = b"/workspace/pending.txt";
    let recursive_path = b"/workspace/recursive/child";
    let nonrecursive_path = b"/workspace/nonrecursive";
    let missing_parent_dir = b"/workspace/missing";
    let missing_parent_path = b"/workspace/missing/child.txt";
    render_vfs_fixture(WASM_VFS_PROBE_FIXTURE_TEMPLATE, &manifest)
        .replace("{{path}}", &wat_bytes(path))
        .replace("{{path_len}}", &path.len().to_string())
        .replace("{{pending_path}}", &wat_bytes(pending_path))
        .replace("{{pending_path_len}}", &pending_path.len().to_string())
        .replace("{{recursive_path}}", &wat_bytes(recursive_path))
        .replace("{{recursive_path_len}}", &recursive_path.len().to_string())
        .replace("{{nonrecursive_path}}", &wat_bytes(nonrecursive_path))
        .replace(
            "{{nonrecursive_path_len}}",
            &nonrecursive_path.len().to_string(),
        )
        .replace("{{missing_parent_path}}", &wat_bytes(missing_parent_path))
        .replace(
            "{{missing_parent_path_len}}",
            &missing_parent_path.len().to_string(),
        )
        .replace(
            "{{missing_parent_dir_len}}",
            &missing_parent_dir.len().to_string(),
        )
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
        .replace(
            "{{write_mode}}",
            &verlet_wasm::runner::FS_MODE_WRITE.to_string(),
        )
        .replace(
            "{{invalid_mode}}",
            &(verlet_wasm::runner::FS_MODE_WRITE + 1).to_string(),
        )
        .replace(
            "{{invalid_argument}}",
            &verlet_wasm::runner::STATUS_INVALID_ARGUMENT.to_string(),
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

async fn writable_vfs() -> std::sync::Arc<verlet_vfs::VerletVfs> {
    let vfs = std::sync::Arc::new(verlet_vfs::VerletVfs::new(std::sync::Arc::new(
        bashkit::InMemoryFs::new(),
    )));
    vfs.mount(
        "/workspace",
        std::sync::Arc::new(bashkit::InMemoryFs::new()),
    )
    .unwrap();
    vfs
}

async fn pi_tool_fixture() -> (std::sync::Arc<verlet_vfs::VerletVfs>, tempfile::TempDir) {
    let vfs = writable_vfs().await;
    let native = tempfile::tempdir().unwrap();
    for (relative, content) in [
        ("input.txt", "alpha\nneedle beta\ngamma\n"),
        ("src/app.rs", "fn needle() {}\n"),
        ("src/nested/info.txt", "quiet\n"),
    ] {
        let vfs_path = std::path::Path::new("/workspace/project").join(relative);
        if let Some(parent) = vfs_path.parent() {
            vfs.mkdir(parent, true).await.unwrap();
        }
        vfs.write_file(&vfs_path, content.as_bytes()).await.unwrap();

        let native_path = native.path().join("project").join(relative);
        if let Some(parent) = native_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(native_path, content).unwrap();
    }
    (vfs, native)
}

async fn write_pi_tool_fixture_file(
    vfs: &std::sync::Arc<verlet_vfs::VerletVfs>,
    native: &tempfile::TempDir,
    relative: &str,
    content: &[u8],
) {
    let vfs_path = std::path::Path::new("/workspace").join(relative);
    if let Some(parent) = vfs_path.parent() {
        vfs.mkdir(parent, true).await.unwrap();
    }
    vfs.write_file(&vfs_path, content).await.unwrap();

    let native_path = native.path().join(relative);
    if let Some(parent) = native_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(native_path, content).unwrap();
}

fn pi_tool_input(args: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "root": "/workspace",
        "args": args,
    }))
    .unwrap()
}

fn tool_envelope(value: serde_json::Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn tool_success_envelope(output: impl serde::Serialize) -> Vec<u8> {
    tool_envelope(serde_json::json!({"ok": output}))
}

fn tool_error_envelope(error: impl Into<String>) -> Vec<u8> {
    tool_envelope(serde_json::json!({"error": error.into()}))
}

fn tool_output(output: &verlet_process::process::WasmOperationOutput) -> serde_json::Value {
    serde_json::from_slice(&output.output).unwrap()
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

fn http_attachment_config(
    origin: &str,
    method: &str,
    allowed_secrets: impl IntoIterator<Item = &'static str>,
) -> verlet_wasm::WasmAttachmentConfig {
    verlet_wasm::WasmAttachmentConfig {
        allowed_secrets: allowed_secrets.into_iter().map(String::from).collect(),
        allowed_private_network: std::collections::BTreeMap::from([(
            origin.to_string(),
            std::collections::BTreeSet::from([method.to_string()]),
        )]),
    }
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
async fn wasm_vfs_close_accepts_read_handle_and_rejects_double_close() {
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
async fn wasm_vfs_write_guest_creates_replaces_stats_and_lists() {
    let vfs = writable_vfs().await;
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_vfs_tools_guest(),
        ))
        .with_vfs(vfs.clone())
        .with_capability_grant(verlet_wasm::runner::FS_WRITE_CAPABILITY),
    )
    .unwrap();

    factory
        .invoke_operation_bytes(
            "put",
            serde_json::to_vec(&serde_json::json!({
                "path": "/workspace/nested/file.txt",
                "content": "first payload",
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        vfs.read_file(std::path::Path::new("/workspace/nested/file.txt"))
            .await
            .unwrap(),
        b"first payload"
    );

    factory
        .invoke_operation_bytes(
            "put",
            serde_json::to_vec(&serde_json::json!({
                "path": "/workspace/nested/file.txt",
                "content": "replacement",
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        vfs.read_file(std::path::Path::new("/workspace/nested/file.txt"))
            .await
            .unwrap(),
        b"replacement"
    );

    for (path, content) in [
        ("/workspace/z.txt", "z"),
        ("/workspace/a.txt", "alpha"),
        ("/workspace/é.txt", "accent"),
    ] {
        factory
            .invoke_operation_bytes(
                "put",
                serde_json::to_vec(&serde_json::json!({
                    "path": path,
                    "content": content,
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }

    let stat = factory
        .invoke_operation_bytes("stat", b"/workspace/nested/file.txt".to_vec())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stat.output).unwrap(),
        serde_json::json!({"kind": "file", "size": 11})
    );
    let stat = factory
        .invoke_operation_bytes("stat", b"/workspace/nested".to_vec())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stat.output).unwrap(),
        serde_json::json!({"kind": "dir", "size": 0})
    );
    let missing = factory
        .invoke_operation_bytes("stat", b"/workspace/missing.txt".to_vec())
        .await
        .unwrap_err()
        .to_string();
    assert!(missing.contains("returned status 2"), "{missing}");
    let missing = factory
        .invoke_operation_bytes("ls", b"/workspace/missing".to_vec())
        .await
        .unwrap_err()
        .to_string();
    assert!(missing.contains("returned status 2"), "{missing}");

    let list = factory
        .invoke_operation_bytes("ls", b"/workspace".to_vec())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&list.output).unwrap(),
        serde_json::json!([
            {"name": "a.txt", "is_dir": false},
            {"name": "dev", "is_dir": true},
            {"name": "home", "is_dir": true},
            {"name": "nested", "is_dir": true},
            {"name": "tmp", "is_dir": true},
            {"name": "z.txt", "is_dir": false},
            {"name": "é.txt", "is_dir": false},
        ])
    );
}

#[tokio::test]
async fn wasm_vfs_write_guest_requires_declared_capability_before_execution() {
    let vfs = writable_vfs().await;
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_vfs_tools_guest(),
        ))
        .with_vfs(vfs.clone()),
    )
    .unwrap();

    let err = factory
        .invoke_operation_bytes(
            "put",
            br#"{"path":"/workspace/denied/file.txt","content":"denied"}"#.to_vec(),
        )
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("requires ungranted capabilities"), "{err}");
    assert!(err.contains(verlet_wasm::runner::FS_WRITE_CAPABILITY));
    assert!(
        !vfs.exists(std::path::Path::new("/workspace/denied"))
            .await
            .unwrap()
    );
    factory
        .invoke_operation_bytes("stat", b"/workspace".to_vec())
        .await
        .unwrap();
    factory
        .invoke_operation_bytes("ls", b"/workspace".to_vec())
        .await
        .unwrap();
}

#[tokio::test]
async fn wasm_pi_read_matches_native_envelope_and_surfaces_tool_errors() {
    let (vfs, native) = pi_tool_fixture().await;
    let oversized = b"oversized\n".repeat(
        verlet_tool_core::MAX_FILE_BYTES
            .saturating_div(b"oversized\n".len())
            .saturating_add(1),
    );
    write_pi_tool_fixture_file(&vfs, &native, "project/oversized.txt", &oversized).await;
    let native_fs = verlet_tool_core::StdFs::new(native.path()).unwrap();
    let args = verlet_tool_read::ReadArgs {
        path: std::path::PathBuf::from("project/input.txt"),
        offset: Some(2),
        limit: Some(1),
    };
    let native_output = verlet_tool_read::run(args, &native_fs).unwrap();
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_read_tool_guest(),
        ))
        .with_vfs(vfs),
    )
    .unwrap();
    let manifest = factory.validate_operation_artifact().await.unwrap();
    assert!(manifest.operation("read").is_some());

    let output = factory
        .invoke_operation_bytes(
            "read",
            pi_tool_input(serde_json::json!({
                "path": "project/input.txt",
                "offset": 2,
                "limit": 1,
            })),
        )
        .await
        .unwrap();

    assert_eq!(output.output, tool_success_envelope(native_output));
    assert_eq!(output.operation.name, "read");

    let native_error = verlet_tool_read::run(
        verlet_tool_read::ReadArgs {
            path: std::path::PathBuf::from("project/missing.txt"),
            offset: None,
            limit: None,
        },
        &native_fs,
    )
    .unwrap_err();
    let missing = factory
        .invoke_operation_bytes(
            "read",
            pi_tool_input(serde_json::json!({"path": "project/missing.txt"})),
        )
        .await
        .unwrap();
    assert_eq!(
        missing.output,
        tool_error_envelope(native_error.to_string())
    );

    let native_directory_error = verlet_tool_read::run(
        verlet_tool_read::ReadArgs {
            path: std::path::PathBuf::from("project"),
            offset: None,
            limit: None,
        },
        &native_fs,
    )
    .unwrap_err();
    let directory = factory
        .invoke_operation_bytes(
            "read",
            pi_tool_input(serde_json::json!({"path": "project"})),
        )
        .await
        .unwrap();
    assert_eq!(
        directory.output,
        tool_error_envelope(native_directory_error.to_string())
    );

    let native_oversized_error = verlet_tool_read::run(
        verlet_tool_read::ReadArgs {
            path: std::path::PathBuf::from("project/oversized.txt"),
            offset: None,
            limit: None,
        },
        &native_fs,
    )
    .unwrap_err();
    let oversized = factory
        .invoke_operation_bytes(
            "read",
            pi_tool_input(serde_json::json!({"path": "project/oversized.txt"})),
        )
        .await
        .unwrap();
    assert_eq!(
        oversized.output,
        tool_error_envelope(native_oversized_error.to_string())
    );
}

#[tokio::test]
async fn wasm_pi_find_and_grep_match_native_envelopes_and_tool_errors() {
    let (vfs, native) = pi_tool_fixture().await;
    write_pi_tool_fixture_file(&vfs, &native, "project/non-utf8.bin", b"\xffneedle\n").await;
    let oversized = b"needle\n".repeat(
        verlet_tool_core::MAX_FILE_BYTES
            .saturating_div(b"needle\n".len())
            .saturating_add(1),
    );
    write_pi_tool_fixture_file(
        &vfs,
        &native,
        "project/build/oversized.artifact",
        &oversized,
    )
    .await;
    let native_fs = verlet_tool_core::StdFs::new(native.path()).unwrap();
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_search_tool_guest(),
        ))
        .with_vfs(vfs),
    )
    .unwrap();
    let manifest = factory.validate_operation_artifact().await.unwrap();
    assert!(manifest.operation("find").is_some());
    assert!(manifest.operation("grep").is_some());

    let native_find = verlet_tool_glob::run(
        verlet_tool_glob::GlobArgs {
            pattern: "**".to_owned(),
            path: Some(std::path::PathBuf::from("project")),
            limit: None,
        },
        &native_fs,
    )
    .unwrap();
    assert!(native_find.paths.contains(&"src/".to_owned()));
    assert!(native_find.paths.contains(&"src/nested/".to_owned()));
    let find = factory
        .invoke_operation_bytes(
            "find",
            pi_tool_input(serde_json::json!({"pattern": "**", "path": "project"})),
        )
        .await
        .unwrap();
    assert_eq!(find.output, tool_success_envelope(native_find));

    let native_find_error = verlet_tool_glob::run(
        verlet_tool_glob::GlobArgs {
            pattern: "[".to_owned(),
            path: None,
            limit: None,
        },
        &native_fs,
    )
    .unwrap_err();
    let find_error = factory
        .invoke_operation_bytes("find", pi_tool_input(serde_json::json!({"pattern": "["})))
        .await
        .unwrap();
    assert_eq!(
        find_error.output,
        tool_error_envelope(native_find_error.to_string())
    );

    let native_find_kind_error = verlet_tool_glob::run(
        verlet_tool_glob::GlobArgs {
            pattern: "**".to_owned(),
            path: Some(std::path::PathBuf::from("project/input.txt")),
            limit: None,
        },
        &native_fs,
    )
    .unwrap_err();
    let find_kind_error = factory
        .invoke_operation_bytes(
            "find",
            pi_tool_input(serde_json::json!({
                "pattern": "**",
                "path": "project/input.txt",
            })),
        )
        .await
        .unwrap();
    assert_eq!(
        find_kind_error.output,
        tool_error_envelope(native_find_kind_error.to_string())
    );

    let native_grep = verlet_tool_grep::run(
        verlet_tool_grep::GrepArgs {
            pattern: "needle".to_owned(),
            path: Some(std::path::PathBuf::from("project")),
            glob: None,
            ignore_case: false,
            literal: false,
            context: None,
            limit: None,
        },
        &native_fs,
    )
    .unwrap();
    assert_eq!(
        native_grep.text,
        "input.txt:2: needle beta\nsrc/app.rs:1: fn needle() {}"
    );
    let grep = factory
        .invoke_operation_bytes(
            "grep",
            pi_tool_input(serde_json::json!({"pattern": "needle", "path": "project"})),
        )
        .await
        .unwrap();
    assert_eq!(grep.output, tool_success_envelope(native_grep));

    let native_grep_error = verlet_tool_grep::run(
        verlet_tool_grep::GrepArgs {
            pattern: "[".to_owned(),
            path: None,
            glob: None,
            ignore_case: false,
            literal: false,
            context: None,
            limit: None,
        },
        &native_fs,
    )
    .unwrap_err();
    let grep_error = factory
        .invoke_operation_bytes("grep", pi_tool_input(serde_json::json!({"pattern": "["})))
        .await
        .unwrap();
    assert_eq!(
        grep_error.output,
        tool_error_envelope(native_grep_error.to_string())
    );
}

#[tokio::test]
async fn wasm_pi_write_matches_native_mutation_and_maps_io_into_envelope() {
    let vfs = writable_vfs().await;
    let native = tempfile::tempdir().unwrap();
    let native_fs = verlet_tool_core::StdFs::new(native.path()).unwrap();
    let native_output = verlet_tool_write::run(
        verlet_tool_write::WriteArgs {
            path: std::path::PathBuf::from("nested/unicode.txt"),
            content: "é🙂".to_owned(),
        },
        &native_fs,
    )
    .unwrap();
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_write_tool_guest(),
        ))
        .with_vfs(vfs.clone())
        .with_capability_grant(verlet_wasm::runner::FS_WRITE_CAPABILITY),
    )
    .unwrap();
    let manifest = factory.validate_operation_artifact().await.unwrap();
    assert!(manifest.operation("write").is_some());

    let output = factory
        .invoke_operation_bytes(
            "write",
            pi_tool_input(serde_json::json!({
                "path": "nested/unicode.txt",
                "content": "é🙂",
            })),
        )
        .await
        .unwrap();

    assert_eq!(output.output, tool_success_envelope(native_output));
    assert_eq!(
        tool_output(&output)["ok"]["text"],
        "Successfully wrote 3 bytes to nested/unicode.txt"
    );
    assert_eq!(
        vfs.read_file(std::path::Path::new("/workspace/nested/unicode.txt"))
            .await
            .unwrap(),
        "é🙂".as_bytes()
    );

    let io_error = factory
        .invoke_operation_bytes(
            "write",
            pi_tool_input(serde_json::json!({"path": "", "content": "no"})),
        )
        .await
        .unwrap();
    let error = tool_output(&io_error)["error"].as_str().unwrap().to_owned();
    assert!(error.contains("failed with status"), "{error}");

    #[derive(Debug, serde::Deserialize)]
    struct NativeWriteInput {
        #[serde(rename = "root")]
        _root: std::path::PathBuf,
        #[serde(rename = "args")]
        _args: verlet_tool_write::WriteArgs,
    }
    let malformed = pi_tool_input(serde_json::json!({"path": "missing-content.txt"}));
    let native_error = serde_json::from_slice::<NativeWriteInput>(&malformed).unwrap_err();
    let malformed_output = factory
        .invoke_operation_bytes("write", malformed)
        .await
        .unwrap();
    assert_eq!(
        malformed_output.output,
        tool_error_envelope(format!("invalid input JSON: {native_error}"))
    );
}

#[tokio::test]
async fn wasm_pi_edit_matches_native_mutation_and_validation_envelope() {
    let (vfs, native) = pi_tool_fixture().await;
    let native_fs = verlet_tool_core::StdFs::new(native.path()).unwrap();
    let raw_args = serde_json::json!({
        "path": "project/input.txt",
        "edits": [{"oldText": "needle beta", "newText": "needle delta"}],
    });
    let native_args = verlet_tool_edit::parse_cli_args(raw_args.clone()).unwrap();
    let native_output = verlet_tool_edit::run(native_args, &native_fs).unwrap();
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_edit_tool_guest(),
        ))
        .with_vfs(vfs.clone())
        .with_capability_grant(verlet_wasm::runner::FS_WRITE_CAPABILITY),
    )
    .unwrap();
    let manifest = factory.validate_operation_artifact().await.unwrap();
    assert!(manifest.operation("edit").is_some());

    let output = factory
        .invoke_operation_bytes("edit", pi_tool_input(raw_args))
        .await
        .unwrap();

    assert_eq!(output.output, tool_success_envelope(native_output));
    assert_eq!(
        vfs.read_file(std::path::Path::new("/workspace/project/input.txt"))
            .await
            .unwrap(),
        b"alpha\nneedle delta\ngamma\n"
    );

    let invalid_args = serde_json::json!({"path": "project/input.txt"});
    let native_error = verlet_tool_edit::parse_cli_args(invalid_args.clone()).unwrap_err();
    let validation = factory
        .invoke_operation_bytes("edit", pi_tool_input(invalid_args))
        .await
        .unwrap();
    assert_eq!(validation.output, tool_error_envelope(native_error));
}

#[tokio::test]
async fn wasm_pi_write_and_edit_require_fs_write_before_execution() {
    let (vfs, _native) = pi_tool_fixture().await;
    let write_factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_write_tool_guest(),
        ))
        .with_vfs(vfs.clone()),
    )
    .unwrap();
    let edit_factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(
            wasm_edit_tool_guest(),
        ))
        .with_vfs(vfs.clone()),
    )
    .unwrap();

    let write_error = write_factory
        .invoke_operation_bytes(
            "write",
            pi_tool_input(serde_json::json!({
                "path": "denied.txt",
                "content": "denied",
            })),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(write_error.contains("requires ungranted capabilities"));
    assert!(write_error.contains(verlet_wasm::runner::FS_WRITE_CAPABILITY));
    assert!(
        !vfs.exists(std::path::Path::new("/workspace/denied.txt"))
            .await
            .unwrap()
    );

    let edit_error = edit_factory
        .invoke_operation_bytes(
            "edit",
            pi_tool_input(serde_json::json!({
                "path": "project/input.txt",
                "edits": [{"oldText": "needle beta", "newText": "denied"}],
            })),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(edit_error.contains("requires ungranted capabilities"));
    assert!(edit_error.contains(verlet_wasm::runner::FS_WRITE_CAPABILITY));
    assert_eq!(
        vfs.read_file(std::path::Path::new("/workspace/project/input.txt"))
            .await
            .unwrap(),
        b"alpha\nneedle beta\ngamma\n"
    );
}

#[tokio::test]
async fn wasm_vfs_mutation_imports_require_exact_write_capability() {
    let vfs = wasm_cat_vfs().await;
    for grant in [None, Some("fs.write:/workspace")] {
        let mut config = verlet_wasm::WasmRuntimeConfig::new(
            verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(wasm_vfs_probe_guest())),
        )
        .with_vfs(vfs.clone());
        if let Some(grant) = grant {
            config = config.with_capability_grant(grant);
        }
        let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(config).unwrap();
        for operation in ["unclosed_write", "mkdir_recursive"] {
            let err = factory
                .invoke_operation_bytes(operation, Vec::new())
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("returned status 3"), "{operation}: {err}");
        }
    }

    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            wasm_vfs_probe_guest(),
        )))
        .with_vfs(vfs.clone())
        .with_capability_grant(verlet_wasm::runner::FS_WRITE_CAPABILITY),
    )
    .unwrap();
    factory
        .invoke_operation_bytes("unclosed_write", Vec::new())
        .await
        .unwrap();
    assert!(
        !vfs.exists(std::path::Path::new("/workspace/pending.txt"))
            .await
            .unwrap()
    );
    factory
        .invoke_operation_bytes("mkdir_recursive", Vec::new())
        .await
        .unwrap();
    factory
        .invoke_operation_bytes("mkdir_recursive", Vec::new())
        .await
        .unwrap();
    assert!(
        vfs.stat(std::path::Path::new("/workspace/recursive/child"))
            .await
            .unwrap()
            .file_type
            .is_dir()
    );
}

#[tokio::test]
async fn wasm_vfs_write_handles_fail_closed_and_failed_close_consumes_handle() {
    let vfs = wasm_cat_vfs().await;
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            wasm_vfs_probe_guest(),
        )))
        .with_vfs(vfs.clone())
        .with_capability_grant(verlet_wasm::runner::FS_WRITE_CAPABILITY),
    )
    .unwrap();

    for operation in ["write_to_read", "read_from_write"] {
        let err = factory
            .invoke_operation_bytes(operation, Vec::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("returned status 1"), "{operation}: {err}");
    }
    let err = factory
        .invoke_operation_bytes("unknown_write", Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("returned status 2"), "{err}");
    factory
        .invoke_operation_bytes("bad_write_pointer", Vec::new())
        .await
        .unwrap();
    assert_eq!(
        vfs.read_file(std::path::Path::new("/workspace/pending.txt"))
            .await
            .unwrap(),
        b"pending"
    );

    let err = factory
        .invoke_operation_bytes("failed_close_consumes_handle", Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("returned status 2"), "{err}");
    assert!(
        vfs.exists(std::path::Path::new("/workspace/missing"))
            .await
            .unwrap()
    );
    assert!(
        !vfs.exists(std::path::Path::new("/workspace/missing/child.txt"))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn wasm_vfs_mkdir_honors_recursive_and_non_recursive_semantics() {
    let vfs = wasm_cat_vfs().await;
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            wasm_vfs_probe_guest(),
        )))
        .with_vfs(vfs)
        .with_capability_grant(verlet_wasm::runner::FS_WRITE_CAPABILITY),
    )
    .unwrap();

    let missing_parent = factory
        .invoke_operation_bytes("mkdir_missing_parent", Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        missing_parent.contains("returned status 2"),
        "{missing_parent}"
    );
    let invalid_recursive = factory
        .invoke_operation_bytes("mkdir_invalid_recursive", Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        invalid_recursive.contains("returned status 1"),
        "{invalid_recursive}"
    );

    factory
        .invoke_operation_bytes("mkdir_non_recursive", Vec::new())
        .await
        .unwrap();
    let existing = factory
        .invoke_operation_bytes("mkdir_non_recursive", Vec::new())
        .await
        .unwrap_err()
        .to_string();
    assert!(existing.contains("returned status 4"), "{existing}");
}

#[tokio::test]
async fn wasm_vfs_stat_writes_exact_little_endian_record() {
    let content = b"alpha\nbeta\ngamma from verlet vfs\n";
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            wasm_vfs_probe_guest(),
        )))
        .with_vfs(wasm_cat_vfs().await),
    )
    .unwrap();

    let output = factory
        .invoke_operation_bytes("stat_record", Vec::new())
        .await
        .unwrap();
    let mut expected = [0u8; 16];
    expected[8..16].copy_from_slice(&(content.len() as u64).to_le_bytes());
    assert_eq!(output.output, expected);
}

#[tokio::test]
async fn pure_compute_policy_rejects_every_fs_import() {
    let guest = r#"
        (module
          (import "cooldis_0.1" "fs_open" (func $fs_open (param i32 i32 i32 i32) (result i32)))
          (import "cooldis_0.1" "fs_read" (func $fs_read (param i32 i32 i32) (result i32)))
          (import "cooldis_0.1" "fs_close" (func $fs_close (param i32) (result i32)))
          (import "cooldis_0.1" "fs_write" (func $fs_write (param i32 i32 i32) (result i32)))
          (import "cooldis_0.1" "fs_stat" (func $fs_stat (param i32 i32 i32) (result i32)))
          (import "cooldis_0.1" "fs_list" (func $fs_list (param i32 i32 i32) (result i32)))
          (import "cooldis_0.1" "fs_mkdir" (func $fs_mkdir (param i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (func (export "__verlet_describe_module__") (param i32) (result i32)
            i32.const 0)
          (func (export "__verlet_call_operation__") (param i32 i32 i32 i32 i32) (result i32)
            i32.const 0))
        "#;
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            guest,
        )))
        .with_host_import_policy(verlet_wasm::WasmHostImportPolicy::PureCompute),
    )
    .unwrap();

    let err = factory
        .validate_operation_artifact()
        .await
        .unwrap_err()
        .to_string();
    for import in [
        "fs_open", "fs_read", "fs_close", "fs_write", "fs_stat", "fs_list", "fs_mkdir",
    ] {
        assert!(err.contains(import), "missing {import} in {err}");
    }
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
        .with_attachment_config(http_attachment_config(&origin, "POST", ["EXAMPLE_API_KEY"]))
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
        .with_attachment_config(http_attachment_config(&origin, "POST", ["EXAMPLE_API_KEY"]))
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
        .with_attachment_config(http_attachment_config(&origin, "POST", ["EXAMPLE_API_KEY"]))
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
        http_attachment_config(&origin, "POST", []),
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
        http_attachment_config(&origin, "POST", []),
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
        http_attachment_config(&origin, "GET", []),
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
        http_attachment_config(&origin, "POST", []),
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
        http_attachment_config(&origin, "POST", ["HOST_OVERRIDE"]),
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
        http_attachment_config(&origin, "POST", []),
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
        verlet_wasm::WasmAttachmentConfig::default(),
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.status, verlet_wasm::runner::STATUS_CAPABILITY_DENIED);
    assert!(err.message.contains("net.http.private:POST"));
}

#[test]
fn wasm_private_http_attachment_config_allows_only_listed_origin_and_method() {
    let origin = "http://127.0.0.1:9000";
    let attachment_config = verlet_wasm::WasmAttachmentConfig {
        allowed_secrets: std::collections::BTreeSet::new(),
        allowed_private_network: std::collections::BTreeMap::from([(
            origin.to_string(),
            std::collections::BTreeSet::from(["POST".to_string()]),
        )]),
    };

    verlet_wasm::runner::ensure_http_capability(
        &std::collections::BTreeSet::new(),
        &attachment_config,
        &reqwest::Method::POST,
        origin,
        true,
    )
    .unwrap();

    for (method, denied_origin) in [
        (reqwest::Method::GET, origin),
        (reqwest::Method::POST, "http://127.0.0.1:9001"),
    ] {
        let err = verlet_wasm::runner::ensure_http_capability(
            &std::collections::BTreeSet::new(),
            &attachment_config,
            &method,
            denied_origin,
            true,
        )
        .unwrap_err();
        assert_eq!(err.status, verlet_wasm::runner::STATUS_CAPABILITY_DENIED);
    }
}

#[tokio::test]
async fn wasm_http_attachment_config_injects_only_listed_secret() {
    let (base_url, server) =
        spawn_http_server(200, r#"{"ok":true}"#, vec!["x-api-key: test-secret"]).await;
    let url = format!("{base_url}/search");
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(&url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([
        format!("net.http.private:POST:{origin}"),
        "secret:EXAMPLE_API_KEY".to_string(),
    ]);
    let secrets = std::collections::BTreeMap::from([(
        "EXAMPLE_API_KEY".to_string(),
        "test-secret".to_string(),
    )]);
    let private_network = std::collections::BTreeMap::from([(
        origin,
        std::collections::BTreeSet::from(["POST".to_string()]),
    )]);

    let denied = verlet_wasm::runner::execute_http_request(
        http_request_bytes(
            &url,
            None,
            vec![("x-api-key".to_string(), "EXAMPLE_API_KEY".to_string())],
        ),
        Vec::new(),
        grants,
        verlet_wasm::WasmAttachmentConfig {
            allowed_secrets: std::collections::BTreeSet::new(),
            allowed_private_network: private_network.clone(),
        },
        secrets.clone(),
    )
    .await
    .unwrap_err();
    assert_eq!(denied.status, verlet_wasm::runner::STATUS_CAPABILITY_DENIED);
    assert_eq!(denied.message, "missing required secret capability");

    let exchange = verlet_wasm::runner::execute_http_request(
        http_request_bytes(
            &url,
            None,
            vec![("x-api-key".to_string(), "EXAMPLE_API_KEY".to_string())],
        ),
        Vec::new(),
        std::collections::BTreeSet::new(),
        verlet_wasm::WasmAttachmentConfig {
            allowed_secrets: std::collections::BTreeSet::from(["EXAMPLE_API_KEY".to_string()]),
            allowed_private_network: private_network,
        },
        secrets,
    )
    .await
    .unwrap();

    assert_eq!(exchange.response.status, 200);
    server.await.unwrap();
}

#[tokio::test]
async fn wasm_http_attachment_without_config_denies_private_network_and_secret_injection() {
    let url = "http://127.0.0.1:9/search";
    let origin = verlet_wasm::runner::http_origin(&reqwest::Url::parse(url).unwrap()).unwrap();
    let grants = std::collections::BTreeSet::from([format!("net.http.private:POST:{origin}")]);

    let err = verlet_wasm::runner::execute_http_request(
        http_request_bytes(url, None, Vec::new()),
        Vec::new(),
        grants,
        verlet_wasm::WasmAttachmentConfig::default(),
        std::collections::BTreeMap::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(err.status, verlet_wasm::runner::STATUS_CAPABILITY_DENIED);
    assert!(err.message.contains("net.http.private:POST"));

    let public_url = "https://example.com/search";
    let public_origin =
        verlet_wasm::runner::http_origin(&reqwest::Url::parse(public_url).unwrap()).unwrap();
    let err = verlet_wasm::runner::execute_http_request(
        http_request_bytes(
            public_url,
            None,
            vec![("x-api-key".to_string(), "EXAMPLE_API_KEY".to_string())],
        ),
        Vec::new(),
        std::collections::BTreeSet::from([
            format!("net.http:POST:{public_origin}"),
            "secret:EXAMPLE_API_KEY".to_string(),
        ]),
        verlet_wasm::WasmAttachmentConfig::default(),
        std::collections::BTreeMap::from([(
            "EXAMPLE_API_KEY".to_string(),
            "test-secret".to_string(),
        )]),
    )
    .await
    .unwrap_err();

    assert_eq!(err.status, verlet_wasm::runner::STATUS_CAPABILITY_DENIED);
    assert_eq!(err.message, "missing required secret capability");
}

#[test]
fn wasm_http_capability_allows_public_origin_wildcards() {
    let grants = std::collections::BTreeSet::from([
        "net.http:GET:https://*".to_string(),
        "net.http:GET:http://*".to_string(),
    ]);

    verlet_wasm::runner::ensure_http_capability(
        &grants,
        &verlet_wasm::WasmAttachmentConfig::default(),
        &reqwest::Method::GET,
        "https://example.com",
        false,
    )
    .unwrap();
    verlet_wasm::runner::ensure_http_capability(
        &grants,
        &verlet_wasm::WasmAttachmentConfig::default(),
        &reqwest::Method::GET,
        "http://news.example:8080",
        false,
    )
    .unwrap();
    let err = verlet_wasm::runner::ensure_http_capability(
        &grants,
        &verlet_wasm::WasmAttachmentConfig::default(),
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
        &verlet_wasm::WasmAttachmentConfig::default(),
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
        &verlet_wasm::WasmAttachmentConfig::default(),
        &reqwest::Method::GET,
        "https://api.example.com",
        false,
    )
    .unwrap();
    verlet_wasm::runner::ensure_http_capability(
        &grants,
        &verlet_wasm::WasmAttachmentConfig::default(),
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
        http_attachment_config(&origin, "POST", []),
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
        http_attachment_config(&origin, "POST", ["EXAMPLE_API_KEY"]),
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
