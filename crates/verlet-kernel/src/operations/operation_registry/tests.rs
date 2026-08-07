fn operation_guest(prefix: &str, required_capabilities: Vec<&str>) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "search",
            "input": "bytes",
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": required_capabilities
        }]
    })
    .to_string();
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

fn multi_operation_guest(prefix: &str, operation_names: &[&str]) -> String {
    let operations = operation_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            serde_json::json!({
                "id": index + 1,
                "name": name,
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
        "operations": operations
    })
    .to_string();
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

fn wat_guest(wat: impl AsRef<str>) -> Vec<u8> {
    wat::parse_str(wat.as_ref()).expect("test WAT fixture should compile to wasm")
}

#[tokio::test]
async fn registry_publishes_describes_and_invokes_operation() {
    let registry = verlet_operations::operation_registry::OperationRegistry::new();
    let registration = verlet_operations::operation_registry::OperationRegistration::new(
        "search",
        verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(operation_guest("search:", Vec::new()))),
    )
    .with_metadata("owner", "test");

    let record = registry.register(registration).await.unwrap();
    assert_eq!(record.name, "search");
    assert_eq!(record.manifest.operations[0].name, "search");
    assert_eq!(record.metadata["owner"], "test");

    let described = registry.describe("search").await.unwrap();
    assert_eq!(described, record);

    let output = registry
        .invoke_bytes("search", "search", b"verlet".to_vec())
        .await
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&output.output), "search:verlet");
}

#[tokio::test]
async fn registry_filters_selected_operations_and_rejects_excluded_invokes() {
    let registry = verlet_operations::operation_registry::OperationRegistry::new();
    let registration = verlet_operations::operation_registry::OperationRegistration::new(
        "analytics",
        verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(multi_operation_guest(
            "profile:",
            &["profile", "summarize"],
        ))),
    )
    .with_operation_names(["profile"]);

    let record = registry.register(registration).await.unwrap();
    assert_eq!(record.manifest.operations.len(), 1);
    assert_eq!(record.manifest.operations[0].name, "profile");
    assert_eq!(record.projections().operations.len(), 1);
    assert_eq!(
        registry
            .describe("analytics")
            .await
            .unwrap()
            .manifest
            .operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<Vec<_>>(),
        vec!["profile"]
    );

    let output = registry
        .invoke_bytes("analytics", "profile", b"row-count".to_vec())
        .await
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&output.output), "profile:row-count");

    let bytes_err = registry
        .invoke_bytes("analytics", "summarize", b"hidden".to_vec())
        .await
        .unwrap_err();
    assert!(bytes_err.to_string().contains("does not expose operation"));

    let process_err = match registry
        .invoke_process("analytics", "summarize", b"hidden".to_vec())
        .await
    {
        Ok(_) => panic!("excluded operation should not be invokable as a process"),
        Err(err) => err,
    };
    assert!(
        process_err
            .to_string()
            .contains("does not expose operation")
    );
}

#[tokio::test]
async fn registry_invokes_operation_as_process_handle() {
    let registry = verlet_operations::operation_registry::OperationRegistry::new();
    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                "search",
                verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(operation_guest(
                    "search:",
                    Vec::new(),
                ))),
            ),
        )
        .await
        .unwrap();

    let process = registry
        .invoke_process("search", "search", b"process".to_vec())
        .await
        .unwrap();
    let output = process.output();

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::WasmOperation
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout), "search:process");
    assert_eq!(output.stderr, Vec::<u8>::new());
    assert_eq!(output.exit_code(), Some(0));
    assert!(output.success());
    assert_eq!(process.events().len(), 3);
}

#[tokio::test]
async fn registry_rejects_publish_when_manifest_grants_are_missing() {
    let registry = verlet_operations::operation_registry::OperationRegistry::new();
    let registration = verlet_operations::operation_registry::OperationRegistration::new(
        "search",
        verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(operation_guest(
            "search:",
            vec!["net.http:POST:https://api.example.invalid"],
        ))),
    );

    let err = registry.register(registration).await.unwrap_err();
    assert!(err.to_string().contains("requires ungranted capabilities"));
    assert!(registry.describe("search").await.is_none());
}

#[tokio::test]
async fn registry_replaces_atomically_after_validation() {
    let registry = verlet_operations::operation_registry::OperationRegistry::new();
    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                "search",
                verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(operation_guest(
                    "v1:",
                    Vec::new(),
                ))),
            ),
        )
        .await
        .unwrap();

    let invalid_replacement = verlet_operations::operation_registry::OperationRegistration::new(
        "search",
        verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(operation_guest(
            "v2:",
            vec!["secret:EXAMPLE_API_KEY"],
        ))),
    );
    let err = registry.register(invalid_replacement).await.unwrap_err();
    assert!(err.to_string().contains("secret:EXAMPLE_API_KEY"));

    let output = registry
        .invoke_bytes("search", "search", b"still-v1".to_vec())
        .await
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&output.output), "v1:still-v1");
}

#[tokio::test]
async fn registry_replaces_after_valid_registration() {
    let registry = verlet_operations::operation_registry::OperationRegistry::new();
    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                "search",
                verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(operation_guest(
                    "v1:",
                    Vec::new(),
                ))),
            ),
        )
        .await
        .unwrap();
    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                "search",
                verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(operation_guest(
                    "v2:",
                    Vec::new(),
                ))),
            ),
        )
        .await
        .unwrap();

    let output = registry
        .invoke_bytes("search", "search", b"now".to_vec())
        .await
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&output.output), "v2:now");
}

#[tokio::test]
async fn registry_derives_cli_http_llm_and_mcp_projections() {
    let registry = verlet_operations::operation_registry::OperationRegistry::new();
    let record = registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                "Example Search",
                verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(operation_guest(
                    "search:",
                    Vec::new(),
                ))),
            ),
        )
        .await
        .unwrap();

    let projections = record.projections();
    assert_eq!(projections.registered_name, "Example Search");
    assert_eq!(projections.operations.len(), 1);
    let projection = &projections.operations[0];
    assert_eq!(
        projection.cli.command,
        "verlet tool run Example Search search"
    );
    assert_eq!(
        projection.process.command,
        "verlet run Example Search search"
    );
    assert_eq!(
        projection.process.stderr,
        verlet_abi::WasmOperationEventKind::None
    );
    assert_eq!(projection.http.method, "POST");
    assert_eq!(projection.http.path, "/operations/Example Search/search");
    assert_eq!(projection.llm_tool.name, "example_search_search");
    assert_eq!(projection.mcp.tool_name, "example_search_search");
    assert_eq!(
        verlet_operations::projection_tool_name("http-fetch", "http_fetch"),
        "http_fetch"
    );
    assert_eq!(
        verlet_operations::projection_tool_name("document", "extract_text"),
        "document_extract_text"
    );
    assert_eq!(projection.input, verlet_abi::WasmOperationValueKind::Bytes);
    assert_eq!(projection.output, verlet_abi::WasmOperationValueKind::Bytes);
    assert_eq!(projection.abi.registered_name, "Example Search");
    assert_eq!(projection.abi.operation_name, "search");
    assert_eq!(projection.abi.source_ports[0].name, "input");
    assert_eq!(projection.abi.sink_ports[0].name, "output");
    assert!(!projection.abi.has_hidden_durable_sink());
}
