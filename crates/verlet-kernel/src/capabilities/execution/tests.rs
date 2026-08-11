use crate::agent::agent_tool_router::AgentKernelToolProvider as _;
use object_store::ObjectStoreExt as _;

async fn expect_output(
    events: &mut tokio::sync::broadcast::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
) -> String {
    loop {
        match events.recv().await.unwrap() {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
                return text;
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                panic!("thread failed: {message}")
            }
            _ => {}
        }
    }
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

fn process_test_provider() -> (
    crate::capabilities::execution::BashToolProvider,
    crate::kernel::runtime_host::turn::TurnContextSnapshot,
) {
    let store: std::sync::Arc<dyn verlet_history::RuntimeStore> =
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "process-tool-session");
    let dispatcher = crate::kernel::process_handle_dispatch::test_process_dispatcher(
        std::sync::Arc::clone(&store),
        coordinates.clone(),
    );
    let context = crate::kernel::runtime_host::turn::TurnContext::new(
        verlet_runtime_contracts::ThreadContext::root(coordinates),
        "process-tool-turn",
        &crate::kernel::runtime_host::turn::TurnInput::text("process tool test"),
        tokio_util::sync::CancellationToken::new(),
    )
    .snapshot();
    (
        crate::capabilities::execution::BashToolProvider::new(
            crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
        )
        .with_process_dispatcher(dispatcher),
        context,
    )
}

fn tool_result_json(message: verlet_history::CanonicalMessage) -> (serde_json::Value, bool) {
    let verlet_history::CanonicalMessage::ToolResult {
        content, is_error, ..
    } = message
    else {
        panic!("expected tool result");
    };
    let text = content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("");
    (serde_json::from_str(&text).unwrap(), is_error)
}

fn tool_result_text(message: verlet_history::CanonicalMessage) -> (String, bool) {
    let verlet_history::CanonicalMessage::ToolResult {
        content, is_error, ..
    } = message
    else {
        panic!("expected tool result");
    };
    let text = content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("");
    (text, is_error)
}

async fn invoke_bash(
    provider: &crate::capabilities::execution::BashToolProvider,
    call_id: &str,
    command: &str,
) -> verlet_history::CanonicalMessage {
    provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: call_id.to_string(),
            tool_name: verlet_vbash::BASH_TOOL.to_string(),
            arguments: serde_json::json!({ "command": command }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap()
}

fn echo_operation_guest() -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "echo",
            "input": "bytes",
            "output": "bytes",
            "events": "jsonl",
            "mode": "sync",
            "required_capabilities": []
        }]
    })
    .to_string();
    let event = br#"{"type":"verlet_run","operation":"echo"}
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
    )
}

async fn echo_operation_registry()
-> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                "echoer",
                verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(echo_operation_guest())),
            ),
        )
        .await
        .unwrap();
    registry
}

fn named_echo_operation_guest(operation_name: &str) -> String {
    named_echo_operation_guest_with_required(operation_name, Vec::new())
}

fn named_echo_operation_guest_with_required(
    operation_name: &str,
    required_capabilities: Vec<&str>,
) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": operation_name,
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
              (data (i32.const 8192) "op:")
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
    )
}

async fn named_echo_operation_registry(
    registered_name: &str,
    operation_name: &str,
) -> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                registered_name,
                verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(named_echo_operation_guest(
                    operation_name,
                ))),
            ),
        )
        .await
        .unwrap();
    registry
}

async fn named_echo_operation_registry_with_required(
    registered_name: &str,
    operation_name: &str,
    required_capabilities: Vec<&str>,
) -> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let mut registration = verlet_operations::operation_registry::OperationRegistration::new(
        registered_name,
        verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(
            named_echo_operation_guest_with_required(operation_name, required_capabilities.clone()),
        )),
    );
    registration =
        registration.with_capability_grants(required_capabilities.into_iter().map(String::from));
    registry.register(registration).await.unwrap();
    registry
}

struct FixedKernelDispatcher(&'static str);

#[async_trait::async_trait]
impl verlet_operations::operation_registry::KernelOperationDispatcher for FixedKernelDispatcher {
    async fn invoke_kernel_operation(
        &self,
        _operation_name: &str,
        _input: Vec<u8>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        Ok(self.0.as_bytes().to_vec())
    }
}

struct BarrierKernelDispatcher {
    output: &'static str,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
}

#[async_trait::async_trait]
impl verlet_operations::operation_registry::KernelOperationDispatcher for BarrierKernelDispatcher {
    async fn invoke_kernel_operation(
        &self,
        _operation_name: &str,
        _input: Vec<u8>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        self.barrier.wait().await;
        Ok(self.output.as_bytes().to_vec())
    }
}

#[tokio::test]
async fn harness_runs_virtual_file_commands_pipes_and_patch() {
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
    )
    .await
    .unwrap();

    let output = harness
        .execute(
            "pwd && mkdir -p dir && touch dir/touched.txt \
                 && echo alpha > dir/a.txt && echo beta >> dir/a.txt \
                 && cp /skills/README.md dir/skill.txt \
                 && head -n 1 dir/a.txt && tail -n 1 dir/a.txt && wc -l < dir/a.txt \
                 && stat dir/a.txt >/dev/null \
                 && cp dir/a.txt dir/b.txt && mv dir/b.txt dir/c.txt \
                 && cat dir/c.txt | grep beta && rm dir/c.txt",
        )
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("/workspace"));
    assert!(output.stdout.contains("alpha"));
    assert!(output.stdout.contains("beta"));
    assert!(output.stdout.contains("2"));
    let copied_skill = harness.read_file("/workspace/dir/skill.txt").await.unwrap();
    assert!(String::from_utf8(copied_skill).unwrap().contains("Verlet"));

    let output = harness.execute("cd /tmp && pwd").await.unwrap();
    assert_eq!(output.stdout.trim(), "/tmp");
    let output = harness.execute("pwd").await.unwrap();
    assert_eq!(output.stdout.trim(), "/tmp");
    let output = harness.execute("cd /workspace").await.unwrap();
    assert!(output.success(), "{output:?}");

    let patch = r#"apply_patch <<'PATCH'
*** Begin Patch
*** Update File: dir/a.txt
@@
-alpha
+gamma
*** End Patch
PATCH"#;
    let output = harness.execute(patch).await.unwrap();
    assert!(output.success(), "{output:?}");
    let content = harness.read_file("/workspace/dir/a.txt").await.unwrap();
    assert_eq!(String::from_utf8(content).unwrap(), "gamma\nbeta\n");

    let patch = r#"apply_patch <<'PATCH'
*** Begin Patch
*** Add File: dir/new.txt
+fresh
*** Update File: dir/new.txt
*** Move to: dir/moved.txt
@@
-fresh
+moved
*** Delete File: dir/touched.txt
*** Update File: dir/a.txt
@@
 beta
+omega
*** End of File
*** End Patch
PATCH"#;
    let output = harness.execute(patch).await.unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("A dir/new.txt"));
    assert!(output.stdout.contains("M dir/moved.txt"));
    assert!(output.stdout.contains("D dir/touched.txt"));
    let moved = harness.read_file("/workspace/dir/moved.txt").await.unwrap();
    assert_eq!(String::from_utf8(moved).unwrap(), "moved\n");
    let content = harness.read_file("/workspace/dir/a.txt").await.unwrap();
    assert_eq!(String::from_utf8(content).unwrap(), "gamma\nbeta\nomega\n");
    assert!(
        harness
            .read_file("/workspace/dir/touched.txt")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn virtual_bash_verlet_run_invokes_registered_operation_from_pipe() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(echo_operation_registry().await);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute("echo hello | verlet run echoer echo")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "op:hello\n");
    assert!(output.stderr.contains(r#""operation":"echo""#));
}

#[tokio::test]
async fn virtual_bash_kernel_operation_uses_dispatch_overlay() {
    let registry = kernel_identity_operation_registry().await;
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_kernel_dispatch_overlay(
            verlet_operations::operation_registry::KernelDispatchOverlay::new().with_dispatcher(
                "thread-identity",
                std::sync::Arc::new(FixedKernelDispatcher("bash-thread")),
            ),
        );
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute("printf ignored | verlet run thread-identity identify-thread")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "bash-thread");
}

#[tokio::test]
async fn virtual_bash_dispatch_overlay_builder_is_order_independent() {
    let registry = kernel_identity_operation_registry().await;
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_kernel_dispatch_overlay(
            verlet_operations::operation_registry::KernelDispatchOverlay::new().with_dispatcher(
                "thread-identity",
                std::sync::Arc::new(FixedKernelDispatcher("overlay-first")),
            ),
        )
        .with_operation_registry(registry);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute("printf ignored | verlet run thread-identity identify-thread")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "overlay-first");
}

#[tokio::test]
async fn virtual_bash_shared_registry_isolates_concurrent_kernel_dispatch_overlays() {
    let registry = kernel_identity_operation_registry().await;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let config_a = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(std::sync::Arc::clone(&registry))
        .with_kernel_dispatch_overlay(
            verlet_operations::operation_registry::KernelDispatchOverlay::new().with_dispatcher(
                "thread-identity",
                std::sync::Arc::new(BarrierKernelDispatcher {
                    output: "bash-thread-a",
                    barrier: std::sync::Arc::clone(&barrier),
                }),
            ),
        );
    let config_b = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_kernel_dispatch_overlay(
            verlet_operations::operation_registry::KernelDispatchOverlay::new().with_dispatcher(
                "thread-identity",
                std::sync::Arc::new(BarrierKernelDispatcher {
                    output: "bash-thread-b",
                    barrier,
                }),
            ),
        );
    let (harness_a, harness_b) = tokio::join!(
        verlet_vbash::harness::BashkitExecutionHarness::new(config_a),
        verlet_vbash::harness::BashkitExecutionHarness::new(config_b),
    );
    let mut harness_a = harness_a.unwrap();
    let mut harness_b = harness_b.unwrap();

    let (output_a, output_b) = tokio::join!(
        harness_a.execute("printf ignored | verlet run thread-identity identify-thread"),
        harness_b.execute("printf ignored | verlet run thread-identity identify-thread"),
    );
    let output_a = output_a.unwrap();
    let output_b = output_b.unwrap();

    assert!(output_a.success(), "{output_a:?}");
    assert!(output_b.success(), "{output_b:?}");
    assert_eq!(output_a.stdout, "bash-thread-a");
    assert_eq!(output_b.stdout, "bash-thread-b");
}

async fn kernel_identity_operation_registry()
-> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    registry
        .register_kernel(
            verlet_operations::operation_registry::KernelOperationRegistration::new(
                "thread-identity",
                verlet_abi::WasmOperationManifest {
                    abi: verlet_wasm::runner::OPERATION_ABI.to_string(),
                    operations: vec![verlet_abi::WasmOperationDefinition {
                        id: 1,
                        name: "identify-thread".to_string(),
                        input: verlet_abi::WasmOperationValueKind::Bytes,
                        output: verlet_abi::WasmOperationValueKind::Bytes,
                        events: verlet_abi::WasmOperationEventKind::None,
                        mode: verlet_abi::WasmOperationMode::Sync,
                        required_capabilities: Vec::new(),
                    }],
                },
            ),
        )
        .await
        .unwrap();
    registry
}

#[tokio::test]
async fn virtual_bash_projects_registry_operations_as_host_builtins() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(named_echo_operation_registry("search", "search").await);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute("command -v search && command -V search && printf verlet | search")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert!(output.stdout.starts_with("search\n"), "{output:?}");
    assert!(output.stdout.contains("search is a shell builtin"));
    assert!(output.stdout.contains("op:verlet"));
}

#[tokio::test]
async fn virtual_bash_man_describes_projected_operation_command() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(named_echo_operation_registry("search", "search").await);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness.execute("man search").await.unwrap();

    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("NAME"));
    assert!(output.stdout.contains("search - search from search"));
    assert!(output.stdout.contains("verlet run search search"));
    assert!(output.stdout.contains("STDIN"));
    assert!(output.stdout.contains("STDOUT"));
    assert!(output.stdout.contains("EXIT STATUS"));
}

#[tokio::test]
async fn virtual_bash_host_builtins_reflect_registry_add_and_remove_without_rebuild() {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry.clone());
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let before = harness.execute("command -v search").await.unwrap();
    assert_ne!(before.exit_code, 0, "{before:?}");

    registry
        .register(
            verlet_operations::operation_registry::OperationRegistration::new(
                "search",
                verlet_wasm::WasmRuntimeArtifact::bytes(wat_guest(named_echo_operation_guest(
                    "search",
                ))),
            ),
        )
        .await
        .unwrap();
    let after_register = harness
        .execute("command -v search && printf verlet | search")
        .await
        .unwrap();
    assert!(after_register.success(), "{after_register:?}");
    assert!(after_register.stdout.contains("search\n"));
    assert!(after_register.stdout.contains("op:verlet"));

    registry.unregister("search").await.unwrap();
    let after_remove = harness.execute("search verlet").await.unwrap();
    assert_ne!(after_remove.exit_code, 0, "{after_remove:?}");
    assert!(
        after_remove.stderr.contains("not found") || after_remove.stderr.contains("command"),
        "{after_remove:?}"
    );
}

#[tokio::test]
async fn virtual_bash_reserved_operation_names_are_not_projected_as_shell_commands() {
    let registry = named_echo_operation_registry("capsule", "type").await;
    let registry_adapter = crate::capabilities::execution::KernelVbashOperationRegistry::new(
        verlet_operations::operation_registry::ScopedOperationRegistry::new(
            std::sync::Arc::clone(&registry),
            verlet_operations::operation_registry::KernelDispatchOverlay::new(),
        ),
    );
    let shell_commands = verlet_vbash::harness::operation_shell_command_names(
        &registry_adapter,
        &verlet_vbash::reserved_operation_shell_commands(),
    )
    .await;
    assert!(!shell_commands.contains("type"));

    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness.execute("printf verlet | type").await.unwrap();

    assert!(!output.stdout.contains("op:verlet"), "{output:?}");
    assert!(!output.stderr.contains("op:verlet"), "{output:?}");
}

#[tokio::test]
async fn virtual_bash_operation_shell_commands_enforce_capability_grants() {
    let registry = named_echo_operation_registry_with_required(
        "secret-search",
        "search",
        vec!["secret:EXAMPLE_API_KEY"],
    )
    .await;
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry.clone());
    let mut denied = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = denied.execute("printf verlet | search").await.unwrap();
    assert_eq!(output.exit_code, 126, "{output:?}");
    assert!(
        output
            .stderr
            .contains("missing capability grants: secret:EXAMPLE_API_KEY"),
        "{output:?}"
    );

    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_capability_grant("secret:EXAMPLE_API_KEY");
    let mut granted = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();
    let output = granted.execute("printf verlet | search").await.unwrap();

    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("op:verlet"));
}

#[tokio::test]
async fn virtual_bash_verlet_run_works_with_vfs_redirection() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(echo_operation_registry().await)
        .with_writable_mount("/work");
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();
    harness
        .execute("printf '{\"query\":\"verlet\"}' > /work/input.json")
        .await
        .unwrap();

    let output = harness
        .execute(
            "verlet run echoer echo < /work/input.json > /work/output.json 2> /work/events.jsonl",
        )
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(harness.read_file("/work/output.json").await.unwrap()).unwrap(),
        "op:{\"query\":\"verlet\"}"
    );
    assert!(
        String::from_utf8(harness.read_file("/work/events.jsonl").await.unwrap())
            .unwrap()
            .contains(r#""type":"verlet_run""#)
    );
}

#[tokio::test]
async fn virtual_bash_execute_process_runs_verlet_operation_with_stdin() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(echo_operation_registry().await);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let process = harness
        .execute_process("echo hello | verlet run echoer echo")
        .await
        .unwrap();
    let output = process.output();

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::VirtualBash
    );
    assert_eq!(output.stdout_text_lossy(), "op:hello\n");
    assert!(output.stderr_text_lossy().contains(r#""operation":"echo""#));
    assert_eq!(output.exit_code(), Some(0));
    assert!(output.success());
}

#[tokio::test]
async fn harness_execute_process_replays_virtual_output_and_exit() {
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
    )
    .await
    .unwrap();

    let process = harness
        .execute_process("echo hi && ls /missing")
        .await
        .unwrap();
    let output = process.output();
    let replay = verlet_process::execution::VirtualCommandOutput::from(&output);

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::VirtualBash
    );
    assert!(output.stdout_text_lossy().contains("hi"));
    assert!(output.stderr_text_lossy().contains("missing"));
    assert_ne!(output.exit_code(), Some(0));
    assert_eq!(replay.stdout, output.stdout_text_lossy());
    assert_eq!(replay.stderr, output.stderr_text_lossy());
    assert_eq!(replay.exit_code, output.exit_code().unwrap());
}

#[tokio::test]
async fn harness_rejects_readonly_mount_and_native_command() {
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
    )
    .await
    .unwrap();

    let output = harness
        .execute("cat /skills/README.md && echo nope > /skills/README.md")
        .await
        .unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(output.stdout.contains("Verlet virtual bash"));
    assert!(output.stderr.contains("read-only") || output.stderr.contains("denied"));

    let output = harness.execute("cargo test").await.unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(
        output.stderr.contains("not found")
            || output.stderr.contains("command")
            || output.stderr.contains("cargo")
    );

    let output = harness.execute("ls /missing").await.unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(output.stderr.contains("not found") || output.stderr.contains("missing"));
}

#[derive(Default)]
struct RecordingExternalExecutor {
    requests: tokio::sync::Mutex<Vec<verlet_process::execution::ExternalCommandRequest>>,
}

#[async_trait::async_trait]
impl verlet_process::execution::ExternalCommandExecutor for RecordingExternalExecutor {
    async fn exec(
        &self,
        request: verlet_process::execution::ExternalCommandRequest,
    ) -> verlet_process::VerletProcessResult<verlet_process::execution::ExternalCommandResult> {
        self.requests.lock().await.push(request.clone());
        match &request.invocation {
            verlet_process::execution::ExternalCommandInvocation::Argv { command, args } => {
                Ok(verlet_process::execution::ExternalCommandResult::new(
                    verlet_process::execution::VirtualCommandOutput {
                        stdout: format!(
                            "{command} args={} stdin={}",
                            args.join(" "),
                            request.stdin.unwrap_or_default()
                        ),
                        stderr: String::new(),
                        exit_code: 0,
                        stdout_truncated: false,
                        stderr_truncated: false,
                    },
                ))
            }
            verlet_process::execution::ExternalCommandInvocation::Script(_) => {
                let prefix: &str = request.executor.as_ref();
                Ok(verlet_process::execution::ExternalCommandResult::new(
                    verlet_process::execution::VirtualCommandOutput {
                        stdout: format!("{prefix} stdout\n"),
                        stderr: format!("{prefix} stderr\n"),
                        exit_code: 7,
                        stdout_truncated: false,
                        stderr_truncated: false,
                    },
                )
                .with_file_write(
                    verlet_process::execution::ExternalFileWrite::new(
                        "/workspace/generated.txt",
                        format!("from {prefix}\n"),
                    ),
                ))
            }
        }
    }
}

#[tokio::test]
async fn host_always_bypasses_bashkit_and_runs_in_host_cwd() {
    let host_root =
        std::env::temp_dir().join(format!("verlet-host-bash-test-{}", uuid::Uuid::now_v7()));
    tokio::fs::create_dir_all(&host_root).await.unwrap();
    let canonical_host_root = tokio::fs::canonicalize(&host_root).await.unwrap();
    tokio::fs::write(host_root.join("host.txt"), "host file\n")
        .await
        .unwrap();
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_execution_policy(verlet_vbash::BashExecutionPolicy::host_always())
        .with_host_bash_executor(&host_root);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let process = harness
        .execute_process("pwd; cat host.txt; echo host err >&2; exit 3")
        .await
        .unwrap();
    let output = process.output();

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::HostBash
    );
    assert!(
        output
            .stdout_text_lossy()
            .contains(&canonical_host_root.display().to_string())
    );
    assert!(output.stdout_text_lossy().contains("host file"));
    assert!(output.stderr_text_lossy().contains("host err"));
    assert_eq!(output.exit_code(), Some(3));
    tokio::fs::remove_dir_all(host_root).await.unwrap();
}

#[tokio::test]
async fn host_always_can_list_real_repo_bin_dir() {
    let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_execution_policy(verlet_vbash::BashExecutionPolicy::host_always())
        .with_host_bash_executor(&repo_root);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let process = harness.execute_process("ls src/bin").await.unwrap();
    let output = process.output();

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::HostBash
    );
    assert!(output.success(), "{output:?}");
    assert!(output.stdout_text_lossy().contains("verlet.rs"));
    assert!(!output.stdout_text_lossy().contains("verlet-vbash-smoke.rs"));
}

#[tokio::test]
async fn selective_proxy_routes_named_command_through_executor() {
    let executor = std::sync::Arc::new(RecordingExternalExecutor::default());
    let policy = verlet_vbash::BashExecutionPolicy::selective([(
        "cargo",
        verlet_vbash::CommandRoute::RemoteLinux,
    )]);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default()
            .with_execution_policy(policy)
            .with_external_executor(executor.clone()),
    )
    .await
    .unwrap();

    let output = harness
        .execute("echo hi | cargo test > /workspace/out.txt")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(harness.read_file("/workspace/out.txt").await.unwrap()).unwrap(),
        "cargo args=test stdin=hi\n"
    );
    let requests = executor.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].executor,
        verlet_process::execution::ExternalExecutorKind::RemoteLinux
    );
    assert_eq!(requests[0].cwd, std::path::PathBuf::from("/workspace"));
    assert_eq!(requests[0].stdin.as_deref(), Some("hi\n"));
    assert_eq!(
        requests[0].max_output_bytes,
        verlet_vbash::SPILL_RETENTION_MAX_BYTES
    );
    assert_eq!(
        requests[0].invocation,
        verlet_process::execution::ExternalCommandInvocation::Argv {
            command: "cargo".to_string(),
            args: vec!["test".to_string()]
        }
    );
}

#[tokio::test]
async fn selective_proxy_sub_cap_pipeline_matches_the_legacy_result() {
    let executor = std::sync::Arc::new(RecordingExternalExecutor::default());
    let policy = verlet_vbash::BashExecutionPolicy::selective([(
        "cargo",
        verlet_vbash::CommandRoute::RemoteLinux,
    )]);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default()
            .with_execution_policy(policy)
            .with_external_executor(executor),
    )
    .await
    .unwrap();

    let output = harness
        .execute("printf input | cargo test | sed 's/cargo/CARGO/'")
        .await
        .unwrap();

    assert_eq!(output.stdout, "CARGO args=test stdin=input\n");
    assert!(output.stderr.is_empty());
    assert_eq!(output.exit_code, 0);
    assert!(!output.stdout_truncated);
    assert!(!output.stderr_truncated);
}

#[tokio::test]
async fn selective_proxy_deny_does_not_invoke_executor() {
    let executor = std::sync::Arc::new(RecordingExternalExecutor::default());
    let policy =
        verlet_vbash::BashExecutionPolicy::selective([("cargo", verlet_vbash::CommandRoute::Deny)]);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default()
            .with_execution_policy(policy)
            .with_external_executor(executor.clone()),
    )
    .await
    .unwrap();

    let output = harness.execute("cargo test").await.unwrap();

    assert_eq!(output.exit_code, 126);
    assert!(output.stderr.contains("denied by routing policy"));
    assert!(executor.requests.lock().await.is_empty());
}

#[tokio::test]
async fn harness_execute_process_records_host_external_result_and_file_deltas() {
    let executor = std::sync::Arc::new(RecordingExternalExecutor::default());
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default()
            .with_execution_policy(verlet_vbash::BashExecutionPolicy::host_always())
            .with_external_executor(executor),
    )
    .await
    .unwrap();

    let process = harness.execute_process("cargo test --lib").await.unwrap();
    let output = process.output();

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::HostBash
    );
    assert_eq!(output.stdout_text_lossy(), "host stdout\n");
    assert_eq!(output.stderr_text_lossy(), "host stderr\n");
    assert_eq!(output.exit_code(), Some(7));
    assert_eq!(output.file_deltas.len(), 1);
    assert_eq!(
        output.file_deltas[0].path,
        std::path::PathBuf::from("/workspace/generated.txt")
    );
    assert_eq!(
        String::from_utf8(harness.read_file("/workspace/generated.txt").await.unwrap()).unwrap(),
        "from host\n"
    );
}

#[tokio::test]
async fn harness_execute_process_records_remote_linux_backend() {
    let executor = std::sync::Arc::new(RecordingExternalExecutor::default());
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default()
            .with_execution_policy(verlet_vbash::BashExecutionPolicy::remote_always())
            .with_external_executor(executor.clone()),
    )
    .await
    .unwrap();

    let process = harness.execute_process("uname -a").await.unwrap();
    let output = process.output();

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::RemoteLinux
    );
    assert_eq!(output.stdout_text_lossy(), "remote stdout\n");
    let requests = executor.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].executor,
        verlet_process::execution::ExternalExecutorKind::RemoteLinux
    );
    assert_eq!(
        requests[0].max_output_bytes,
        verlet_vbash::SPILL_RETENTION_MAX_BYTES
    );
    assert_eq!(
        requests[0].invocation,
        verlet_process::execution::ExternalCommandInvocation::Script("uname -a".to_string())
    );
}

struct SlowDeadlineExecutor;

#[async_trait::async_trait]
impl verlet_process::execution::ExternalCommandExecutor for SlowDeadlineExecutor {
    async fn exec(
        &self,
        request: verlet_process::execution::ExternalCommandRequest,
    ) -> verlet_process::VerletProcessResult<verlet_process::execution::ExternalCommandResult> {
        let slow = tokio::time::sleep(std::time::Duration::from_secs(60));
        match tokio::time::timeout(request.deadline.remaining(), slow).await {
            Ok(_) => unreachable!("slow executor should outlive the deadline"),
            Err(_) => Ok(verlet_process::execution::ExternalCommandResult::new(
                verlet_process::execution::VirtualCommandOutput {
                    stdout: String::new(),
                    stderr: "host bash exec timed out\n".to_string(),
                    exit_code: 124,
                    stdout_truncated: false,
                    stderr_truncated: false,
                },
            )),
        }
    }
}

struct SerialNonObservingExternalExecutor {
    started: std::sync::atomic::AtomicUsize,
    started_notify: tokio::sync::Notify,
    first_released: std::sync::atomic::AtomicBool,
    first_release: tokio::sync::Notify,
}

impl SerialNonObservingExternalExecutor {
    async fn wait_for_started(&self, count: usize) {
        while self.started.load(std::sync::atomic::Ordering::SeqCst) < count {
            self.started_notify.notified().await;
        }
    }

    fn release_first(&self) {
        self.first_released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.first_release.notify_waiters();
    }
}

#[async_trait::async_trait]
impl verlet_process::execution::ExternalCommandExecutor for SerialNonObservingExternalExecutor {
    async fn exec(
        &self,
        request: verlet_process::execution::ExternalCommandRequest,
    ) -> verlet_process::VerletProcessResult<verlet_process::execution::ExternalCommandResult> {
        let order = self
            .started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.started_notify.notify_waiters();
        if order == 0 {
            while !self
                .first_released
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                self.first_release.notified().await;
            }
        }
        let label = request.label();
        Ok(verlet_process::execution::ExternalCommandResult::new(
            verlet_process::execution::VirtualCommandOutput {
                stdout: format!("{label}\n").repeat(4_000),
                stderr: String::new(),
                exit_code: 0,
                stdout_truncated: false,
                stderr_truncated: false,
            },
        ))
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn abandoned_bash_invocation_keeps_the_harness_mutex_and_serializes_the_next_call() {
    let executor = std::sync::Arc::new(SerialNonObservingExternalExecutor {
        started: std::sync::atomic::AtomicUsize::new(0),
        started_notify: tokio::sync::Notify::new(),
        first_released: std::sync::atomic::AtomicBool::new(false),
        first_release: tokio::sync::Notify::new(),
    });
    let provider = std::sync::Arc::new(crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig {
            max_output_bytes: 64,
            ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        }
        .with_execution_policy(verlet_vbash::BashExecutionPolicy::host_always())
        .with_external_executor(executor.clone()),
    ));
    let cancellation = tokio_util::sync::CancellationToken::new();
    let first = tokio::spawn({
        let provider = std::sync::Arc::clone(&provider);
        let cancellation = cancellation.clone();
        async move {
            provider
                .invoke_tool_call_cancellable(
                    crate::agent::agent_tool_router::AgentKernelToolCall {
                        call_id: "call-abandoned".to_string(),
                        tool_name: verlet_vbash::BASH_TOOL.to_string(),
                        arguments: serde_json::json!({"command": "first"}),
                        turn_context: None,
                    },
                    crate::agent::agent_tool_router::ToolInvocationCancellation::new(
                        cancellation,
                        std::time::Duration::from_millis(10),
                    ),
                )
                .await
        }
    });
    executor.wait_for_started(1).await;
    cancellation.cancel();
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    tokio::task::yield_now().await;

    let mut second = std::pin::pin!(provider.invoke_tool_call(
        crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call-abandoned".to_string(),
            tool_name: verlet_vbash::BASH_TOOL.to_string(),
            arguments: serde_json::json!({"command": "second"}),
            turn_context: None,
        }
    ));
    assert!(matches!(
        futures_util::poll!(&mut second),
        std::task::Poll::Pending
    ));
    assert_eq!(
        executor.started.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the next call must serialize behind the abandoned call's harness mutex"
    );

    executor.release_first();
    first.await.unwrap().unwrap();
    second.await.unwrap();
    assert_eq!(
        executor.started.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    let harness = provider.harness.lock().await;
    let stored = harness
        .as_ref()
        .unwrap()
        .read_file("/spill/call-abandoned.stdout.txt")
        .await
        .unwrap();
    assert_eq!(stored, b"first\n".repeat(4_000));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn harness_execute_process_marks_external_timeout_from_shared_deadline() {
    let mut config = crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_execution_policy(verlet_vbash::BashExecutionPolicy::host_always())
        .with_external_executor(std::sync::Arc::new(SlowDeadlineExecutor));
    config.execution_timeout = std::time::Duration::from_millis(10);
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let process = harness.execute_process("sleep 60").await.unwrap();
    let output = process.output();

    assert_eq!(
        process.backend(),
        &verlet_process::process::VerletProcessBackend::HostBash
    );
    assert_eq!(output.exit_code(), Some(124));
    assert!(matches!(
        output.terminal,
        Some(verlet_process::process::VerletProcessTerminalState::TimedOut { .. })
    ));
}

#[tokio::test]
async fn async_manager_wraps_bashkit_backend_without_stdin_sink() {
    let manager = verlet_process::live::AsyncExecutionManager::default();
    let backend: std::sync::Arc<dyn verlet_process::live::LiveProcessBackend> =
        std::sync::Arc::new(verlet_vbash::harness::BashkitLiveBackend::new(
            crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
        ));
    let request = verlet_process::live::AsyncProcessStartRequest::virtual_bash_script(
        "sleep 0.05; echo done",
    )
    .with_deadline(verlet_process::execution::ExecutionDeadline::from_now(
        std::time::Duration::from_secs(1),
    ))
    .with_yield_time(std::time::Duration::from_millis(5))
    .with_output_cap_bytes(1024);

    let started = manager.start(backend, request).await.unwrap();
    assert_eq!(
        started.snapshot.status,
        verlet_process::live::ProcessSnapshotStatus::Running
    );
    let process_id = started.snapshot.process_id.unwrap();

    let write = manager
        .write(
            process_id,
            b"hello\n".to_vec(),
            std::time::Duration::from_millis(10),
            1024,
        )
        .await
        .unwrap_err();
    assert!(write.to_string().contains("stdin"));

    let completed = manager
        .poll(process_id, std::time::Duration::from_secs(1), 1024)
        .await
        .unwrap();
    assert_eq!(
        completed.snapshot.status,
        verlet_process::live::ProcessSnapshotStatus::Completed
    );
    assert!(String::from_utf8_lossy(&completed.snapshot.stdout).contains("done"));
}

#[tokio::test]
async fn bash_tool_provider_exposes_process_handle_tools() {
    let provider = crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
    );

    let names = provider
        .tool_definitions()
        .await
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(names.contains(&verlet_vbash::BASH_TOOL.to_string()));
    assert!(names.contains(&crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string()));
    assert!(names.contains(&crate::capabilities::execution::WRITE_STDIN_TOOL.to_string()));
}

#[tokio::test]
async fn bash_tool_inline_result_keeps_the_legacy_wire_bytes() {
    let provider = crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
    );

    let (text, is_error) =
        tool_result_text(invoke_bash(&provider, "call_inline", "echo exact").await);

    assert!(!is_error);
    assert_eq!(
        text,
        r#"{"exit_code":0,"stderr":"","stderr_truncated":false,"stdout":"exact\n","stdout_truncated":false}"#
    );
}

#[tokio::test]
async fn inline_stream_keeps_the_legacy_lossy_utf8_conversion() {
    let harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
    )
    .await
    .unwrap();

    let (text, receipt, spilled) = crate::capabilities::execution::present_output_stream(
        &harness,
        b"before\xffafter",
        1024,
        false,
        "/spill/lossy.stdout.txt",
    )
    .await;

    assert_eq!(text, "before\u{fffd}after");
    assert!(receipt.is_none());
    assert!(!spilled);
}

#[tokio::test]
async fn bash_tool_spills_complete_stdout_and_cat_round_trips_it() {
    let provider = crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig {
            max_output_bytes: 64,
            ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        },
    );
    let expected = b"x\n".repeat(10_000);

    let (result, is_error) =
        tool_result_json(invoke_bash(&provider, "call_round_trip", "yes x | head -c 20000").await);

    assert!(!is_error, "{result}");
    assert_eq!(result["stdout_truncated"], true);
    assert_eq!(result["stderr_truncated"], false);
    assert_eq!(
        result["spill"]["stdout"]["path"],
        "/spill/call_round_trip.stdout.txt"
    );
    assert_eq!(result["spill"]["stdout"]["total_bytes"], 20_000);
    assert_eq!(result["spill"]["stdout"]["preview_bytes"], 16_384);
    assert!(result["spill"].get("stderr").is_none());
    assert!(
        result["stdout"]
            .as_str()
            .unwrap()
            .contains("Tip: cat /spill/call_round_trip.stdout.txt")
    );

    let copied = invoke_bash(
        &provider,
        "call_copy_spill",
        "cat /spill/call_round_trip.stdout.txt > /workspace/retrieved.txt",
    )
    .await;
    let (_, copy_is_error) = tool_result_json(copied);
    assert!(!copy_is_error);
    let harness = provider.harness.lock().await;
    let retrieved = harness
        .as_ref()
        .unwrap()
        .read_file("/workspace/retrieved.txt")
        .await
        .unwrap();
    assert_eq!(retrieved, expected);
}

#[tokio::test]
async fn bash_tool_spills_stderr_independently() {
    let provider = crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig {
            max_output_bytes: 64,
            ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        },
    );

    let (result, is_error) = tool_result_json(
        invoke_bash(
            &provider,
            "call_stderr",
            "printf ok; yes e | head -c 20000 >&2",
        )
        .await,
    );

    assert!(!is_error, "{result}");
    assert_eq!(result["stdout"], "ok");
    assert!(result["spill"].get("stdout").is_none());
    assert_eq!(
        result["spill"]["stderr"]["path"],
        "/spill/call_stderr.stderr.txt"
    );
    assert_eq!(result["spill"]["stderr"]["total_bytes"], 20_000);
}

#[tokio::test]
async fn bash_tool_spill_failure_returns_emergency_stub_without_failing_call() {
    let root: std::sync::Arc<dyn verlet_vfs::VerletVfsBackend> = std::sync::Arc::new(
        verlet_vfs::ReadOnlyFileSystem::new(std::sync::Arc::new(bashkit::InMemoryFs::new())),
    );
    let workspace_vfs = std::sync::Arc::new(verlet_vfs::VerletVfs::new(root));
    let provider = crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig {
            max_output_bytes: 64,
            workspace_vfs: Some(workspace_vfs),
            ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        },
    );

    let (result, is_error) = tool_result_json(
        invoke_bash(&provider, "call_spill_failure", "yes f | head -c 20000").await,
    );

    assert!(!is_error, "{result}");
    assert_eq!(result["stdout_truncated"], true);
    assert!(result.get("spill").is_none());
    let stdout = result["stdout"].as_str().unwrap();
    assert!(stdout.contains("[CONTENT_OVERFLOW - spill path unavailable]"));
    assert!(stdout.contains("Length: 20000 bytes"));
    assert!(stdout.contains("Head bytes: 500"));
    assert!(stdout.contains("Tail bytes: 500"));
}

#[tokio::test]
async fn concurrent_bash_spills_are_isolated_by_call_id() {
    let provider = std::sync::Arc::new(crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig {
            max_output_bytes: 64,
            ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        },
    ));
    let left = std::sync::Arc::clone(&provider);
    let right = std::sync::Arc::clone(&provider);

    let (left, right) = tokio::join!(
        async move { invoke_bash(&left, "call_left", "yes l | head -c 20000").await },
        async move { invoke_bash(&right, "call_right", "yes r | head -c 20000").await },
    );
    let (left, _) = tool_result_json(left);
    let (right, _) = tool_result_json(right);

    assert_eq!(
        left["spill"]["stdout"]["path"],
        "/spill/call_left.stdout.txt"
    );
    assert_eq!(
        right["spill"]["stdout"]["path"],
        "/spill/call_right.stdout.txt"
    );
    let harness = provider.harness.lock().await;
    let harness = harness.as_ref().unwrap();
    assert_eq!(
        harness
            .read_file("/spill/call_left.stdout.txt")
            .await
            .unwrap(),
        b"l\n".repeat(10_000)
    );
    assert_eq!(
        harness
            .read_file("/spill/call_right.stdout.txt")
            .await
            .unwrap(),
        b"r\n".repeat(10_000)
    );
}

#[tokio::test]
async fn repeated_call_id_does_not_overwrite_an_earlier_spill() {
    let provider = crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig {
            max_output_bytes: 64,
            ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        },
    );
    let first = b"a\n".repeat(10_000);

    let _ = invoke_bash(&provider, "call_repeat", "yes a | head -c 20000").await;
    let (second, is_error) =
        tool_result_json(invoke_bash(&provider, "call_repeat", "yes b | head -c 20000").await);

    assert!(!is_error, "{second}");
    assert!(second.get("spill").is_none());
    assert!(
        second["stdout"]
            .as_str()
            .unwrap()
            .contains("spill path unavailable")
    );
    let harness = provider.harness.lock().await;
    assert_eq!(
        harness
            .as_ref()
            .unwrap()
            .read_file("/spill/call_repeat.stdout.txt")
            .await
            .unwrap(),
        first
    );
}

#[tokio::test]
async fn concurrent_same_call_id_spills_keep_the_first_complete_stream() {
    let provider = std::sync::Arc::new(crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig {
            max_output_bytes: 64,
            ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        },
    ));
    let left = std::sync::Arc::clone(&provider);
    let right = std::sync::Arc::clone(&provider);

    let (left, right) = tokio::join!(
        async move { invoke_bash(&left, "call_same", "yes a | head -c 20000").await },
        async move { invoke_bash(&right, "call_same", "yes b | head -c 20000").await },
    );
    let (left, _) = tool_result_json(left);
    let (right, _) = tool_result_json(right);

    assert_eq!(
        usize::from(left.get("spill").is_some()) + usize::from(right.get("spill").is_some()),
        1
    );
    assert_eq!(
        usize::from(
            left["stdout"]
                .as_str()
                .unwrap()
                .contains("spill path unavailable")
        ) + usize::from(
            right["stdout"]
                .as_str()
                .unwrap()
                .contains("spill path unavailable")
        ),
        1
    );
    let harness = provider.harness.lock().await;
    let stored = harness
        .as_ref()
        .unwrap()
        .read_file("/spill/call_same.stdout.txt")
        .await
        .unwrap();
    assert!(stored == b"a\n".repeat(10_000) || stored == b"b\n".repeat(10_000));
}

#[test]
fn spill_paths_are_single_component_collision_safe_and_bounded() {
    let hostile = crate::capabilities::execution::spill_path("../call/_2f/💥", "stdout");
    assert!(hostile.starts_with("/spill/"));
    assert_eq!(hostile.matches('/').count(), 2);
    assert!(!hostile.contains(".."));
    assert_ne!(
        crate::capabilities::execution::spill_path("/", "stdout"),
        crate::capabilities::execution::spill_path("_2f", "stdout")
    );

    let overlong = crate::capabilities::execution::spill_path(&"x".repeat(10_000), "stderr");
    assert!(
        overlong.len() <= 240,
        "overlong spill path: {}",
        overlong.len()
    );
    assert_eq!(overlong.matches('/').count(), 2);
}

struct RetentionCeilingExternalExecutor {
    requested_cap: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl verlet_process::execution::ExternalCommandExecutor for RetentionCeilingExternalExecutor {
    async fn exec(
        &self,
        request: verlet_process::execution::ExternalCommandRequest,
    ) -> verlet_process::VerletProcessResult<verlet_process::execution::ExternalCommandResult> {
        self.requested_cap.store(
            request.max_output_bytes,
            std::sync::atomic::Ordering::SeqCst,
        );
        Ok(verlet_process::execution::ExternalCommandResult::new(
            verlet_process::execution::VirtualCommandOutput {
                stdout: "x".repeat(request.max_output_bytes),
                stderr: String::new(),
                exit_code: 0,
                stdout_truncated: true,
                stderr_truncated: false,
            },
        ))
    }
}

#[tokio::test]
async fn retention_ceiling_truncation_still_spills_and_succeeds() {
    let executor = std::sync::Arc::new(RetentionCeilingExternalExecutor {
        requested_cap: std::sync::atomic::AtomicUsize::new(0),
    });
    let provider = crate::capabilities::execution::BashToolProvider::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig {
            max_output_bytes: usize::MAX,
            ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
        }
        .with_execution_policy(verlet_vbash::BashExecutionPolicy::host_always())
        .with_external_executor(executor.clone()),
    );

    let (result, is_error) =
        tool_result_json(invoke_bash(&provider, "call_retention_ceiling", "runaway-output").await);

    assert!(!is_error, "{result}");
    assert_eq!(
        executor
            .requested_cap
            .load(std::sync::atomic::Ordering::SeqCst),
        verlet_vbash::SPILL_RETENTION_MAX_BYTES
    );
    assert_eq!(result["stdout_truncated"], true);
    assert_eq!(
        result["spill"]["stdout"]["total_bytes"],
        verlet_vbash::SPILL_RETENTION_MAX_BYTES
    );
    assert_eq!(result["spill"]["stdout"]["retention_truncated"], true);
    assert!(
        result["stdout"]
            .as_str()
            .unwrap()
            .contains("exceeded the 67108864-byte retention ceiling")
    );
    let harness = provider.harness.lock().await;
    assert_eq!(
        harness
            .as_ref()
            .unwrap()
            .read_file("/spill/call_retention_ceiling.stdout.txt")
            .await
            .unwrap()
            .len(),
        verlet_vbash::SPILL_RETENTION_MAX_BYTES
    );
}

#[test]
fn spill_payload_decodes_in_old_and_new_reader_shapes() {
    let old = r#"{"stdout":"ok","stderr":"","exit_code":0,"stdout_truncated":false,"stderr_truncated":false}"#;
    let decoded: crate::capabilities::execution::BashToolResultPayload =
        serde_json::from_str(old).unwrap();
    assert!(decoded.spill.is_empty());

    let new = r#"{"stdout":"preview","stderr":"","exit_code":0,"stdout_truncated":true,"stderr_truncated":false,"spill":{"stdout":{"path":"/spill/c.stdout.txt","total_bytes":20000,"preview_bytes":16384}}}"#;
    let decoded: crate::capabilities::execution::BashToolResultPayload =
        serde_json::from_str(new).unwrap();
    let receipt = decoded.spill.stdout.unwrap();
    assert_eq!(receipt.path, "/spill/c.stdout.txt");
    assert!(!receipt.retention_truncated);

    let retention_truncated = r#"{"stdout":"preview","stderr":"","exit_code":0,"stdout_truncated":true,"stderr_truncated":false,"spill":{"stdout":{"path":"/spill/c.stdout.txt","total_bytes":67108864,"preview_bytes":16384,"retention_truncated":true}}}"#;
    let decoded: crate::capabilities::execution::BashToolResultPayload =
        serde_json::from_str(retention_truncated).unwrap();
    assert!(decoded.spill.stdout.unwrap().retention_truncated);

    #[derive(serde::Deserialize)]
    struct LegacyBashToolResult {
        stdout: String,
        stderr: String,
        exit_code: i32,
        stdout_truncated: bool,
        stderr_truncated: bool,
    }
    let legacy: LegacyBashToolResult = serde_json::from_str(new).unwrap();
    assert_eq!(legacy.stdout, "preview");
    assert_eq!(legacy.stderr, "");
    assert_eq!(legacy.exit_code, 0);
    assert!(legacy.stdout_truncated);
    assert!(!legacy.stderr_truncated);

    let legacy: LegacyBashToolResult = serde_json::from_str(retention_truncated).unwrap();
    assert_eq!(legacy.stdout, "preview");
}

#[tokio::test]
async fn process_exec_tool_starts_and_polls_virtual_bash_handle() {
    let (provider, turn_context) = process_test_provider();

    let started = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_process_start".to_string(),
            tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "sleep 0.05; echo done",
                "yield_time_ms": 1,
                "timeout_ms": 1000,
                "output_bytes_cap": 1024
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, is_error) = tool_result_json(started);
    assert!(!is_error, "{started}");
    assert_eq!(started["status"].as_str(), Some("running"));
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let completed = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_process_poll".to_string(),
            tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "process_id": process_id.clone(),
                "yield_time_ms": 1000,
                "output_bytes_cap": 1024
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let (completed, is_error) = tool_result_json(completed);
    assert!(!is_error, "{completed}");
    assert_eq!(completed["status"].as_str(), Some("completed"));
    assert!(completed["stdout"].as_str().unwrap().contains("done"));
    assert_eq!(completed["process_id"].as_str(), Some(process_id.as_str()));
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_a_process_poll_terminates_the_live_handle_and_returns_its_snapshot() {
    let store: std::sync::Arc<dyn verlet_history::RuntimeStore> =
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "process-cancel");
    let dispatcher = crate::kernel::process_handle_dispatch::test_process_dispatcher(
        std::sync::Arc::clone(&store),
        coordinates.clone(),
    );
    let turn_context = crate::kernel::runtime_host::turn::TurnContext::new(
        verlet_runtime_contracts::ThreadContext::root(coordinates),
        "process-cancel-turn",
        &crate::kernel::runtime_host::turn::TurnInput::text("process cancellation test"),
        tokio_util::sync::CancellationToken::new(),
    )
    .snapshot();
    let provider = std::sync::Arc::new(
        crate::capabilities::execution::BashToolProvider::new(
            crate::capabilities::execution::VirtualBashRuntimeConfig::default()
                .with_execution_policy(verlet_vbash::BashExecutionPolicy::host_always())
                .with_host_bash_executor("/"),
        )
        .with_process_dispatcher(dispatcher),
    );
    let started = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_process_start_for_cancel".to_string(),
            tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "sleep 60",
                "yield_time_ms": 1,
                "timeout_ms": 60_000,
                "output_bytes_cap": 1024
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, _) = tool_result_json(started);
    let process_id = started["process_id"].as_str().unwrap().to_string();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let poll = tokio::spawn({
        let provider = std::sync::Arc::clone(&provider);
        let process_id = process_id.clone();
        let cancellation = cancellation.clone();
        async move {
            provider
                .invoke_tool_call_cancellable(
                    crate::agent::agent_tool_router::AgentKernelToolCall {
                        call_id: "call_process_cancel_poll".to_string(),
                        tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
                        arguments: serde_json::json!({
                            "process_id": process_id,
                            "yield_time_ms": 10_000,
                            "output_bytes_cap": 1024
                        }),
                        turn_context: None,
                    },
                    crate::agent::agent_tool_router::ToolInvocationCancellation::new(
                        cancellation,
                        std::time::Duration::from_secs(1),
                    ),
                )
                .await
        }
    });
    tokio::task::yield_now().await;
    cancellation.cancel();

    let crate::agent::agent_tool_router::AgentKernelToolOutcome::Completed(Some(cancelled)) =
        tokio::time::timeout(std::time::Duration::from_secs(30), poll)
            .await
            .expect("cancelled process poll did not return promptly")
            .unwrap()
            .unwrap()
    else {
        panic!("process poll did not return a completed tool result");
    };
    let (cancelled, is_error) = tool_result_json(cancelled);
    assert!(is_error, "{cancelled}");
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["process_id"], process_id);
}

#[tokio::test]
async fn process_exec_tool_spills_a_completed_initial_snapshot() {
    let (provider, turn_context) = process_test_provider();

    let completed = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_process_exec_spill".to_string(),
            tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "yes q | head -c 20000; yes e | head -c 20000 >&2",
                "yield_time_ms": 1000,
                "timeout_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (completed, is_error) = tool_result_json(completed);

    assert!(!is_error, "{completed}");
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["stdout_truncated"], true);
    assert_eq!(
        completed["spill"]["stdout"]["path"],
        "/spill/call_process_exec_spill.stdout.txt"
    );
    assert_eq!(completed["spill"]["stdout"]["total_bytes"], 20_000);
    assert_eq!(
        completed["spill"]["stderr"]["path"],
        "/spill/call_process_exec_spill.stderr.txt"
    );
    assert_eq!(completed["spill"]["stderr"]["total_bytes"], 20_000);
    assert!(
        completed["stdout"]
            .as_str()
            .unwrap()
            .contains("Tip: cat /spill/call_process_exec_spill.stdout.txt")
    );
}

#[tokio::test]
async fn process_poll_tool_spills_the_complete_later_snapshot() {
    let (provider, turn_context) = process_test_provider();
    let started = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_process_start_for_spill".to_string(),
            tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "sleep 0.05; yes p | head -c 20000",
                "yield_time_ms": 1,
                "timeout_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, is_error) = tool_result_json(started);
    assert!(!is_error, "{started}");
    assert_eq!(started["status"], "running");
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let completed = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_process_poll_spill".to_string(),
            tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "process_id": process_id,
                "yield_time_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let (completed, is_error) = tool_result_json(completed);

    assert!(!is_error, "{completed}");
    assert_eq!(completed["status"], "completed");
    assert_eq!(
        completed["spill"]["stdout"]["path"],
        "/spill/call_process_poll_spill.stdout.txt"
    );
    assert_eq!(completed["spill"]["stdout"]["total_bytes"], 20_000);
    let harness = provider.harness.lock().await;
    assert_eq!(
        harness
            .as_ref()
            .unwrap()
            .read_file("/spill/call_process_poll_spill.stdout.txt")
            .await
            .unwrap(),
        b"p\n".repeat(10_000)
    );
}

#[tokio::test]
async fn write_stdin_tool_reports_unsupported_for_virtual_bash_handle() {
    let (provider, turn_context) = process_test_provider();
    let started = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_process_start".to_string(),
            tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "sleep 0.05; echo done",
                "yield_time_ms": 1,
                "timeout_ms": 1000,
                "output_bytes_cap": 1024
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, _) = tool_result_json(started);
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let unsupported = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_stdin".to_string(),
            tool_name: crate::capabilities::execution::WRITE_STDIN_TOOL.to_string(),
            arguments: serde_json::json!({
                "process_id": process_id,
                "delta_base64": "aGkK",
                "yield_time_ms": 1,
                "output_bytes_cap": 1024
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let (unsupported, is_error) = tool_result_json(unsupported);
    assert!(is_error, "{unsupported}");
    assert_eq!(unsupported["status"].as_str(), Some("unsupported"));
    assert!(unsupported["error"].as_str().unwrap().contains("stdin"));
}

struct StdinOutputBackend {
    requested_cap: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl verlet_process::live::LiveProcessBackend for StdinOutputBackend {
    fn backend_kind(&self) -> verlet_process::process::VerletProcessBackend {
        verlet_process::process::VerletProcessBackend::VirtualBash
    }

    async fn start(
        &self,
        request: verlet_process::live::LiveProcessStartRequest,
        process: verlet_process::process::VerletProcessHandle,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> verlet_process::VerletProcessResult<verlet_process::live::LiveProcessSpawn> {
        self.requested_cap.store(
            request.output_cap_bytes,
            std::sync::atomic::Ordering::SeqCst,
        );
        process.record(verlet_process::process::VerletProcessEventKind::Started {
            command: Some("stdin-output-test".to_string()),
        });
        let (stdin, mut input) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
        let join = tokio::spawn(async move {
            tokio::select! {
                delta = input.recv() => {
                    if delta.is_some() {
                        process.record(verlet_process::process::VerletProcessEventKind::Stdout {
                            bytes: vec![b'w'; 20_000],
                        });
                        process.record(verlet_process::process::VerletProcessEventKind::Completed {
                            status: verlet_process::process::VerletProcessExitStatus::exited(0),
                        });
                    }
                }
                _ = cancellation.cancelled() => {
                    process.record(verlet_process::process::VerletProcessEventKind::Cancelled {
                        reason: "cancelled".to_string(),
                    });
                }
            }
            Ok(())
        });
        Ok(verlet_process::live::LiveProcessSpawn {
            stdin: Some(stdin),
            join,
        })
    }
}

#[tokio::test]
async fn write_stdin_snapshot_uses_the_shared_spill_and_retention_bounds() {
    let (mut provider, turn_context) = process_test_provider();
    let requested_cap = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    provider.live_backend = std::sync::Arc::new(StdinOutputBackend {
        requested_cap: std::sync::Arc::clone(&requested_cap),
    });

    let started = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_stdin_start".to_string(),
            tool_name: crate::capabilities::execution::PROCESS_EXEC_TOOL.to_string(),
            arguments: serde_json::json!({
                "command": "wait-for-stdin",
                "yield_time_ms": 1,
                "timeout_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: Some(turn_context),
        })
        .await
        .unwrap()
        .unwrap();
    let (started, is_error) = tool_result_json(started);
    assert!(!is_error, "{started}");
    let process_id = started["process_id"].as_str().unwrap();

    let written = provider
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_stdin_spill".to_string(),
            tool_name: crate::capabilities::execution::WRITE_STDIN_TOOL.to_string(),
            arguments: serde_json::json!({
                "process_id": process_id,
                "delta_base64": "aGkK",
                "yield_time_ms": 1000,
                "output_bytes_cap": 64
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let (written, is_error) = tool_result_json(written);

    assert!(!is_error, "{written}");
    assert_eq!(
        requested_cap.load(std::sync::atomic::Ordering::SeqCst),
        verlet_vbash::SPILL_RETENTION_MAX_BYTES
    );
    assert_eq!(written["status"], "completed");
    assert_eq!(
        written["spill"]["stdout"]["path"],
        "/spill/call_stdin_spill.stdout.txt"
    );
    assert_eq!(written["spill"]["stdout"]["total_bytes"], 20_000);
    assert!(
        written["spill"]["stdout"]
            .get("retention_truncated")
            .is_none()
    );
}

struct EscapingExternalExecutor;

#[async_trait::async_trait]
impl verlet_process::execution::ExternalCommandExecutor for EscapingExternalExecutor {
    async fn exec(
        &self,
        _request: verlet_process::execution::ExternalCommandRequest,
    ) -> verlet_process::VerletProcessResult<verlet_process::execution::ExternalCommandResult> {
        Ok(verlet_process::execution::ExternalCommandResult::new(
            verlet_process::execution::VirtualCommandOutput {
                stdout: String::new(),
                stderr: String::new(),
                exit_code: 0,
                stdout_truncated: false,
                stderr_truncated: false,
            },
        )
        .with_file_write(verlet_process::execution::ExternalFileWrite::new(
            "/workspace/../escape.txt",
            "bad\n",
        )))
    }
}

#[tokio::test]
async fn harness_rejects_external_file_writes_outside_normalized_vfs_paths() {
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(
        crate::capabilities::execution::VirtualBashRuntimeConfig::default()
            .with_execution_policy(verlet_vbash::BashExecutionPolicy::host_always())
            .with_external_executor(std::sync::Arc::new(EscapingExternalExecutor)),
    )
    .await
    .unwrap();

    let err = harness.execute("cargo test --lib").await.unwrap_err();

    assert!(err.to_string().contains("must be normalized"));
}

#[tokio::test]
async fn harness_uses_configured_mounts_instead_of_hardcoded_mounts() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig {
        cwd: std::path::PathBuf::from("/work"),
        mounts: vec![
            verlet_vbash::VirtualMount::writable("/work").with_file("seed.txt", "seed\n"),
            verlet_vbash::VirtualMount::readonly(
                "/docs",
                vec![verlet_vbash::VirtualFile::new("guide.txt", "read me\n")],
            ),
        ],
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute(
            "cat /work/seed.txt && echo changed > /work/new.txt \
                 && cat /docs/guide.txt && test ! -e /workspace",
        )
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("seed"));
    assert!(output.stdout.contains("read me"));
    assert_eq!(
        String::from_utf8(harness.read_file("/work/new.txt").await.unwrap()).unwrap(),
        "changed\n"
    );

    let output = harness
        .execute("echo nope > /docs/guide.txt")
        .await
        .unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(output.stderr.contains("read-only") || output.stderr.contains("denied"));
}

#[tokio::test]
async fn object_store_mount_has_read_your_writes_and_persists_final_state() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new())
        as std::sync::Arc<dyn object_store::ObjectStore>;
    let prefix = "tenant-a/session-a";
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig {
        cwd: std::path::PathBuf::from("/s3"),
        mounts: vec![verlet_vbash::VirtualMount::object_store(
            "/s3",
            verlet_vfs::ObjectStoreMountConfig::shared(store.clone(), prefix),
        )],
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute(
            "mkdir -p docs \
                 && echo alpha > docs/a.txt \
                 && echo beta >> docs/a.txt \
                 && cat docs/a.txt \
                 && cp docs/a.txt docs/b.txt \
                 && mv docs/b.txt docs/c.txt \
                 && rm docs/c.txt \
                 && test ! -e docs/c.txt \
                 && ls docs",
        )
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("alpha"));
    assert!(output.stdout.contains("beta"));
    assert!(output.stdout.contains("a.txt"));
    assert!(!output.stdout.contains("c.txt"));
    assert!(harness.mutations().iter().any(|mutation| {
        mutation.kind == verlet_vfs::VfsMutationKind::Write
            && mutation.path == std::path::Path::new("/s3/docs/a.txt")
    }));

    let stored = store
        .get(&object_store::path::Path::from(
            "tenant-a/session-a/docs/a.txt",
        ))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), b"alpha\nbeta\n");
    assert!(matches!(
        store
            .head(&object_store::path::Path::from(
                "tenant-a/session-a/docs/c.txt"
            ))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));

    let reload_config = crate::capabilities::execution::VirtualBashRuntimeConfig {
        cwd: std::path::PathBuf::from("/s3"),
        mounts: vec![verlet_vbash::VirtualMount::object_store(
            "/s3",
            verlet_vfs::ObjectStoreMountConfig::shared(store.clone(), prefix),
        )],
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let mut reloaded = verlet_vbash::harness::BashkitExecutionHarness::new(reload_config)
        .await
        .unwrap();
    let output = reloaded
        .execute("cat docs/a.txt && test ! -e docs/c.txt")
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "alpha\nbeta\n");
}

#[tokio::test]
async fn object_store_mounts_are_prefix_isolated_and_can_be_readonly() {
    let store = std::sync::Arc::new(object_store::memory::InMemory::new())
        as std::sync::Arc<dyn object_store::ObjectStore>;
    store
        .put(
            &object_store::path::Path::from("readonly/guide.txt"),
            Vec::from("read only\n").into(),
        )
        .await
        .unwrap();

    let config = crate::capabilities::execution::VirtualBashRuntimeConfig {
        cwd: std::path::PathBuf::from("/a"),
        mounts: vec![
            verlet_vbash::VirtualMount::object_store(
                "/a",
                verlet_vfs::ObjectStoreMountConfig::shared(store.clone(), "tenant-a"),
            ),
            verlet_vbash::VirtualMount::object_store(
                "/b",
                verlet_vfs::ObjectStoreMountConfig::shared(store.clone(), "tenant-b"),
            ),
            verlet_vbash::VirtualMount::readonly_object_store(
                "/docs",
                verlet_vfs::ObjectStoreMountConfig::shared(store.clone(), "readonly"),
            ),
        ],
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute("echo alpha > /a/file.txt && test ! -e /b/file.txt && cat /docs/guide.txt")
        .await
        .unwrap();
    assert!(output.success(), "{output:?}");
    assert!(output.stdout.contains("read only"));
    assert!(
        store
            .head(&object_store::path::Path::from("tenant-a/file.txt"))
            .await
            .is_ok()
    );
    assert!(matches!(
        store
            .head(&object_store::path::Path::from("tenant-b/file.txt"))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));

    let output = harness.execute("echo nope > /docs/new.txt").await.unwrap();
    assert_ne!(output.exit_code, 0);
    assert!(output.stderr.contains("read-only") || output.stderr.contains("denied"));
    assert!(matches!(
        store
            .head(&object_store::path::Path::from("readonly/new.txt"))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn harness_rejects_bad_mount_config() {
    let duplicate = crate::capabilities::execution::VirtualBashRuntimeConfig {
        mounts: vec![
            verlet_vbash::VirtualMount::writable("/work"),
            verlet_vbash::VirtualMount::readonly("/work", Vec::new()),
        ],
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let err = match verlet_vbash::harness::BashkitExecutionHarness::new(duplicate).await {
        Ok(_) => panic!("duplicate mount config should fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("duplicate virtual mount path"));

    let relative = crate::capabilities::execution::VirtualBashRuntimeConfig {
        mounts: vec![verlet_vbash::VirtualMount::writable("relative")],
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let err = match verlet_vbash::harness::BashkitExecutionHarness::new(relative).await {
        Ok(_) => panic!("relative mount config should fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("must be absolute"));
}

#[tokio::test]
async fn runtime_isolates_virtual_filesystems_by_thread() {
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
    ));
    let first = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant-a", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let second = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant-b", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut first_events = first.subscribe_events();
    let mut second_events = second.subscribe_events();

    first
        .send(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                turn_id: "one".to_string(),
                input: crate::kernel::runtime_host::turn::TurnInput::text(
                    "echo tenant-a > marker && cat marker",
                ),
                mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
            },
        )
        .await
        .unwrap();
    second
        .send(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                turn_id: "two".to_string(),
                input: crate::kernel::runtime_host::turn::TurnInput::text(
                    "test ! -e marker && echo isolated",
                ),
                mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
            },
        )
        .await
        .unwrap();

    assert!(expect_output(&mut first_events).await.contains("tenant-a"));
    assert!(expect_output(&mut second_events).await.contains("isolated"));
}

#[tokio::test]
async fn runtime_cancels_busy_virtual_bash_without_poisoning_thread() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig {
        execution_timeout: std::time::Duration::from_secs(30),
        max_output_bytes: 1024,
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::capabilities::execution::VirtualBashRuntimeFactory::new(config),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    thread
        .send(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                turn_id: "loop".to_string(),
                input: crate::kernel::runtime_host::turn::TurnInput::text("while true; do :; done"),
                mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
            },
        )
        .await
        .unwrap();
    thread.cancel("stop loop").await.unwrap();

    let cancelled = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                reason, ..
            } = events.recv().await.unwrap()
            {
                break reason;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(cancelled, "stop loop");

    thread
        .send(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                turn_id: "after".to_string(),
                input: crate::kernel::runtime_host::turn::TurnInput::text(
                    "echo ok > after.txt && cat after.txt",
                ),
                mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
            },
        )
        .await
        .unwrap();
    assert!(expect_output(&mut events).await.contains("ok"));
}

#[tokio::test]
async fn runtime_cancels_busy_virtual_bash_with_operation_registry_and_recovers() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig {
        execution_timeout: std::time::Duration::from_secs(30),
        max_output_bytes: 1024,
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
            .with_operation_registry(echo_operation_registry().await)
    };
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::capabilities::execution::VirtualBashRuntimeFactory::new(config),
    ));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    thread
        .send(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                turn_id: "loop-after-op".to_string(),
                input: crate::kernel::runtime_host::turn::TurnInput::text(
                    "echo before | verlet run echoer echo && while true; do :; done",
                ),
                mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
            },
        )
        .await
        .unwrap();
    thread.cancel("stop projected process").await.unwrap();

    let cancelled = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if let crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                reason, ..
            } = events.recv().await.unwrap()
            {
                break reason;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(cancelled, "stop projected process");

    thread
        .send(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                turn_id: "after-projected-cancel".to_string(),
                input: crate::kernel::runtime_host::turn::TurnInput::text(
                    "echo after | verlet run echoer echo",
                ),
                mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
            },
        )
        .await
        .unwrap();
    assert!(expect_output(&mut events).await.contains("op:after"));
}

#[tokio::test]
async fn runtime_does_not_project_agent_process_as_virtual_bash_processes() {
    let config = crate::capabilities::execution::VirtualBashRuntimeConfig {
        execution_timeout: std::time::Duration::from_secs(5),
        max_output_bytes: 4096,
        ..crate::capabilities::execution::VirtualBashRuntimeConfig::default()
    };
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::capabilities::execution::VirtualBashRuntimeFactory::new(config),
    ));
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
        "agent-process",
        r#"
agent ps 2>&1 || echo agent-missing
checkpoint create --label parent-save 2>&1 || echo checkpoint-missing
"#,
    )
    .await
    .unwrap();

    let output = expect_output(&mut events).await;
    assert!(output.contains("agent-missing"), "output:\n{output}");
    assert!(output.contains("checkpoint-missing"), "output:\n{output}");
    assert!(!output.contains("cooldis.wait_thread"), "output:\n{output}");
    assert!(
        !output.contains("cooldis.create_checkpoint"),
        "output:\n{output}"
    );

    host.shutdown_all().await.unwrap();
}
