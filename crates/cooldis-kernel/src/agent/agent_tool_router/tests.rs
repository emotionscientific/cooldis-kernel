use super::*;
use crate::{
    CHANNEL_EMIT_CAPABILITY, CHANNEL_EMIT_OPERATION, COOLDIS_NOTIFY_PACKAGE,
    COOLDIS_PROCESS_PACKAGE, KERNEL_RUNTIME_KIND, KernelNotifyOperationProvider,
    KernelOperationDispatcher, KernelOperationRegistration, KernelProcessOperationProvider,
    NOTIFY_PREVIEW_OPERATION, OPERATION_METADATA_RUNTIME_KIND, OperationRegistration,
    PROCESS_EXEC_OPERATION, ThreadContext, ThreadCoordinates, TurnInput, WasmRuntimeArtifact,
    cooldis_notify_kernel_package, cooldis_process_kernel_package,
};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn router_projects_registry_operations_as_tool_definitions() {
    let router = router_with_echo_operation("echo").await;

    let definitions = router.tool_definitions().await;

    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name, "echo_search");
    assert_eq!(definitions[0].input_schema["required"][0], "input");
}

#[tokio::test]
async fn router_projects_kernel_tool_definitions_with_registry_operations() {
    let router = router_with_echo_operation("echo")
        .await
        .with_kernel_tool_provider(Arc::new(FakeKernelToolProvider));

    let definitions = router.tool_definitions().await;
    let names = definitions
        .iter()
        .map(|definition| definition.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(names, vec!["echo_search", "record_context"]);
}

#[tokio::test]
async fn router_invokes_registry_operation_and_returns_tool_result() {
    let router = router_with_echo_operation("echo").await;

    let result = router
        .invoke_tool_call(
            "call_1",
            "echo_search",
            json!({
                "input": "cooldis"
            }),
        )
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if content == vec![crate::CanonicalContent::text("echo:cooldis")]
    ));
}

#[tokio::test]
async fn router_invokes_kernel_tool_provider() {
    let router = AgentToolRouter::new(Arc::new(OperationRegistry::new()))
        .with_kernel_tool_provider(Arc::new(FakeKernelToolProvider));

    let result = router
        .invoke_tool_call("call_1", "record_context", json!({}))
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if tool_result_text(&content) == r#"{"status":"idle"}"#
    ));
}

#[tokio::test]
async fn router_outcome_adapter_wraps_synchronous_kernel_tools_as_completed() {
    let router = AgentToolRouter::new(Arc::new(OperationRegistry::new()))
        .with_kernel_tool_provider(Arc::new(FakeKernelToolProvider));

    let result = router
        .invoke_tool_call_outcome("call_1", "record_context", json!({}))
        .await
        .unwrap();

    assert!(matches!(
        result,
        AgentKernelToolOutcome::Completed(Some(CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        })) if tool_result_text(&content) == r#"{"status":"idle"}"#
    ));
}

#[tokio::test]
async fn router_passes_turn_context_to_kernel_tool_provider() {
    let seen = Arc::new(Mutex::new(Vec::<Option<TurnContextSnapshot>>::new()));
    let router = AgentToolRouter::new(Arc::new(OperationRegistry::new()))
        .with_kernel_tool_provider(Arc::new(RecordingKernelToolProvider {
            seen: Arc::clone(&seen),
        }));
    let input = TurnInput::text("hello")
        .with_cwd("/tmp/cooldis-turn")
        .with_model("gpt-test")
        .with_provider("openai")
        .with_permission_profile("workspace-write")
        .with_metadata("source", "test");
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let turn_context = TurnContext::new(
        ThreadContext::root(coordinates.clone()),
        "turn-1",
        &input,
        CancellationToken::new(),
    );

    let result = router
        .invoke_tool_call_for_turn(&turn_context, "call_1", "record_context", json!({}))
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: false,
            ..
        }
    ));
    let snapshots = seen.lock().unwrap();
    let snapshot = snapshots[0].as_ref().expect("turn context snapshot");
    assert_eq!(snapshot.turn_id, "turn-1");
    assert_eq!(snapshot.coordinates, coordinates);
    assert_eq!(snapshot.model.as_deref(), Some("gpt-test"));
    assert_eq!(snapshot.provider.as_deref(), Some("openai"));
    assert_eq!(
        snapshot.permission_profile.as_deref(),
        Some("workspace-write")
    );
    assert_eq!(
        snapshot.metadata.get("source").map(String::as_str),
        Some("test")
    );
    assert!(!snapshot.cancellation_requested);
}

#[tokio::test]
async fn router_returns_error_tool_result_for_bad_input_shape() {
    let router = router_with_echo_operation("echo").await;

    let result = router
        .invoke_tool_call("call_1", "echo_search", json!({"other": "cooldis"}))
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: true,
            content,
            ..
        } if tool_result_text(&content).contains("requires a string input field")
    ));
}

#[tokio::test]
async fn router_rejects_non_object_json_tool_arguments() {
    let router = router_with_operation("json-echo", "echo", "json", Vec::new()).await;

    let result = router
        .invoke_tool_call("call_1", "json_echo_search", json!("not an object"))
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: true,
            content,
            ..
        } if tool_result_text(&content).contains("requires object arguments")
    ));
}

#[tokio::test]
async fn router_rejects_operation_when_tool_invocation_lacks_required_capability() {
    let router = router_with_operation(
        "secret-echo",
        "echo",
        "bytes",
        vec!["secret:EXAMPLE_API_KEY"],
    )
    .await;

    let result = router
        .invoke_tool_call("call_1", "secret_echo_search", json!({"input": "cooldis"}))
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: true,
            content,
            ..
        } if tool_result_text(&content).contains("missing capability grants: secret:EXAMPLE_API_KEY")
    ));
}

#[tokio::test]
async fn router_invokes_capability_protected_operation_when_granted() {
    let router = router_with_operation(
        "secret-echo",
        "echo",
        "bytes",
        vec!["secret:EXAMPLE_API_KEY"],
    )
    .await
    .with_capability_grant("secret:EXAMPLE_API_KEY");

    let result = router
        .invoke_tool_call("call_1", "secret_echo_search", json!({"input": "cooldis"}))
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if tool_result_text(&content) == "echo:cooldis"
    ));
}

#[tokio::test]
async fn router_invokes_kernel_process_operation_alias() {
    let cwd = temp_cwd("router");
    let router = router_with_kernel_process_operation(cwd.clone()).await;

    let result = router
        .invoke_tool_call(
            "call_1",
            PROCESS_EXEC_OPERATION,
            json!({
                "command": ["/bin/pwd"],
                "yield_time_ms": 1_000,
                "timeout_ms": 2_000
            }),
        )
        .await;

    let CanonicalMessage::ToolResult {
        is_error: false,
        content,
        ..
    } = result
    else {
        panic!("expected successful tool result");
    };
    let receipt = serde_json::from_str::<Value>(&tool_result_text(&content)).unwrap();
    assert_eq!(receipt["operation"], "cooldis.process_exec");
    assert_eq!(receipt["status"], "completed");
    assert_eq!(receipt["backend"], "host_bash");
    assert_eq!(
        receipt["stdout"].as_str().unwrap().trim(),
        cwd.display().to_string()
    );
    assert!(receipt.get("process_id").is_none());
}

#[tokio::test]
async fn router_invokes_kernel_notify_operation_alias_without_delivery_claim() {
    let router = router_with_kernel_notify_operation().await;

    let result = router
        .invoke_tool_call(
            "call_1",
            CHANNEL_EMIT_OPERATION,
            json!({
                "channel": "slack",
                "message": "Ready for review",
                "thread_id": "thread-1"
            }),
        )
        .await;

    let CanonicalMessage::ToolResult {
        is_error: false,
        content,
        ..
    } = result
    else {
        panic!("expected successful tool result");
    };
    let receipt = serde_json::from_str::<Value>(&tool_result_text(&content)).unwrap();
    assert_eq!(receipt["operation"], "cooldis.channel_emit");
    assert_eq!(receipt["status"], "recorded");
    assert_eq!(receipt["delivery"], "not_sent");
    assert_eq!(receipt["channel_decision_required"], true);
    assert_eq!(receipt["channel"], "slack");
    assert_eq!(receipt["message"], "Ready for review");
}

async fn router_with_echo_operation(name: &str) -> AgentToolRouter {
    router_with_operation(name, "echo", "bytes", Vec::new()).await
}

async fn router_with_operation(
    name: &str,
    prefix: &str,
    input: &str,
    required_capabilities: Vec<&str>,
) -> AgentToolRouter {
    let registry = Arc::new(OperationRegistry::new());
    let wasm = wat::parse_str(echo_operation_guest(prefix, input, &required_capabilities))
        .expect("echo operation fixture should compile");
    let mut registration = OperationRegistration::new(name, WasmRuntimeArtifact::bytes(wasm));
    for capability in required_capabilities {
        registration = registration.with_capability_grant(capability);
    }
    registry.register(registration).await.unwrap();
    AgentToolRouter::new(registry)
}

async fn router_with_kernel_process_operation(cwd: PathBuf) -> AgentToolRouter {
    let registry = Arc::new(OperationRegistry::new());
    let package = cooldis_process_kernel_package();
    let context = ThreadContext::root(ThreadCoordinates::new("tenant", "user", "session"));
    let dispatcher: Arc<dyn KernelOperationDispatcher> =
        Arc::new(KernelProcessOperationProvider::new(context, cwd));
    let mut registration =
        KernelOperationRegistration::new(COOLDIS_PROCESS_PACKAGE, package.manifest.clone())
            .with_capability_grants(package.capability_grants.clone())
            .with_dispatcher(dispatcher);
    registration.metadata.insert(
        OPERATION_METADATA_RUNTIME_KIND.to_string(),
        json!(KERNEL_RUNTIME_KIND),
    );
    registry.register_kernel(registration).await.unwrap();
    AgentToolRouter::new(registry)
        .with_capability_grants(package.capability_grants)
        .with_tool_aliases(vec![OperationToolAlias {
            tool_name: PROCESS_EXEC_OPERATION.to_string(),
            registered_name: COOLDIS_PROCESS_PACKAGE.to_string(),
            operation_name: PROCESS_EXEC_OPERATION.to_string(),
        }])
}

async fn router_with_kernel_notify_operation() -> AgentToolRouter {
    let registry = Arc::new(OperationRegistry::new());
    let package = cooldis_notify_kernel_package();
    let dispatcher: Arc<dyn KernelOperationDispatcher> = Arc::new(KernelNotifyOperationProvider);
    let mut registration =
        KernelOperationRegistration::new(COOLDIS_NOTIFY_PACKAGE, package.manifest.clone())
            .with_capability_grants(package.capability_grants.clone())
            .with_dispatcher(dispatcher);
    registration.metadata.insert(
        OPERATION_METADATA_RUNTIME_KIND.to_string(),
        json!(KERNEL_RUNTIME_KIND),
    );
    registry.register_kernel(registration).await.unwrap();
    AgentToolRouter::new(registry)
        .with_capability_grant(CHANNEL_EMIT_CAPABILITY)
        .with_tool_aliases(vec![
            OperationToolAlias {
                tool_name: NOTIFY_PREVIEW_OPERATION.to_string(),
                registered_name: COOLDIS_NOTIFY_PACKAGE.to_string(),
                operation_name: NOTIFY_PREVIEW_OPERATION.to_string(),
            },
            OperationToolAlias {
                tool_name: CHANNEL_EMIT_OPERATION.to_string(),
                registered_name: COOLDIS_NOTIFY_PACKAGE.to_string(),
                operation_name: CHANNEL_EMIT_OPERATION.to_string(),
            },
        ])
}

fn temp_cwd(name: &str) -> PathBuf {
    let cwd = std::env::temp_dir().join(format!(
        "cooldis-agent-tool-router-{name}-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::canonicalize(cwd).unwrap()
}

fn tool_result_text(content: &[crate::CanonicalContent]) -> String {
    content
        .iter()
        .find_map(|content| match content {
            crate::CanonicalContent::Text { text, .. } => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn echo_operation_guest(prefix: &str, input: &str, required_capabilities: &[&str]) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "search",
            "input": input,
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": required_capabilities
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

struct FakeKernelToolProvider;

#[async_trait]
impl AgentKernelToolProvider for FakeKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "record_context",
            "Return the current Cooldis thread status.",
            json!({
                "type": "object",
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        if call.tool_name != "record_context" {
            return Ok(None);
        }
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            r#"{"status":"idle"}"#,
            false,
        )))
    }
}

struct RecordingKernelToolProvider {
    seen: Arc<Mutex<Vec<Option<TurnContextSnapshot>>>>,
}

#[async_trait]
impl AgentKernelToolProvider for RecordingKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "record_context",
            "Record the current Cooldis turn context.",
            json!({
                "type": "object",
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        self.seen.lock().unwrap().push(call.turn_context.clone());
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "recorded",
            false,
        )))
    }
}
