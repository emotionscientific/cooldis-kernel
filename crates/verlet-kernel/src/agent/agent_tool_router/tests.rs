use super::*;
use crate::{
    AgentManifestGrantExpiry, CHANNEL_EMIT_CAPABILITY, CHANNEL_EMIT_OPERATION, KERNEL_RUNTIME_KIND,
    KernelNotifyOperationProvider, KernelOperationDispatcher, KernelOperationRegistration,
    KernelProcessOperationProvider, NOTIFY_PREVIEW_OPERATION, OPERATION_METADATA_RUNTIME_KIND,
    OperationRegistration, PROCESS_EXEC_OPERATION, RuntimeHost, THREAD_CANCEL_OPERATION,
    THREAD_SPAWN_OPERATION, THREAD_STATUS_OPERATION, THREAD_SUBMIT_OPERATION,
    THREAD_WAIT_OPERATION, ThreadContext, ThreadCoordinates, ThreadTopology, TurnInput,
    VERLET_NOTIFY_PACKAGE, VERLET_PROCESS_PACKAGE, VirtualBashRuntimeFactory, WasmRuntimeArtifact,
    verlet_notify_kernel_package, verlet_process_kernel_package, verlet_threads_kernel_package,
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
                "input": "verlet"
            }),
        )
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if content == vec![crate::CanonicalContent::text("echo:verlet")]
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
        .with_cwd("/tmp/verlet-turn")
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
        .invoke_tool_call("call_1", "echo_search", json!({"other": "verlet"}))
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
        .invoke_tool_call("call_1", "secret_echo_search", json!({"input": "verlet"}))
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
        .invoke_tool_call("call_1", "secret_echo_search", json!({"input": "verlet"}))
        .await;

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if tool_result_text(&content) == "echo:verlet"
    ));
}

#[tokio::test]
async fn router_checks_grant_expiry_live_at_each_tool_invocation() {
    let router = router_with_operation(
        "secret-echo",
        "echo",
        "bytes",
        vec!["secret:EXAMPLE_API_KEY"],
    )
    .await
    .with_capability_grant("secret:EXAMPLE_API_KEY")
    .with_capability_grant("fs.read:/workspace")
    .with_capability_grant_expiries([
        AgentManifestGrantExpiry {
            capability: "secret:EXAMPLE_API_KEY".to_string(),
            expires_at: "1970-01-01T00:00:01Z".to_string(),
        },
        AgentManifestGrantExpiry {
            capability: "fs.read:/workspace".to_string(),
            expires_at: "1970-01-01T00:00:02Z".to_string(),
        },
    ]);

    let at_expiry = router
        .invoke_tool_call_at(
            "call_before",
            "secret_echo_search",
            json!({"input": "before"}),
            1_000,
        )
        .await;
    let after_expiry = router
        .invoke_tool_call_at(
            "call_after",
            "secret_echo_search",
            json!({"input": "after"}),
            1_001,
        )
        .await;

    assert!(matches!(
        at_expiry,
        CanonicalMessage::ToolResult {
            is_error: false,
            ..
        }
    ));
    assert!(matches!(
        after_expiry,
        CanonicalMessage::ToolResult {
            is_error: true,
            content,
            ..
        } if {
            let text = tool_result_text(&content);
            text.contains("missing capability grants: secret:EXAMPLE_API_KEY")
                && text.contains("1970-01-01T00:00:01Z")
        }
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
    assert!(receipt["process_id"].as_str().is_some());
    assert_eq!(receipt["dispatch_id"], "call_1");
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

#[tokio::test]
async fn router_addresses_child_threads_by_task_name_without_exposing_raw_ids() {
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::default()));
    let root = host
        .start_thread(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let router = router_with_kernel_thread_operations(&host, root.context().clone()).await;

    let spawn = router
        .invoke_tool_call(
            "spawn-call-1",
            THREAD_SPAWN_OPERATION,
            json!({"task_name": "worker", "message": "echo first"}),
        )
        .await;
    let retry = router
        .invoke_tool_call(
            "spawn-call-1",
            THREAD_SPAWN_OPERATION,
            json!({"task_name": "worker", "message": "echo retry"}),
        )
        .await;
    let duplicate = router
        .invoke_tool_call(
            "spawn-call-2",
            THREAD_SPAWN_OPERATION,
            json!({"task_name": "worker", "message": "echo duplicate"}),
        )
        .await;
    let submit = router
        .invoke_tool_call(
            "submit-call-1",
            THREAD_SUBMIT_OPERATION,
            json!({"task_name": "worker", "message": "echo steered"}),
        )
        .await;
    let status = router
        .invoke_tool_call(
            "status-call-1",
            THREAD_STATUS_OPERATION,
            json!({"task_name": "worker"}),
        )
        .await;
    let wait = router
        .invoke_tool_call(
            "wait-call-1",
            THREAD_WAIT_OPERATION,
            json!({"task_name": "worker", "timeout_ms": 1_000}),
        )
        .await;

    let children = host.children_of(root.context().coordinates.thread_id).await;
    assert_eq!(children.len(), 1);
    let child_id = children[0].context().coordinates.thread_id.to_string();
    let parent_id = root.context().coordinates.thread_id.to_string();

    for (message, operation) in [
        (&spawn, "cooldis.thread_spawn"),
        (&retry, "cooldis.thread_spawn"),
        (&submit, "cooldis.thread_submit"),
        (&wait, "cooldis.thread_wait"),
        (&status, "cooldis.thread_status"),
    ] {
        let CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } = message
        else {
            panic!("expected successful alias-only tool result: {message:?}");
        };
        let text = tool_result_text(content);
        assert!(!text.contains(&child_id), "child id leaked in {text}");
        assert!(!text.contains(&parent_id), "parent id leaked in {text}");
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["operation"], operation);
        assert_eq!(value["task_name"], "worker");
        assert!(value["status"].is_string());
        assert_eq!(value.as_object().unwrap().len(), 3);
    }

    let CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = duplicate
    else {
        panic!("expected duplicate task_name tool error");
    };
    let duplicate_error = tool_result_text(&content);
    assert!(duplicate_error.contains("task_name \"worker\" is already bound"));
    assert!(!duplicate_error.contains(&child_id));
    assert!(!duplicate_error.contains(&parent_id));

    let missing = router
        .invoke_tool_call(
            "status-missing",
            THREAD_STATUS_OPERATION,
            json!({"task_name": "missing"}),
        )
        .await;
    let CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = missing
    else {
        panic!("expected missing task_name tool error");
    };
    let missing_error = tool_result_text(&content);
    assert!(missing_error.contains("task_name \"missing\" was not found"));
    assert!(!missing_error.contains(&child_id));
    assert!(!missing_error.contains(&parent_id));

    let missing_wait = router
        .invoke_tool_call(
            "wait-missing",
            THREAD_WAIT_OPERATION,
            json!({"task_name": "missing", "timeout_ms": 1}),
        )
        .await;
    let CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = missing_wait
    else {
        panic!("expected missing wait task_name tool error");
    };
    let missing_wait_error = tool_result_text(&content);
    assert!(missing_wait_error.contains("task_name \"missing\" was not found"));
    assert!(!missing_wait_error.contains(&child_id));
    assert!(!missing_wait_error.contains(&parent_id));

    let rejected_raw_wait = router
        .invoke_tool_call(
            "wait-raw-id",
            THREAD_WAIT_OPERATION,
            json!({"target_thread_id": child_id, "timeout_ms": 1}),
        )
        .await;
    let CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = rejected_raw_wait
    else {
        panic!("expected raw-id wait input to fail decode");
    };
    let rejected_raw_wait_error = tool_result_text(&content);
    assert!(rejected_raw_wait_error.contains("unknown field `target_thread_id`"));
    assert!(!rejected_raw_wait_error.contains(&child_id));
    assert!(!rejected_raw_wait_error.contains(&parent_id));

    let cancel = router
        .invoke_tool_call(
            "cancel-call-1",
            THREAD_CANCEL_OPERATION,
            json!({"task_name": "worker"}),
        )
        .await;
    let CanonicalMessage::ToolResult {
        is_error: false,
        content,
        ..
    } = cancel
    else {
        panic!("expected successful task_name cancellation");
    };
    let cancel_text = tool_result_text(&content);
    assert!(!cancel_text.contains(&child_id));
    assert!(!cancel_text.contains(&parent_id));
    assert_eq!(
        serde_json::from_str::<Value>(&cancel_text).unwrap(),
        json!({
            "operation": "cooldis.thread_cancel",
            "status": "stopped",
            "task_name": "worker"
        })
    );

    let unavailable = router
        .invoke_tool_call(
            "status-call-2",
            THREAD_STATUS_OPERATION,
            json!({"task_name": "worker"}),
        )
        .await;
    let CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = unavailable
    else {
        panic!("expected unavailable task_name tool error");
    };
    let unavailable_error = tool_result_text(&content);
    assert!(
        unavailable_error.ends_with("thread_status task_name \"worker\" target is not available")
    );
    assert!(!unavailable_error.contains(&child_id));
    assert!(!unavailable_error.contains(&parent_id));

    host.shutdown_all().await.unwrap();
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
    let package = verlet_process_kernel_package();
    let context = ThreadContext::root(ThreadCoordinates::new("tenant", "user", "session"));
    let store: Arc<dyn crate::RuntimeStore> = Arc::new(crate::InMemorySessionStore::new());
    let process_dispatcher = crate::kernel::process_handle_dispatch::test_process_dispatcher(
        store,
        context.coordinates.clone(),
    );
    let dispatcher: Arc<dyn KernelOperationDispatcher> = Arc::new(
        KernelProcessOperationProvider::new(context, cwd)
            .with_process_dispatcher(process_dispatcher),
    );
    let mut registration =
        KernelOperationRegistration::new(VERLET_PROCESS_PACKAGE, package.manifest.clone())
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
            registered_name: VERLET_PROCESS_PACKAGE.to_string(),
            operation_name: PROCESS_EXEC_OPERATION.to_string(),
            grant_expiries: Vec::new(),
        }])
}

async fn router_with_kernel_notify_operation() -> AgentToolRouter {
    let registry = Arc::new(OperationRegistry::new());
    let package = verlet_notify_kernel_package();
    let dispatcher: Arc<dyn KernelOperationDispatcher> = Arc::new(KernelNotifyOperationProvider);
    let mut registration =
        KernelOperationRegistration::new(VERLET_NOTIFY_PACKAGE, package.manifest.clone())
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
                registered_name: VERLET_NOTIFY_PACKAGE.to_string(),
                operation_name: NOTIFY_PREVIEW_OPERATION.to_string(),
                grant_expiries: Vec::new(),
            },
            OperationToolAlias {
                tool_name: CHANNEL_EMIT_OPERATION.to_string(),
                registered_name: VERLET_NOTIFY_PACKAGE.to_string(),
                operation_name: CHANNEL_EMIT_OPERATION.to_string(),
                grant_expiries: Vec::new(),
            },
        ])
}

async fn router_with_kernel_thread_operations(
    host: &RuntimeHost,
    context: ThreadContext,
) -> AgentToolRouter {
    let registry = Arc::new(OperationRegistry::new());
    let package = verlet_threads_kernel_package();
    let dispatcher: Arc<dyn KernelOperationDispatcher> = Arc::new(
        crate::KernelThreadOperationProvider::new(host.kernel_control(), context),
    );
    let mut registration =
        KernelOperationRegistration::new(crate::VERLET_THREADS_PACKAGE, package.manifest.clone())
            .with_capability_grants(package.capability_grants.clone())
            .with_dispatcher(dispatcher);
    registration.metadata.insert(
        OPERATION_METADATA_RUNTIME_KIND.to_string(),
        json!(KERNEL_RUNTIME_KIND),
    );
    registry.register_kernel(registration).await.unwrap();
    AgentToolRouter::new(registry)
        .with_capability_grants(package.capability_grants)
        .with_tool_aliases(
            [
                THREAD_SPAWN_OPERATION,
                THREAD_SUBMIT_OPERATION,
                THREAD_WAIT_OPERATION,
                THREAD_STATUS_OPERATION,
                THREAD_CANCEL_OPERATION,
            ]
            .into_iter()
            .map(|operation| OperationToolAlias {
                tool_name: operation.to_string(),
                registered_name: crate::VERLET_THREADS_PACKAGE.to_string(),
                operation_name: operation.to_string(),
                grant_expiries: Vec::new(),
            }),
        )
}

fn temp_cwd(name: &str) -> PathBuf {
    let cwd = std::env::temp_dir().join(format!(
        "verlet-agent-tool-router-{name}-{}",
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

struct FakeKernelToolProvider;

#[async_trait]
impl AgentKernelToolProvider for FakeKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "record_context",
            "Return the current Verlet thread status.",
            json!({
                "type": "object",
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> VerletResult<Option<CanonicalMessage>> {
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
            "Record the current Verlet turn context.",
            json!({
                "type": "object",
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> VerletResult<Option<CanonicalMessage>> {
        self.seen.lock().unwrap().push(call.turn_context.clone());
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "recorded",
            false,
        )))
    }
}
