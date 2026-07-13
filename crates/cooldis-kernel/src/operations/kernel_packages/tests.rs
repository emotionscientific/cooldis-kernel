use super::*;
use cooldis_runtime_contracts::{validate_json_schema_subset, validate_json_value_against_schema};
use std::collections::BTreeSet;
use uuid::Uuid;

#[test]
fn cooldis_threads_package_declares_five_kernel_operations() {
    let package = cooldis_threads_kernel_package();
    let operations = package
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![
            THREAD_SPAWN_OPERATION,
            THREAD_SUBMIT_OPERATION,
            THREAD_WAIT_OPERATION,
            THREAD_STATUS_OPERATION,
            THREAD_CANCEL_OPERATION,
        ]
    );
    assert_eq!(package.interface.runtime.kind, KERNEL_RUNTIME_KIND);
    assert!(package.interface.runtime.module_path.is_none());
    assert!(package.interface.runtime.bin_path.is_none());
    assert_eq!(
        package.capability_grants,
        BTreeSet::from([
            THREADS_CONTROL_CAPABILITY.to_string(),
            THREADS_READ_CAPABILITY.to_string(),
            THREADS_SPAWN_CAPABILITY.to_string(),
        ])
    );
    assert_eq!(package.interface.identity.name, COOLDIS_THREADS_PACKAGE);
    assert_eq!(package.interface.identity.owner.as_deref(), Some("cooldis"));
    assert_eq!(package.interface.identity.version.as_deref(), Some("1.0.0"));
    assert_eq!(package.interface.operations.len(), operations.len());

    for manifest_operation in &package.manifest.operations {
        let interface = package
            .interface
            .operations
            .iter()
            .find(|operation| operation.name == manifest_operation.name)
            .unwrap_or_else(|| panic!("missing interface operation {}", manifest_operation.name));
        assert_eq!(
            interface.required_capabilities,
            manifest_operation
                .required_capabilities
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(manifest_operation.input, WasmOperationValueKind::Json);
        assert_eq!(manifest_operation.output, WasmOperationValueKind::Json);
        assert_eq!(manifest_operation.mode, WasmOperationMode::Sync);
        assert_eq!(manifest_operation.events, WasmOperationEventKind::None);
        validate_json_schema_subset(
            &interface.input_schema,
            &format!("{}.{}.input", COOLDIS_THREADS_PACKAGE, interface.name),
        )
        .unwrap();
        validate_json_schema_subset(
            &interface.output_schema,
            &format!("{}.{}.output", COOLDIS_THREADS_PACKAGE, interface.name),
        )
        .unwrap();

        let command = interface.command.as_ref().expect("command projection");
        assert_eq!(command.name, interface.name);
        assert_eq!(command.stdin.as_deref(), Some("json"));
        assert_eq!(command.stdout.as_deref(), Some("json"));
        assert!(interface.mcp.is_none());

        let manual = interface.manual.as_ref().expect("operation manual");
        assert_eq!(manual.schema_version, TOOL_MANUAL_SCHEMA_VERSION);
        assert_eq!(manual.tool_name, COOLDIS_THREADS_PACKAGE);
        assert_eq!(manual.operation_name, interface.name);
        assert_eq!(manual.input_schema, interface.input_schema);
        assert_eq!(manual.output_schema, interface.output_schema);
        assert_eq!(
            manual.required_capabilities,
            interface.required_capabilities
        );
        assert!(!manual.generated);
        assert_eq!(
            manual
                .exit_status
                .iter()
                .map(|status| status.code)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 126, 127]
        );
    }
}

#[test]
fn thread_spawn_model_input_schema_has_no_dispatch_identity_field() {
    let package = cooldis_threads_kernel_package();
    let operation = package
        .interface
        .operations
        .iter()
        .find(|operation| operation.name == THREAD_SPAWN_OPERATION)
        .unwrap();

    assert_eq!(
        operation.input_schema,
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "task_name": {
                    "type": "string",
                    "description": "Stable task name for the child thread."
                },
                "message": {
                    "type": "string",
                    "description": "Initial user message submitted to the child thread."
                },
                "agent_ref": {
                    "type": "string",
                    "description": "Optional published agent reference for the child thread."
                }
            },
            "required": ["task_name", "message"]
        })
    );
}

#[test]
fn thread_wait_model_schema_uses_task_name_and_has_no_raw_identity_fields() {
    let package = cooldis_threads_kernel_package();
    let operation = package
        .interface
        .operations
        .iter()
        .find(|operation| operation.name == THREAD_WAIT_OPERATION)
        .unwrap();

    assert_eq!(
        operation.input_schema,
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "task_name": {
                    "type": "string",
                    "description": "Parent-scoped task name of the child thread."
                },
                "timeout_ms": {
                    "type": "integer",
                    "description": "Optional timeout in milliseconds."
                }
            },
            "required": ["task_name"]
        })
    );
    assert_eq!(
        operation.output_schema,
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "operation": {
                    "type": "string",
                    "description": "Receipt operation name."
                },
                "task_name": {
                    "type": "string",
                    "description": "Parent-scoped task name of the child thread."
                },
                "status": {
                    "type": "string",
                    "enum": ["starting", "idle", "running", "cancelling", "stopped", "failed"],
                    "description": "Current target thread status after waiting."
                }
            },
            "required": ["operation", "task_name", "status"]
        })
    );
}

#[test]
fn cooldis_schedule_package_declares_three_kernel_operations() {
    let package = cooldis_schedule_kernel_package();
    let operations = package
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![
            MANDATE_START_OPERATION,
            MANDATE_REVOKE_OPERATION,
            MANDATE_LIST_OPERATION,
        ]
    );
    assert_eq!(package.interface.runtime.kind, KERNEL_RUNTIME_KIND);
    assert_eq!(
        package.capability_grants,
        BTreeSet::from([
            SCHEDULE_MANAGE_CAPABILITY.to_string(),
            SCHEDULE_READ_CAPABILITY.to_string(),
        ])
    );
    assert_eq!(package.interface.identity.name, COOLDIS_SCHEDULE_PACKAGE);
    assert_eq!(package.interface.identity.owner.as_deref(), Some("cooldis"));
    assert_eq!(package.interface.identity.version.as_deref(), Some("1.0.0"));
    assert_eq!(package.interface.operations.len(), operations.len());

    for manifest_operation in &package.manifest.operations {
        let interface = package
            .interface
            .operations
            .iter()
            .find(|operation| operation.name == manifest_operation.name)
            .unwrap_or_else(|| panic!("missing interface operation {}", manifest_operation.name));
        assert_eq!(
            interface.required_capabilities,
            manifest_operation
                .required_capabilities
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(manifest_operation.input, WasmOperationValueKind::Json);
        assert_eq!(manifest_operation.output, WasmOperationValueKind::Json);
        assert_eq!(manifest_operation.mode, WasmOperationMode::Sync);
        assert_eq!(manifest_operation.events, WasmOperationEventKind::None);
        validate_json_schema_subset(
            &interface.input_schema,
            &format!("{}.{}.input", COOLDIS_SCHEDULE_PACKAGE, interface.name),
        )
        .unwrap();
        validate_json_schema_subset(
            &interface.output_schema,
            &format!("{}.{}.output", COOLDIS_SCHEDULE_PACKAGE, interface.name),
        )
        .unwrap();
        assert_eq!(
            interface.command.as_ref().unwrap().stdin.as_deref(),
            Some("json")
        );
        assert_eq!(
            interface.command.as_ref().unwrap().stdout.as_deref(),
            Some("json")
        );
        assert_eq!(
            interface.manual.as_ref().unwrap().tool_name,
            COOLDIS_SCHEDULE_PACKAGE
        );
    }
}

#[test]
fn cooldis_process_package_declares_four_kernel_operations() {
    let package = cooldis_process_kernel_package();
    let operations = package
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![
            PROCESS_EXEC_OPERATION,
            PROCESS_POLL_OPERATION,
            PROCESS_WRITE_OPERATION,
            PROCESS_TERMINATE_OPERATION,
        ]
    );
    assert_eq!(package.interface.runtime.kind, KERNEL_RUNTIME_KIND);
    assert!(package.interface.runtime.module_path.is_none());
    assert!(package.interface.runtime.bin_path.is_none());
    assert_eq!(
        package.capability_grants,
        BTreeSet::from([
            PROCESS_CONTROL_CAPABILITY.to_string(),
            PROCESS_READ_CAPABILITY.to_string(),
            PROCESS_SPAWN_CAPABILITY.to_string(),
            PROCESS_WRITE_CAPABILITY.to_string(),
        ])
    );
    assert_eq!(package.interface.identity.name, COOLDIS_PROCESS_PACKAGE);
    assert_eq!(package.interface.identity.owner.as_deref(), Some("cooldis"));
    assert_eq!(package.interface.identity.version.as_deref(), Some("1.0.0"));
    assert_eq!(package.interface.operations.len(), operations.len());

    for manifest_operation in &package.manifest.operations {
        let interface = package
            .interface
            .operations
            .iter()
            .find(|operation| operation.name == manifest_operation.name)
            .unwrap_or_else(|| panic!("missing interface operation {}", manifest_operation.name));
        assert_eq!(
            interface.required_capabilities,
            manifest_operation
                .required_capabilities
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(manifest_operation.input, WasmOperationValueKind::Json);
        assert_eq!(manifest_operation.output, WasmOperationValueKind::Json);
        assert_eq!(manifest_operation.mode, WasmOperationMode::Sync);
        assert_eq!(manifest_operation.events, WasmOperationEventKind::None);
        validate_json_schema_subset(
            &interface.input_schema,
            &format!("{}.{}.input", COOLDIS_PROCESS_PACKAGE, interface.name),
        )
        .unwrap();
        validate_json_schema_subset(
            &interface.output_schema,
            &format!("{}.{}.output", COOLDIS_PROCESS_PACKAGE, interface.name),
        )
        .unwrap();

        let command = interface.command.as_ref().expect("command projection");
        assert_eq!(command.name, interface.name);
        assert_eq!(command.stdin.as_deref(), Some("json"));
        assert_eq!(command.stdout.as_deref(), Some("json"));

        let manual = interface.manual.as_ref().expect("operation manual");
        assert_eq!(manual.schema_version, TOOL_MANUAL_SCHEMA_VERSION);
        assert_eq!(manual.tool_name, COOLDIS_PROCESS_PACKAGE);
        assert_eq!(manual.operation_name, interface.name);
        assert_eq!(manual.input_schema, interface.input_schema);
        assert_eq!(manual.output_schema, interface.output_schema);
        assert_eq!(
            manual.required_capabilities,
            interface.required_capabilities
        );
        assert!(!manual.generated);
    }
}

#[test]
fn cooldis_notify_package_declares_reference_channel_operations() {
    let package = cooldis_notify_kernel_package();
    let operations = package
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![NOTIFY_PREVIEW_OPERATION, CHANNEL_EMIT_OPERATION]
    );
    assert_eq!(package.interface.runtime.kind, KERNEL_RUNTIME_KIND);
    assert!(package.interface.runtime.module_path.is_none());
    assert!(package.interface.runtime.bin_path.is_none());
    assert_eq!(
        package.capability_grants,
        BTreeSet::from([
            CHANNEL_EMIT_CAPABILITY.to_string(),
            NOTIFY_PREVIEW_CAPABILITY.to_string(),
        ])
    );
    assert_eq!(package.interface.identity.name, COOLDIS_NOTIFY_PACKAGE);
    assert_eq!(package.interface.identity.owner.as_deref(), Some("cooldis"));
    assert_eq!(package.interface.identity.version.as_deref(), Some("1.0.0"));
    assert_eq!(package.interface.operations.len(), operations.len());

    for manifest_operation in &package.manifest.operations {
        let interface = package
            .interface
            .operations
            .iter()
            .find(|operation| operation.name == manifest_operation.name)
            .unwrap_or_else(|| panic!("missing interface operation {}", manifest_operation.name));
        assert_eq!(
            interface.required_capabilities,
            manifest_operation
                .required_capabilities
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(manifest_operation.input, WasmOperationValueKind::Json);
        assert_eq!(manifest_operation.output, WasmOperationValueKind::Json);
        assert_eq!(manifest_operation.mode, WasmOperationMode::Sync);
        assert_eq!(manifest_operation.events, WasmOperationEventKind::None);
        validate_json_schema_subset(
            &interface.input_schema,
            &format!("{}.{}.input", COOLDIS_NOTIFY_PACKAGE, interface.name),
        )
        .unwrap();
        validate_json_schema_subset(
            &interface.output_schema,
            &format!("{}.{}.output", COOLDIS_NOTIFY_PACKAGE, interface.name),
        )
        .unwrap();

        let command = interface.command.as_ref().expect("command projection");
        assert_eq!(command.name, interface.name);
        assert_eq!(command.stdin.as_deref(), Some("json"));
        assert_eq!(command.stdout.as_deref(), Some("json"));

        let manual = interface.manual.as_ref().expect("operation manual");
        assert_eq!(manual.schema_version, TOOL_MANUAL_SCHEMA_VERSION);
        assert_eq!(manual.tool_name, COOLDIS_NOTIFY_PACKAGE);
        assert_eq!(manual.operation_name, interface.name);
        assert_eq!(manual.input_schema, interface.input_schema);
        assert_eq!(manual.output_schema, interface.output_schema);
        assert_eq!(
            manual.required_capabilities,
            interface.required_capabilities
        );
        assert!(!manual.generated);
        assert!(manual.warnings.iter().any(|warning| {
            warning.contains("records channel intent") && warning.contains("does not deliver")
        }));
    }
}

#[test]
fn cooldis_threads_package_schemas_accept_operation_receipts() {
    let package = cooldis_threads_kernel_package();

    validate_operation_output(
        &package,
        THREAD_SPAWN_OPERATION,
        json!({
            "operation": "cooldis.thread_spawn",
            "status": "idle",
            "task_name": "worker",
        }),
    );
    validate_operation_output(
        &package,
        THREAD_WAIT_OPERATION,
        json!({
            "operation": "cooldis.thread_wait",
            "status": "idle",
            "task_name": "worker",
        }),
    );
    validate_operation_output(
        &package,
        THREAD_SUBMIT_OPERATION,
        json!({
            "operation": "cooldis.thread_submit",
            "status": "running",
            "task_name": "worker",
        }),
    );
    validate_operation_output(
        &package,
        THREAD_STATUS_OPERATION,
        json!({
            "operation": "cooldis.thread_status",
            "status": "running",
            "task_name": "worker",
        }),
    );
    validate_operation_output(
        &package,
        THREAD_CANCEL_OPERATION,
        json!({
            "operation": "cooldis.thread_cancel",
            "status": "stopped",
            "task_name": "worker",
        }),
    );
}

#[test]
fn cooldis_process_package_schemas_accept_operation_receipts() {
    let package = cooldis_process_kernel_package();
    let process_id = Uuid::now_v7().to_string();

    validate_operation_output(
        &package,
        PROCESS_EXEC_OPERATION,
        json!({
            "operation": "cooldis.process_exec",
            "process_id": process_id,
            "status": "running",
            "backend": "host",
            "label": "sh -lc echo ok",
            "stdout": "",
            "stderr": "",
            "truncated": false,
            "stdout_truncated": false,
            "stderr_truncated": false,
            "event_count": 1
        }),
    );
    validate_operation_output(
        &package,
        PROCESS_POLL_OPERATION,
        json!({
            "operation": "cooldis.process_poll",
            "status": "completed",
            "backend": "host",
            "label": "sh -lc echo ok",
            "exit_code": 0,
            "stdout": "ok\n",
            "stderr": "",
            "truncated": false,
            "stdout_truncated": false,
            "stderr_truncated": false,
            "event_count": 3
        }),
    );
    validate_operation_output(
        &package,
        PROCESS_TERMINATE_OPERATION,
        json!({
            "operation": "cooldis.process_terminate",
            "status": "cancelled",
            "backend": "host",
            "label": "cat",
            "exit_code": 130,
            "stdout": "",
            "stderr": "",
            "truncated": false,
            "stdout_truncated": false,
            "stderr_truncated": false,
            "event_count": 2
        }),
    );
}

#[test]
fn cooldis_notify_package_schemas_accept_reference_receipts() {
    let package = cooldis_notify_kernel_package();

    validate_operation_output(
        &package,
        NOTIFY_PREVIEW_OPERATION,
        json!({
            "operation": "cooldis.notify_preview",
            "status": "recorded",
            "delivery": "not_sent",
            "channel": "email",
            "subject": "Build complete",
            "body": "The build finished successfully.",
            "severity": "info",
            "channel_decision_required": true,
            "reason": "V1 records notification intent; channel-specific delivery adapters are explicit operations."
        }),
    );
    validate_operation_output(
        &package,
        CHANNEL_EMIT_OPERATION,
        json!({
            "operation": "cooldis.channel_emit",
            "status": "recorded",
            "delivery": "not_sent",
            "channel": "slack",
            "message": "Ready for review",
            "thread_id": Uuid::now_v7().to_string(),
            "channel_decision_required": true,
            "reason": "V1 records channel egress intent; channel-specific delivery adapters are explicit operations."
        }),
    );
}

#[test]
fn cooldis_threads_publish_is_idempotent_by_contract_hash() {
    let root = std::env::temp_dir().join(format!("cooldis-kernel-package-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();

    let first = ensure_cooldis_threads_published(Some(&root))
        .unwrap()
        .unwrap();
    let second = ensure_cooldis_threads_published(Some(&root))
        .unwrap()
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(second.name, COOLDIS_THREADS_PACKAGE);
    assert_eq!(
        second.source,
        PublishedOperationSource::Kernel {
            package: COOLDIS_THREADS_PACKAGE.to_string()
        }
    );
    assert_eq!(
        second
            .metadata
            .get(OPERATION_METADATA_RUNTIME_KIND)
            .and_then(Value::as_str),
        Some(KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        second
            .interface
            .as_ref()
            .expect("published interface")
            .runtime
            .kind,
        KERNEL_RUNTIME_KIND
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn cooldis_schedule_publish_is_idempotent_by_contract_hash() {
    let root = std::env::temp_dir().join(format!(
        "cooldis-kernel-schedule-package-{}",
        Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let first = ensure_cooldis_schedule_published(Some(&root))
        .unwrap()
        .unwrap();
    let second = ensure_cooldis_schedule_published(Some(&root))
        .unwrap()
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(second.name, COOLDIS_SCHEDULE_PACKAGE);
    assert_eq!(
        second.source,
        PublishedOperationSource::Kernel {
            package: COOLDIS_SCHEDULE_PACKAGE.to_string()
        }
    );
    assert_eq!(
        second
            .metadata
            .get(OPERATION_METADATA_RUNTIME_KIND)
            .and_then(Value::as_str),
        Some(KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        second
            .interface
            .as_ref()
            .expect("published interface")
            .runtime
            .kind,
        KERNEL_RUNTIME_KIND
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn cooldis_process_publish_is_idempotent_by_contract_hash() {
    let root =
        std::env::temp_dir().join(format!("cooldis-kernel-process-package-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();

    let first = ensure_cooldis_process_published(Some(&root))
        .unwrap()
        .unwrap();
    let second = ensure_cooldis_process_published(Some(&root))
        .unwrap()
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(second.name, COOLDIS_PROCESS_PACKAGE);
    assert_eq!(
        second.source,
        PublishedOperationSource::Kernel {
            package: COOLDIS_PROCESS_PACKAGE.to_string()
        }
    );
    assert_eq!(
        second
            .metadata
            .get(OPERATION_METADATA_RUNTIME_KIND)
            .and_then(Value::as_str),
        Some(KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        second
            .interface
            .as_ref()
            .expect("published interface")
            .runtime
            .kind,
        KERNEL_RUNTIME_KIND
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn cooldis_notify_publish_is_idempotent_by_contract_hash() {
    let root =
        std::env::temp_dir().join(format!("cooldis-kernel-notify-package-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();

    let first = ensure_cooldis_notify_published(Some(&root))
        .unwrap()
        .unwrap();
    let second = ensure_cooldis_notify_published(Some(&root))
        .unwrap()
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(second.name, COOLDIS_NOTIFY_PACKAGE);
    assert_eq!(
        second.source,
        PublishedOperationSource::Kernel {
            package: COOLDIS_NOTIFY_PACKAGE.to_string()
        }
    );
    assert_eq!(
        second
            .metadata
            .get(OPERATION_METADATA_RUNTIME_KIND)
            .and_then(Value::as_str),
        Some(KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        second
            .interface
            .as_ref()
            .expect("published interface")
            .runtime
            .kind,
        KERNEL_RUNTIME_KIND
    );
    let _ = std::fs::remove_dir_all(root);
}

fn validate_operation_output(
    package: &KernelPackageDefinition,
    operation_name: &str,
    value: Value,
) {
    let operation = package
        .interface
        .operations
        .iter()
        .find(|operation| operation.name == operation_name)
        .unwrap_or_else(|| panic!("missing operation {operation_name}"));
    validate_json_value_against_schema(
        &operation.output_schema,
        &value,
        &format!("{}.{}.output", COOLDIS_THREADS_PACKAGE, operation.name),
    )
    .unwrap();
}
