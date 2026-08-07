#[test]
fn verlet_threads_package_declares_five_kernel_operations() {
    let package = crate::operations::kernel_packages::verlet_threads_kernel_package();
    let operations = package
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION,
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
            crate::operations::kernel_packages::THREAD_CANCEL_OPERATION,
        ]
    );
    assert_eq!(
        package.interface.runtime.kind,
        crate::operations::kernel_packages::KERNEL_RUNTIME_KIND
    );
    assert!(package.interface.runtime.module_path.is_none());
    assert!(package.interface.runtime.bin_path.is_none());
    assert_eq!(
        package.capability_grants,
        std::collections::BTreeSet::from([
            crate::operations::kernel_packages::THREADS_CONTROL_CAPABILITY.to_string(),
            crate::operations::kernel_packages::THREADS_READ_CAPABILITY.to_string(),
            crate::operations::kernel_packages::THREADS_SPAWN_CAPABILITY.to_string(),
        ])
    );
    assert_eq!(
        package.interface.identity.name,
        crate::operations::kernel_packages::VERLET_THREADS_PACKAGE
    );
    assert_eq!(package.interface.identity.owner.as_deref(), Some("verlet"));
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
                .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(
            manifest_operation.input,
            verlet_abi::WasmOperationValueKind::Json
        );
        assert_eq!(
            manifest_operation.output,
            verlet_abi::WasmOperationValueKind::Json
        );
        assert_eq!(manifest_operation.mode, verlet_abi::WasmOperationMode::Sync);
        assert_eq!(
            manifest_operation.events,
            verlet_abi::WasmOperationEventKind::None
        );
        verlet_runtime_contracts::schema::validate_json_schema_subset(
            &interface.input_schema,
            &format!(
                "{}.{}.input",
                crate::operations::kernel_packages::VERLET_THREADS_PACKAGE,
                interface.name
            ),
        )
        .unwrap();
        verlet_runtime_contracts::schema::validate_json_schema_subset(
            &interface.output_schema,
            &format!(
                "{}.{}.output",
                crate::operations::kernel_packages::VERLET_THREADS_PACKAGE,
                interface.name
            ),
        )
        .unwrap();

        let command = interface.command.as_ref().expect("command projection");
        assert_eq!(command.name, interface.name);
        assert_eq!(command.stdin.as_deref(), Some("json"));
        assert_eq!(command.stdout.as_deref(), Some("json"));
        assert!(interface.mcp.is_none());

        let manual = interface.manual.as_ref().expect("operation manual");
        assert_eq!(
            manual.schema_version,
            verlet_operations::tool_package::TOOL_MANUAL_SCHEMA_VERSION
        );
        assert_eq!(
            manual.tool_name,
            crate::operations::kernel_packages::VERLET_THREADS_PACKAGE
        );
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
    let package = crate::operations::kernel_packages::verlet_threads_kernel_package();
    let operation = package
        .interface
        .operations
        .iter()
        .find(|operation| {
            operation.name == crate::operations::kernel_packages::THREAD_SPAWN_OPERATION
        })
        .unwrap();

    assert_eq!(
        operation.input_schema,
        serde_json::json!({
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
    let package = crate::operations::kernel_packages::verlet_threads_kernel_package();
    let operation = package
        .interface
        .operations
        .iter()
        .find(|operation| {
            operation.name == crate::operations::kernel_packages::THREAD_WAIT_OPERATION
        })
        .unwrap();

    assert_eq!(
        operation.input_schema,
        serde_json::json!({
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
        serde_json::json!({
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
fn verlet_schedule_package_declares_three_kernel_operations() {
    let package = crate::operations::kernel_packages::verlet_schedule_kernel_package();
    let operations = package
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![
            crate::operations::kernel_packages::MANDATE_START_OPERATION,
            crate::operations::kernel_packages::MANDATE_REVOKE_OPERATION,
            crate::operations::kernel_packages::MANDATE_LIST_OPERATION,
        ]
    );
    assert_eq!(
        package.interface.runtime.kind,
        crate::operations::kernel_packages::KERNEL_RUNTIME_KIND
    );
    assert_eq!(
        package.capability_grants,
        std::collections::BTreeSet::from([
            crate::operations::kernel_packages::SCHEDULE_MANAGE_CAPABILITY.to_string(),
            crate::operations::kernel_packages::SCHEDULE_READ_CAPABILITY.to_string(),
        ])
    );
    assert_eq!(
        package.interface.identity.name,
        crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE
    );
    assert_eq!(package.interface.identity.owner.as_deref(), Some("verlet"));
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
                .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(
            manifest_operation.input,
            verlet_abi::WasmOperationValueKind::Json
        );
        assert_eq!(
            manifest_operation.output,
            verlet_abi::WasmOperationValueKind::Json
        );
        assert_eq!(manifest_operation.mode, verlet_abi::WasmOperationMode::Sync);
        assert_eq!(
            manifest_operation.events,
            verlet_abi::WasmOperationEventKind::None
        );
        verlet_runtime_contracts::schema::validate_json_schema_subset(
            &interface.input_schema,
            &format!(
                "{}.{}.input",
                crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE,
                interface.name
            ),
        )
        .unwrap();
        verlet_runtime_contracts::schema::validate_json_schema_subset(
            &interface.output_schema,
            &format!(
                "{}.{}.output",
                crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE,
                interface.name
            ),
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
            crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE
        );
    }
}

#[test]
fn verlet_process_package_declares_four_kernel_operations() {
    let package = crate::operations::kernel_packages::verlet_process_kernel_package();
    let operations = package
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![
            crate::operations::kernel_packages::PROCESS_EXEC_OPERATION,
            crate::operations::kernel_packages::PROCESS_POLL_OPERATION,
            crate::operations::kernel_packages::PROCESS_WRITE_OPERATION,
            crate::operations::kernel_packages::PROCESS_TERMINATE_OPERATION,
        ]
    );
    assert_eq!(
        package.interface.runtime.kind,
        crate::operations::kernel_packages::KERNEL_RUNTIME_KIND
    );
    assert!(package.interface.runtime.module_path.is_none());
    assert!(package.interface.runtime.bin_path.is_none());
    assert_eq!(
        package.capability_grants,
        std::collections::BTreeSet::from([
            crate::operations::kernel_packages::PROCESS_CONTROL_CAPABILITY.to_string(),
            crate::operations::kernel_packages::PROCESS_READ_CAPABILITY.to_string(),
            crate::operations::kernel_packages::PROCESS_SPAWN_CAPABILITY.to_string(),
            crate::operations::kernel_packages::PROCESS_WRITE_CAPABILITY.to_string(),
        ])
    );
    assert_eq!(
        package.interface.identity.name,
        crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE
    );
    assert_eq!(package.interface.identity.owner.as_deref(), Some("verlet"));
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
                .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(
            manifest_operation.input,
            verlet_abi::WasmOperationValueKind::Json
        );
        assert_eq!(
            manifest_operation.output,
            verlet_abi::WasmOperationValueKind::Json
        );
        assert_eq!(manifest_operation.mode, verlet_abi::WasmOperationMode::Sync);
        assert_eq!(
            manifest_operation.events,
            verlet_abi::WasmOperationEventKind::None
        );
        verlet_runtime_contracts::schema::validate_json_schema_subset(
            &interface.input_schema,
            &format!(
                "{}.{}.input",
                crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE,
                interface.name
            ),
        )
        .unwrap();
        verlet_runtime_contracts::schema::validate_json_schema_subset(
            &interface.output_schema,
            &format!(
                "{}.{}.output",
                crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE,
                interface.name
            ),
        )
        .unwrap();

        let command = interface.command.as_ref().expect("command projection");
        assert_eq!(command.name, interface.name);
        assert_eq!(command.stdin.as_deref(), Some("json"));
        assert_eq!(command.stdout.as_deref(), Some("json"));

        let manual = interface.manual.as_ref().expect("operation manual");
        assert_eq!(
            manual.schema_version,
            verlet_operations::tool_package::TOOL_MANUAL_SCHEMA_VERSION
        );
        assert_eq!(
            manual.tool_name,
            crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE
        );
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
fn verlet_notify_package_declares_reference_channel_operations() {
    let package = crate::operations::kernel_packages::verlet_notify_kernel_package();
    let operations = package
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        operations,
        vec![
            crate::operations::kernel_packages::NOTIFY_PREVIEW_OPERATION,
            crate::operations::kernel_packages::CHANNEL_EMIT_OPERATION
        ]
    );
    assert_eq!(
        package.interface.runtime.kind,
        crate::operations::kernel_packages::KERNEL_RUNTIME_KIND
    );
    assert!(package.interface.runtime.module_path.is_none());
    assert!(package.interface.runtime.bin_path.is_none());
    assert_eq!(
        package.capability_grants,
        std::collections::BTreeSet::from([
            crate::operations::kernel_packages::CHANNEL_EMIT_CAPABILITY.to_string(),
            crate::operations::kernel_packages::NOTIFY_PREVIEW_CAPABILITY.to_string(),
        ])
    );
    assert_eq!(
        package.interface.identity.name,
        crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE
    );
    assert_eq!(package.interface.identity.owner.as_deref(), Some("verlet"));
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
                .collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(
            manifest_operation.input,
            verlet_abi::WasmOperationValueKind::Json
        );
        assert_eq!(
            manifest_operation.output,
            verlet_abi::WasmOperationValueKind::Json
        );
        assert_eq!(manifest_operation.mode, verlet_abi::WasmOperationMode::Sync);
        assert_eq!(
            manifest_operation.events,
            verlet_abi::WasmOperationEventKind::None
        );
        verlet_runtime_contracts::schema::validate_json_schema_subset(
            &interface.input_schema,
            &format!(
                "{}.{}.input",
                crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE,
                interface.name
            ),
        )
        .unwrap();
        verlet_runtime_contracts::schema::validate_json_schema_subset(
            &interface.output_schema,
            &format!(
                "{}.{}.output",
                crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE,
                interface.name
            ),
        )
        .unwrap();

        let command = interface.command.as_ref().expect("command projection");
        assert_eq!(command.name, interface.name);
        assert_eq!(command.stdin.as_deref(), Some("json"));
        assert_eq!(command.stdout.as_deref(), Some("json"));

        let manual = interface.manual.as_ref().expect("operation manual");
        assert_eq!(
            manual.schema_version,
            verlet_operations::tool_package::TOOL_MANUAL_SCHEMA_VERSION
        );
        assert_eq!(
            manual.tool_name,
            crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE
        );
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
fn verlet_threads_package_schemas_accept_operation_receipts() {
    let package = crate::operations::kernel_packages::verlet_threads_kernel_package();

    validate_operation_output(
        &package,
        crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
        serde_json::json!({
            "operation": "cooldis.thread_spawn",
            "status": "idle",
            "task_name": "worker",
        }),
    );
    validate_operation_output(
        &package,
        crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
        serde_json::json!({
            "operation": "cooldis.thread_wait",
            "status": "idle",
            "task_name": "worker",
        }),
    );
    validate_operation_output(
        &package,
        crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION,
        serde_json::json!({
            "operation": "cooldis.thread_submit",
            "status": "running",
            "task_name": "worker",
        }),
    );
    validate_operation_output(
        &package,
        crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
        serde_json::json!({
            "operation": "cooldis.thread_status",
            "status": "running",
            "task_name": "worker",
        }),
    );
    validate_operation_output(
        &package,
        crate::operations::kernel_packages::THREAD_CANCEL_OPERATION,
        serde_json::json!({
            "operation": "cooldis.thread_cancel",
            "status": "stopped",
            "task_name": "worker",
        }),
    );
}

#[test]
fn verlet_process_package_schemas_accept_operation_receipts() {
    let package = crate::operations::kernel_packages::verlet_process_kernel_package();
    let process_id = uuid::Uuid::now_v7().to_string();

    validate_operation_output(
        &package,
        crate::operations::kernel_packages::PROCESS_EXEC_OPERATION,
        serde_json::json!({
            "operation": "cooldis.process_exec",
            "dispatch_id": "process-dispatch-1",
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
        crate::operations::kernel_packages::PROCESS_POLL_OPERATION,
        serde_json::json!({
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
        crate::operations::kernel_packages::PROCESS_TERMINATE_OPERATION,
        serde_json::json!({
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
fn verlet_notify_package_schemas_accept_reference_receipts() {
    let package = crate::operations::kernel_packages::verlet_notify_kernel_package();

    validate_operation_output(
        &package,
        crate::operations::kernel_packages::NOTIFY_PREVIEW_OPERATION,
        serde_json::json!({
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
        crate::operations::kernel_packages::CHANNEL_EMIT_OPERATION,
        serde_json::json!({
            "operation": "cooldis.channel_emit",
            "status": "recorded",
            "delivery": "not_sent",
            "channel": "slack",
            "message": "Ready for review",
            "thread_id": uuid::Uuid::now_v7().to_string(),
            "channel_decision_required": true,
            "reason": "V1 records channel egress intent; channel-specific delivery adapters are explicit operations."
        }),
    );
}

#[test]
fn verlet_threads_publish_is_idempotent_by_contract_hash() {
    let root = std::env::temp_dir().join(format!("verlet-kernel-package-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();

    let first = crate::operations::kernel_packages::ensure_verlet_threads_published(Some(&root))
        .unwrap()
        .unwrap();
    let second = crate::operations::kernel_packages::ensure_verlet_threads_published(Some(&root))
        .unwrap()
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(
        second.name,
        crate::operations::kernel_packages::VERLET_THREADS_PACKAGE
    );
    assert_eq!(
        second.source,
        verlet_operations::operation_store::PublishedOperationSource::Kernel {
            package: crate::operations::kernel_packages::VERLET_THREADS_PACKAGE.to_string()
        }
    );
    assert_eq!(
        second
            .metadata
            .get(crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND)
            .and_then(serde_json::Value::as_str),
        Some(crate::operations::kernel_packages::KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        second
            .interface
            .as_ref()
            .expect("published interface")
            .runtime
            .kind,
        crate::operations::kernel_packages::KERNEL_RUNTIME_KIND
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verlet_schedule_publish_is_idempotent_by_contract_hash() {
    let root = std::env::temp_dir().join(format!(
        "verlet-kernel-schedule-package-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let first = crate::operations::kernel_packages::ensure_verlet_schedule_published(Some(&root))
        .unwrap()
        .unwrap();
    let second = crate::operations::kernel_packages::ensure_verlet_schedule_published(Some(&root))
        .unwrap()
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(
        second.name,
        crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE
    );
    assert_eq!(
        second.source,
        verlet_operations::operation_store::PublishedOperationSource::Kernel {
            package: crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE.to_string()
        }
    );
    assert_eq!(
        second
            .metadata
            .get(crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND)
            .and_then(serde_json::Value::as_str),
        Some(crate::operations::kernel_packages::KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        second
            .interface
            .as_ref()
            .expect("published interface")
            .runtime
            .kind,
        crate::operations::kernel_packages::KERNEL_RUNTIME_KIND
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verlet_process_publish_is_idempotent_by_contract_hash() {
    let root = std::env::temp_dir().join(format!(
        "verlet-kernel-process-package-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let first = crate::operations::kernel_packages::ensure_verlet_process_published(Some(&root))
        .unwrap()
        .unwrap();
    let second = crate::operations::kernel_packages::ensure_verlet_process_published(Some(&root))
        .unwrap()
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(
        second.name,
        crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE
    );
    assert_eq!(
        second.source,
        verlet_operations::operation_store::PublishedOperationSource::Kernel {
            package: crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE.to_string()
        }
    );
    assert_eq!(
        second
            .metadata
            .get(crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND)
            .and_then(serde_json::Value::as_str),
        Some(crate::operations::kernel_packages::KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        second
            .interface
            .as_ref()
            .expect("published interface")
            .runtime
            .kind,
        crate::operations::kernel_packages::KERNEL_RUNTIME_KIND
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn verlet_notify_publish_is_idempotent_by_contract_hash() {
    let root = std::env::temp_dir().join(format!(
        "verlet-kernel-notify-package-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root).unwrap();

    let first = crate::operations::kernel_packages::ensure_verlet_notify_published(Some(&root))
        .unwrap()
        .unwrap();
    let second = crate::operations::kernel_packages::ensure_verlet_notify_published(Some(&root))
        .unwrap()
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(
        second.name,
        crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE
    );
    assert_eq!(
        second.source,
        verlet_operations::operation_store::PublishedOperationSource::Kernel {
            package: crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE.to_string()
        }
    );
    assert_eq!(
        second
            .metadata
            .get(crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND)
            .and_then(serde_json::Value::as_str),
        Some(crate::operations::kernel_packages::KERNEL_RUNTIME_KIND)
    );
    assert_eq!(
        second
            .interface
            .as_ref()
            .expect("published interface")
            .runtime
            .kind,
        crate::operations::kernel_packages::KERNEL_RUNTIME_KIND
    );
    let _ = std::fs::remove_dir_all(root);
}

fn validate_operation_output(
    package: &crate::operations::kernel_packages::KernelPackageDefinition,
    operation_name: &str,
    value: serde_json::Value,
) {
    let operation = package
        .interface
        .operations
        .iter()
        .find(|operation| operation.name == operation_name)
        .unwrap_or_else(|| panic!("missing operation {operation_name}"));
    verlet_runtime_contracts::schema::validate_json_value_against_schema(
        &operation.output_schema,
        &value,
        &format!(
            "{}.{}.output",
            crate::operations::kernel_packages::VERLET_THREADS_PACKAGE,
            operation.name
        ),
    )
    .unwrap();
}
