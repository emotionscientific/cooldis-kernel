fn operation(
    name: &str,
    input: crate::WasmOperationValueKind,
    output: crate::WasmOperationValueKind,
) -> crate::WasmOperationDefinition {
    crate::WasmOperationDefinition {
        id: 1,
        name: name.to_string(),
        input,
        output,
        events: crate::WasmOperationEventKind::None,
        mode: crate::WasmOperationMode::Sync,
        required_capabilities: Vec::new(),
    }
}

#[test]
fn abi_contracts_allow_compatible_producer_consumer_ports() {
    let producer = crate::AbiOperationContract::from_operation(
        "producer",
        &operation(
            "emit_bytes",
            crate::WasmOperationValueKind::Json,
            crate::WasmOperationValueKind::Bytes,
        ),
    );
    let consumer = crate::AbiOperationContract::from_operation(
        "consumer",
        &operation(
            "read_bytes",
            crate::WasmOperationValueKind::Bytes,
            crate::WasmOperationValueKind::Json,
        ),
    );

    assert!(producer.output_can_feed(&consumer));
}

#[test]
fn abi_contracts_reject_incompatible_producer_consumer_ports() {
    let producer = crate::AbiOperationContract::from_operation(
        "producer",
        &operation(
            "emit_json",
            crate::WasmOperationValueKind::Bytes,
            crate::WasmOperationValueKind::Json,
        ),
    );
    let consumer = crate::AbiOperationContract::from_operation(
        "consumer",
        &operation(
            "read_text",
            crate::WasmOperationValueKind::Text,
            crate::WasmOperationValueKind::Bytes,
        ),
    );

    assert!(!producer.output_can_feed(&consumer));
}

#[test]
fn durable_output_requires_effect_receipt_not_hidden_sink() {
    let mut contract = crate::AbiOperationContract::from_operation(
        "writer",
        &operation(
            "write_file",
            crate::WasmOperationValueKind::Json,
            crate::WasmOperationValueKind::Json,
        ),
    );
    contract.effect_ports.push(crate::AbiEffectPort {
        name: "write_output".to_string(),
        kind: crate::AbiEffectKind::VfsWrite {
            mode: crate::AbiVfsWriteMode::WriteNew,
        },
        binding: crate::AbiEffectBinding::HostAllocatedPath,
        required: true,
    });

    assert!(!contract.has_hidden_durable_sink());
    let receipt = crate::AbiEffectReceipt {
        effect_port: "write_output".to_string(),
        kind: crate::AbiEffectReceiptKind::VfsWrite {
            path: "/workspace/out.txt".to_string(),
            bytes: Some(5),
            sha256: None,
            media_type: Some("text/plain".to_string()),
        },
        invocation_id: Some("invocation-1".to_string()),
    };
    assert_eq!(receipt.effect_port, "write_output");
}

#[test]
fn invocation_context_separates_caller_execution_and_attachment_identity() {
    let context = crate::InvocationContext::new(crate::ExecutionPrincipal::system(
        "shared-credential-provisioner",
    ))
    .with_caller(crate::Principal::user("user-123"))
    .with_grant("net.http:POST:https://api.example.invalid")
    .with_attachment_binding(
        crate::AttachmentBinding::new(
            "search-api",
            "secret:EXAMPLE_API_KEY",
            crate::AttachmentIdentity::Secret {
                name: "search-api-key".to_string(),
            },
        )
        .with_metadata("provider", "search"),
    )
    .with_audit_metadata("request_id", "req-123");

    assert_eq!(context.caller, Some(crate::Principal::user("user-123")));
    assert_eq!(
        context.execution,
        crate::ExecutionPrincipal::system("shared-credential-provisioner")
    );
    assert!(
        context
            .grant_set()
            .contains("net.http:POST:https://api.example.invalid")
    );
    assert_eq!(context.attachment_bindings[0].handle, "search-api");
    assert_eq!(
        context.attachment_bindings[0].capability.as_str(),
        "secret:EXAMPLE_API_KEY"
    );
    assert_eq!(
        context.attachment_bindings[0].metadata["provider"],
        "search"
    );
    assert_eq!(context.audit_metadata["request_id"], "req-123");
}

fn write_contract(
    mode: crate::AbiVfsWriteMode,
    binding: crate::AbiEffectBinding,
) -> crate::AbiOperationContract {
    let mut contract = crate::AbiOperationContract::from_operation(
        "writer",
        &operation(
            "write_file",
            crate::WasmOperationValueKind::Json,
            crate::WasmOperationValueKind::Json,
        ),
    );
    contract.effect_ports.push(crate::AbiEffectPort {
        name: "write_output".to_string(),
        kind: crate::AbiEffectKind::VfsWrite { mode },
        binding,
        required: true,
    });
    contract
}

fn write_claim(
    mode: crate::AbiVfsWriteMode,
    binding: crate::AbiEffectBinding,
) -> crate::AbiEffectClaim {
    crate::AbiEffectClaim {
        effect_port: "write_output".to_string(),
        kind: crate::AbiEffectKind::VfsWrite { mode },
        binding,
    }
}

#[test]
fn write_new_claim_requires_declared_effect_port() {
    let read_only = crate::AbiOperationContract::from_operation(
        "reader",
        &operation(
            "cat",
            crate::WasmOperationValueKind::Text,
            crate::WasmOperationValueKind::Text,
        ),
    );
    let claim = write_claim(
        crate::AbiVfsWriteMode::WriteNew,
        crate::AbiEffectBinding::CallerBoundPath {
            path: Some("/workspace/new.txt".to_string()),
        },
    );

    assert!(!read_only.allows_effect_claim(&claim));
    let writable = write_contract(
        crate::AbiVfsWriteMode::WriteNew,
        crate::AbiEffectBinding::CallerBoundPath { path: None },
    );
    assert!(writable.allows_effect_claim(&claim));
}

#[test]
fn replace_claim_can_be_bound_to_exact_caller_path() {
    let contract = write_contract(
        crate::AbiVfsWriteMode::Replace,
        crate::AbiEffectBinding::CallerBoundPath {
            path: Some("/workspace/existing.txt".to_string()),
        },
    );

    assert!(contract.allows_effect_claim(&write_claim(
        crate::AbiVfsWriteMode::Replace,
        crate::AbiEffectBinding::CallerBoundPath {
            path: Some("/workspace/existing.txt".to_string()),
        },
    )));
    assert!(!contract.allows_effect_claim(&write_claim(
        crate::AbiVfsWriteMode::Replace,
        crate::AbiEffectBinding::CallerBoundPath {
            path: Some("/workspace/other.txt".to_string()),
        },
    )));
}

#[test]
fn append_claim_preserves_operation_selected_scope() {
    let contract = write_contract(
        crate::AbiVfsWriteMode::Append,
        crate::AbiEffectBinding::OperationSelectedPath {
            scope: "/workspace/logs".to_string(),
        },
    );

    assert!(contract.allows_effect_claim(&write_claim(
        crate::AbiVfsWriteMode::Append,
        crate::AbiEffectBinding::OperationSelectedPath {
            scope: "/workspace/logs".to_string(),
        },
    )));
    assert!(!contract.allows_effect_claim(&write_claim(
        crate::AbiVfsWriteMode::Append,
        crate::AbiEffectBinding::OperationSelectedPath {
            scope: "/workspace".to_string(),
        },
    )));
}

#[test]
fn scratch_claim_uses_host_allocated_path() {
    let contract = write_contract(
        crate::AbiVfsWriteMode::Scratch,
        crate::AbiEffectBinding::HostAllocatedPath,
    );

    assert!(contract.allows_effect_claim(&write_claim(
        crate::AbiVfsWriteMode::Scratch,
        crate::AbiEffectBinding::HostAllocatedPath,
    )));
    assert!(!contract.allows_effect_claim(&write_claim(
        crate::AbiVfsWriteMode::Scratch,
        crate::AbiEffectBinding::CallerBoundPath {
            path: Some("/workspace/scratch.txt".to_string()),
        },
    )));
}

#[test]
fn coupling_invocation_and_discharge_use_versioned_abi_tags() {
    let invocation = crate::CouplingInvocation::new(
        crate::CouplingInvocationEvent {
            id: "event-1".to_string(),
            stream_id: "thread:abc".to_string(),
            sequence: 7,
            kind: "turn.completed".to_string(),
            origin: "witnessed".to_string(),
            payload: serde_json::json!({"turn_id": "t1"}),
        },
        Vec::new(),
        serde_json::json!({"every": 3}),
        crate::CouplingInvocationMeta {
            coupling_id: "org.example.counter".to_string(),
            thread_id: "abc".to_string(),
            depth: 0,
        },
    );

    let value = serde_json::to_value(&invocation).unwrap();
    assert_eq!(value["abi"], crate::COUPLING_INVOCATION_ABI);
    assert_eq!(
        value["invocation_meta"]["coupling_id"],
        "org.example.counter"
    );

    let discharge = crate::CouplingDischarge::new(vec![crate::CouplingDischargeEvent {
        stream: "derived:counter".to_string(),
        kind: "placement.decision".to_string(),
        payload: serde_json::json!({"count": 3}),
        provenance: Some(serde_json::json!({"ignored": true})),
    }]);

    let value = serde_json::to_value(&discharge).unwrap();
    assert_eq!(value["abi"], crate::COUPLING_DISCHARGE_ABI);
    assert_eq!(value["events"][0]["kind"], "placement.decision");
}
