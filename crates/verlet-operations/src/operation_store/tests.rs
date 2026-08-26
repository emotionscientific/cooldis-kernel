fn temp_dir(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("verlet-{label}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn test_manifest() -> verlet_abi::WasmOperationManifest {
    test_manifest_with_operation("cat")
}

fn test_manifest_with_operation(operation_name: &str) -> verlet_abi::WasmOperationManifest {
    verlet_abi::WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: vec![verlet_abi::WasmOperationDefinition {
            id: 1,
            name: operation_name.to_string(),
            input: verlet_abi::WasmOperationValueKind::Text,
            output: verlet_abi::WasmOperationValueKind::Text,
            events: Default::default(),
            mode: Default::default(),
            required_capabilities: vec![],
        }],
    }
}

fn test_record(
    name: &str,
    hash: &str,
    operation_name: &str,
) -> crate::operation_store::PublishedOperationRecord {
    let manifest = test_manifest_with_operation(operation_name);
    let registered = crate::RegisteredOperation {
        name: name.to_string(),
        manifest: manifest.clone(),
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
    };
    crate::operation_store::PublishedOperationRecord {
        schema_version: crate::operation_store::RECORD_SCHEMA_VERSION,
        name: name.to_string(),
        active_artifact_hash: hash.to_string(),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
        source: crate::operation_store::PublishedOperationSource::Wasm {
            bin_path: std::path::PathBuf::from(format!("{name}.wasm")),
        },
        build: crate::operation_store::PublishedOperationBuild {
            artifact_path: std::path::PathBuf::from(format!("{name}.wasm")),
            published_at_ms: 1,
        },
    }
}

#[test]
fn blob_store_put_get_and_detects_corruption() {
    let root = temp_dir("blob-store");
    let store = crate::operation_store::OperationBlobStore::new(root.join("blobs"));
    let hash = store.put(b"hello wasm-ish bytes").unwrap();

    assert_eq!(
        store.get(&hash).unwrap(),
        Some(b"hello wasm-ish bytes".to_vec())
    );
    assert_eq!(store.put(b"hello wasm-ish bytes").unwrap(), hash);
    assert_eq!(store.get(&"0".repeat(64)).unwrap(), None);

    let path = store.artifact_path(&hash).unwrap();
    std::fs::write(path, b"corrupt").unwrap();
    let err = store.get(&hash).unwrap_err().to_string();
    assert!(err.contains("hash mismatch"), "{err}");
}

#[test]
fn published_record_round_trips_and_validates_projection() {
    let manifest = test_manifest();
    let registered = crate::RegisteredOperation {
        name: "tailcat".to_string(),
        manifest: manifest.clone(),
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
    };
    let record = crate::operation_store::PublishedOperationRecord {
        schema_version: crate::operation_store::RECORD_SCHEMA_VERSION,
        name: "tailcat".to_string(),
        active_artifact_hash: "a".repeat(64),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
        source: crate::operation_store::PublishedOperationSource::Wasm {
            bin_path: std::path::PathBuf::from("tool.wasm"),
        },
        build: crate::operation_store::PublishedOperationBuild {
            artifact_path: std::path::PathBuf::from("tool.wasm"),
            published_at_ms: 1,
        },
    };

    record.validate().unwrap();
    let json = serde_json::to_vec(&record).unwrap();
    let decoded: crate::operation_store::PublishedOperationRecord =
        serde_json::from_slice(&json).unwrap();
    assert_eq!(decoded, record);
}

#[test]
fn pre_overlay_record_with_generated_mcp_name_still_validates() {
    let manifest = test_manifest();
    let registered = crate::RegisteredOperation {
        name: "tailcat".to_string(),
        manifest: manifest.clone(),
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
    };
    let generated_projections = registered.projections();
    let generated_mcp_name = generated_projections.operations[0].mcp.tool_name.clone();
    let record = crate::operation_store::PublishedOperationRecord {
        schema_version: crate::operation_store::RECORD_SCHEMA_VERSION,
        name: "tailcat".to_string(),
        active_artifact_hash: "9".repeat(64),
        manifest,
        projections: generated_projections,
        interface: Some(crate::tool_package::ToolInterfaceContract {
            schema_version: crate::tool_package::TOOL_PACKAGE_SCHEMA_VERSION,
            identity: crate::tool_package::ToolPackageIdentity {
                name: "tailcat".to_string(),
                version: Some("0.1.0".to_string()),
                description: Some("Pre-overlay fixture record.".to_string()),
                owner: None,
            },
            runtime: crate::tool_package::ToolRuntimeContract {
                kind: "wasm32-unknown-unknown".to_string(),
                state: Some("stateless".to_string()),
                module_path: None,
                bin_path: Some(std::path::PathBuf::from("tool.wasm")),
                release: None,
                timeout_ms: None,
                max_input_bytes: None,
                max_output_bytes: None,
            },
            operations: vec![crate::tool_package::ToolOperationInterface {
                name: "cat".to_string(),
                description: None,
                input_schema: serde_json::json!({"type": "string"}),
                output_schema: serde_json::json!({"type": "string"}),
                required_capabilities: std::collections::BTreeSet::new(),
                surface: None,
                command: None,
                mcp: Some(crate::tool_package::ToolMcpContract {
                    tool_name: generated_mcp_name,
                    description: None,
                }),
                manual: None,
            }],
            fixtures: Vec::new(),
        }),
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
        source: crate::operation_store::PublishedOperationSource::Wasm {
            bin_path: std::path::PathBuf::from("tool.wasm"),
        },
        build: crate::operation_store::PublishedOperationBuild {
            artifact_path: std::path::PathBuf::from("tool.wasm"),
            published_at_ms: 1,
        },
    };

    let fixture_json = serde_json::to_vec(&record).unwrap();
    let loaded: crate::operation_store::PublishedOperationRecord =
        serde_json::from_slice(&fixture_json).unwrap();

    loaded.validate().unwrap();
}

#[test]
fn legacy_wasm_source_deserializes_after_import_source_is_added() {
    let source: crate::operation_store::PublishedOperationSource =
        serde_json::from_value(serde_json::json!({
            "kind": "wasm",
            "bin_path": "tool.wasm"
        }))
        .unwrap();
    assert_eq!(
        source,
        crate::operation_store::PublishedOperationSource::Wasm {
            bin_path: std::path::PathBuf::from("tool.wasm")
        }
    );
}

#[test]
fn imported_record_rejects_invalid_spec_provenance_hash() {
    let mut record = test_record("catalog", &"a".repeat(64), "search");
    record.source = crate::operation_store::PublishedOperationSource::Import {
        manifest_path: std::path::PathBuf::from("catalog.import.toml"),
        spec_sha256: "not-a-sha256".to_string(),
    };

    let err = record.validate().unwrap_err().to_string();
    assert!(err.contains("import spec sha256"), "{err}");
}

#[test]
fn record_validation_rejects_stale_projection() {
    let manifest = test_manifest();
    let registered = crate::RegisteredOperation {
        name: "tailcat".to_string(),
        manifest: manifest.clone(),
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
    };
    let mut record = crate::operation_store::PublishedOperationRecord {
        schema_version: crate::operation_store::RECORD_SCHEMA_VERSION,
        name: "tailcat".to_string(),
        active_artifact_hash: "b".repeat(64),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
        source: crate::operation_store::PublishedOperationSource::Wasm {
            bin_path: std::path::PathBuf::from("tool.wasm"),
        },
        build: crate::operation_store::PublishedOperationBuild {
            artifact_path: std::path::PathBuf::from("tool.wasm"),
            published_at_ms: 1,
        },
    };
    record.projections.registered_name = "other".to_string();

    let err = record.validate().unwrap_err().to_string();
    assert!(err.contains("projections are stale"), "{err}");
}

#[test]
fn record_validation_rejects_manifest_without_operations() {
    let manifest = verlet_abi::WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: vec![],
    };
    let registered = crate::RegisteredOperation {
        name: "empty".to_string(),
        manifest: manifest.clone(),
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
    };
    let record = crate::operation_store::PublishedOperationRecord {
        schema_version: crate::operation_store::RECORD_SCHEMA_VERSION,
        name: "empty".to_string(),
        active_artifact_hash: "c".repeat(64),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
        source: crate::operation_store::PublishedOperationSource::Wasm {
            bin_path: std::path::PathBuf::from("tool.wasm"),
        },
        build: crate::operation_store::PublishedOperationBuild {
            artifact_path: std::path::PathBuf::from("tool.wasm"),
            published_at_ms: 1,
        },
    };

    let err = record.validate().unwrap_err().to_string();
    assert!(err.contains("has no operations"), "{err}");
}

#[test]
fn capsule_binding_scope_json_uses_camel_case_and_accepts_legacy_snake_case() {
    let scope = crate::operation_store::CapsuleBindingScope::thread("tenant-a", "thread-a");
    let value = serde_json::to_value(&scope).unwrap();
    assert_eq!(value["kind"].as_str(), Some("thread"));
    assert_eq!(value["tenantId"].as_str(), Some("tenant-a"));
    assert_eq!(value["threadId"].as_str(), Some("thread-a"));
    assert!(value.get("tenant_id").is_none());
    assert!(value.get("thread_id").is_none());

    let legacy: crate::operation_store::CapsuleBindingScope =
        serde_json::from_value(serde_json::json!({
            "kind": "thread",
            "tenant_id": "tenant-a",
            "thread_id": "thread-a"
        }))
        .unwrap();
    assert_eq!(legacy, scope);
}

#[test]
fn version_records_preserve_old_artifacts_after_active_record_moves() {
    let root = temp_dir("operation-versions");
    let registry = crate::operation_store::LocalOperationRegistry::new(&root);
    let old_hash = "d".repeat(64);
    let new_hash = "e".repeat(64);
    let old_record = test_record("search", &old_hash, "search_old");
    let new_record = test_record("search", &new_hash, "search_new");

    registry
        .write_version_record_atomically(&old_record)
        .unwrap();
    registry.write_record_atomically(&old_record).unwrap();
    registry
        .write_version_record_atomically(&new_record)
        .unwrap();
    registry.write_record_atomically(&new_record).unwrap();

    assert_eq!(
        registry
            .load_version_record("search", &old_hash)
            .unwrap()
            .manifest
            .operations[0]
            .name,
        "search_old"
    );
    assert_eq!(
        registry.load_record("search").unwrap().active_artifact_hash,
        new_hash
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn binding_snapshot_merges_scopes_and_tombstones_inherited_binding() {
    let root = temp_dir("capsule-bindings");
    let registry = crate::operation_store::LocalOperationRegistry::new(&root);
    let global_hash = "1".repeat(64);
    let tenant_hash = "2".repeat(64);
    let thread_hash = "3".repeat(64);
    for record in [
        test_record("global", &global_hash, "global_search"),
        test_record("tenant", &tenant_hash, "tenant_search"),
        test_record("thread", &thread_hash, "thread_search"),
    ] {
        registry.write_version_record_atomically(&record).unwrap();
    }

    registry
        .bind_capsule_operation(
            crate::operation_store::CapsuleBindingScope::global(),
            "global",
            &global_hash,
        )
        .unwrap();
    registry
        .bind_capsule_operation(
            crate::operation_store::CapsuleBindingScope::tenant("tenant-a"),
            "tenant",
            &tenant_hash,
        )
        .unwrap();
    registry
        .bind_capsule_operation(
            crate::operation_store::CapsuleBindingScope::thread("tenant-a", "thread-a"),
            "thread",
            &thread_hash,
        )
        .unwrap();

    let snapshot = registry
        .resolve_capsule_binding_snapshot(
            crate::operation_store::CapsuleBindingResolutionRequest::for_thread(
                "tenant-a", "thread-a",
            ),
        )
        .unwrap();
    assert_eq!(
        snapshot
            .records
            .iter()
            .map(|record| record.name.as_str())
            .collect::<Vec<_>>(),
        vec!["global", "tenant", "thread"]
    );

    registry
        .unbind_capsule_operation(
            crate::operation_store::CapsuleBindingScope::thread("tenant-a", "thread-a"),
            "global",
        )
        .unwrap();
    let snapshot = registry
        .resolve_capsule_binding_snapshot(
            crate::operation_store::CapsuleBindingResolutionRequest::for_thread(
                "tenant-a", "thread-a",
            ),
        )
        .unwrap();
    assert_eq!(
        snapshot
            .records
            .iter()
            .map(|record| record.name.as_str())
            .collect::<Vec<_>>(),
        vec!["tenant", "thread"]
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn binding_rejects_missing_version() {
    let root = temp_dir("capsule-binding-missing-version");
    let registry = crate::operation_store::LocalOperationRegistry::new(&root);
    let err = registry
        .bind_capsule_operation(
            crate::operation_store::CapsuleBindingScope::global(),
            "missing",
            &"f".repeat(64),
        )
        .unwrap_err()
        .to_string();

    assert!(err.contains("version"), "{err}");

    let _ = std::fs::remove_dir_all(root);
}
