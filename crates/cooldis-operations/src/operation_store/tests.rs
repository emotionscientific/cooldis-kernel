use super::*;
use cooldis_abi::{WasmOperationDefinition, WasmOperationValueKind};
use uuid::Uuid;

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("cooldis-{label}-{}", Uuid::now_v7()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn test_manifest() -> WasmOperationManifest {
    test_manifest_with_operation("cat")
}

fn test_manifest_with_operation(operation_name: &str) -> WasmOperationManifest {
    WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: vec![WasmOperationDefinition {
            id: 1,
            name: operation_name.to_string(),
            input: WasmOperationValueKind::Text,
            output: WasmOperationValueKind::Text,
            events: Default::default(),
            mode: Default::default(),
            required_capabilities: vec![],
        }],
    }
}

fn test_record(name: &str, hash: &str, operation_name: &str) -> PublishedOperationRecord {
    let manifest = test_manifest_with_operation(operation_name);
    let registered = RegisteredOperation {
        name: name.to_string(),
        manifest: manifest.clone(),
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
    };
    PublishedOperationRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        name: name.to_string(),
        active_artifact_hash: hash.to_string(),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
        source: PublishedOperationSource::Wasm {
            bin_path: PathBuf::from(format!("{name}.wasm")),
        },
        build: PublishedOperationBuild {
            artifact_path: PathBuf::from(format!("{name}.wasm")),
            published_at_ms: 1,
        },
    }
}

#[test]
fn blob_store_put_get_and_detects_corruption() {
    let root = temp_dir("blob-store");
    let store = OperationBlobStore::new(root.join("blobs"));
    let hash = store.put(b"hello wasm-ish bytes").unwrap();

    assert_eq!(
        store.get(&hash).unwrap(),
        Some(b"hello wasm-ish bytes".to_vec())
    );
    assert_eq!(store.put(b"hello wasm-ish bytes").unwrap(), hash);
    assert_eq!(store.get(&"0".repeat(64)).unwrap(), None);

    let path = store.artifact_path(&hash).unwrap();
    fs::write(path, b"corrupt").unwrap();
    let err = store.get(&hash).unwrap_err().to_string();
    assert!(err.contains("hash mismatch"), "{err}");
}

#[test]
fn published_record_round_trips_and_validates_projection() {
    let manifest = test_manifest();
    let registered = RegisteredOperation {
        name: "tailcat".to_string(),
        manifest: manifest.clone(),
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
    };
    let record = PublishedOperationRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        name: "tailcat".to_string(),
        active_artifact_hash: "a".repeat(64),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
        source: PublishedOperationSource::Wasm {
            bin_path: PathBuf::from("tool.wasm"),
        },
        build: PublishedOperationBuild {
            artifact_path: PathBuf::from("tool.wasm"),
            published_at_ms: 1,
        },
    };

    record.validate().unwrap();
    let json = serde_json::to_vec(&record).unwrap();
    let decoded: PublishedOperationRecord = serde_json::from_slice(&json).unwrap();
    assert_eq!(decoded, record);
}

#[test]
fn record_validation_rejects_stale_projection() {
    let manifest = test_manifest();
    let registered = RegisteredOperation {
        name: "tailcat".to_string(),
        manifest: manifest.clone(),
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
    };
    let mut record = PublishedOperationRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        name: "tailcat".to_string(),
        active_artifact_hash: "b".repeat(64),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
        source: PublishedOperationSource::Wasm {
            bin_path: PathBuf::from("tool.wasm"),
        },
        build: PublishedOperationBuild {
            artifact_path: PathBuf::from("tool.wasm"),
            published_at_ms: 1,
        },
    };
    record.projections.registered_name = "other".to_string();

    let err = record.validate().unwrap_err().to_string();
    assert!(err.contains("projections are stale"), "{err}");
}

#[test]
fn record_validation_rejects_manifest_without_operations() {
    let manifest = WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: vec![],
    };
    let registered = RegisteredOperation {
        name: "empty".to_string(),
        manifest: manifest.clone(),
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
    };
    let record = PublishedOperationRecord {
        schema_version: RECORD_SCHEMA_VERSION,
        name: "empty".to_string(),
        active_artifact_hash: "c".repeat(64),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
        source: PublishedOperationSource::Wasm {
            bin_path: PathBuf::from("tool.wasm"),
        },
        build: PublishedOperationBuild {
            artifact_path: PathBuf::from("tool.wasm"),
            published_at_ms: 1,
        },
    };

    let err = record.validate().unwrap_err().to_string();
    assert!(err.contains("has no operations"), "{err}");
}

#[test]
fn capsule_binding_scope_json_uses_camel_case_and_accepts_legacy_snake_case() {
    let scope = CapsuleBindingScope::thread("tenant-a", "thread-a");
    let value = serde_json::to_value(&scope).unwrap();
    assert_eq!(value["kind"].as_str(), Some("thread"));
    assert_eq!(value["tenantId"].as_str(), Some("tenant-a"));
    assert_eq!(value["threadId"].as_str(), Some("thread-a"));
    assert!(value.get("tenant_id").is_none());
    assert!(value.get("thread_id").is_none());

    let legacy: CapsuleBindingScope = serde_json::from_value(serde_json::json!({
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
    let registry = LocalOperationRegistry::new(&root);
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

    let _ = fs::remove_dir_all(root);
}

#[test]
fn binding_snapshot_merges_scopes_and_tombstones_inherited_binding() {
    let root = temp_dir("capsule-bindings");
    let registry = LocalOperationRegistry::new(&root);
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
        .bind_capsule_operation(CapsuleBindingScope::global(), "global", &global_hash)
        .unwrap();
    registry
        .bind_capsule_operation(
            CapsuleBindingScope::tenant("tenant-a"),
            "tenant",
            &tenant_hash,
        )
        .unwrap();
    registry
        .bind_capsule_operation(
            CapsuleBindingScope::thread("tenant-a", "thread-a"),
            "thread",
            &thread_hash,
        )
        .unwrap();

    let snapshot = registry
        .resolve_capsule_binding_snapshot(CapsuleBindingResolutionRequest::for_thread(
            "tenant-a", "thread-a",
        ))
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
            CapsuleBindingScope::thread("tenant-a", "thread-a"),
            "global",
        )
        .unwrap();
    let snapshot = registry
        .resolve_capsule_binding_snapshot(CapsuleBindingResolutionRequest::for_thread(
            "tenant-a", "thread-a",
        ))
        .unwrap();
    assert_eq!(
        snapshot
            .records
            .iter()
            .map(|record| record.name.as_str())
            .collect::<Vec<_>>(),
        vec!["tenant", "thread"]
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn binding_rejects_missing_version() {
    let root = temp_dir("capsule-binding-missing-version");
    let registry = LocalOperationRegistry::new(&root);
    let err = registry
        .bind_capsule_operation(CapsuleBindingScope::global(), "missing", &"f".repeat(64))
        .unwrap_err()
        .to_string();

    assert!(err.contains("version"), "{err}");

    let _ = fs::remove_dir_all(root);
}
