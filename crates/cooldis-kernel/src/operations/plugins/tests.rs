use super::*;
use crate::{
    PublishOperationRequest, PublishedOperationBuild, PublishedOperationSource, ResolvedSecret,
    SecretStoreResult,
};
use bashkit::FileSystemExt as _;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

struct EmptySecretResolver;

#[async_trait::async_trait]
impl SecretResolver for EmptySecretResolver {
    async fn resolve_secret(&self, _name: &str) -> SecretStoreResult<Option<ResolvedSecret>> {
        Ok(None)
    }
}

struct StaticSecretResolver {
    secrets: BTreeMap<String, String>,
}

impl StaticSecretResolver {
    fn new(secrets: impl IntoIterator<Item = (&'static str, &'static str)>) -> Self {
        Self {
            secrets: secrets
                .into_iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        }
    }
}

#[async_trait::async_trait]
impl SecretResolver for StaticSecretResolver {
    async fn resolve_secret(&self, name: &str) -> SecretStoreResult<Option<ResolvedSecret>> {
        Ok(self.secrets.get(name).map(|value| ResolvedSecret {
            name: name.to_string(),
            value: value.clone(),
            source_kind: crate::SecretSourceKind::Local,
            source_label: None,
            updated_at_ms: 0,
        }))
    }
}

#[cfg(unix)]
#[test]
fn pinned_host_mount_rejects_repointing_after_bind_resolution() {
    use std::os::unix::fs::symlink;

    let root = temp_dir("plugin-pinned-host-root");
    let selected = root.join("selected");
    let original = root.join("original");
    let outside = root.join("outside");
    std::fs::create_dir_all(&selected).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let witnessed = std::fs::canonicalize(&selected).unwrap();
    std::fs::rename(&selected, &original).unwrap();
    symlink(&outside, &selected).unwrap();
    let vfs = CooldisVfs::new(Arc::new(InMemoryFs::new()));

    let error = mount_plugin_filesystems(
        &vfs,
        vec![PluginMount::pinned_host_read_write("/work", witnessed)],
    )
    .unwrap_err();

    assert!(error.to_string().contains("witnessed canonical root"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn plugin_mount_assembly_rejects_spill_and_descendants() {
    let root = temp_dir("plugin-reserved-spill-mount");
    std::fs::create_dir_all(&root).unwrap();

    for guest_path in ["/spill", "/spill/nested"] {
        let vfs = CooldisVfs::new(Arc::new(InMemoryFs::new()));
        let error =
            mount_plugin_filesystems(&vfs, vec![PluginMount::host_read_write(guest_path, &root)])
                .unwrap_err();
        assert!(error.to_string().contains("reserved"), "{error}");
    }

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn catalog_vfs_allows_two_retention_sized_spill_files() {
    let root = temp_dir("plugin-spill-retention-limit");
    let catalog = LocalPluginCatalog::load_records(root.clone(), Vec::new(), Vec::new())
        .await
        .unwrap();

    assert!(
        catalog.vfs().limits().max_file_size
            >= u64::try_from(crate::SPILL_RETENTION_MAX_BYTES).unwrap()
    );
    assert!(
        catalog.vfs().limits().max_total_bytes
            >= u64::try_from(cooldis_vbash::SPILL_VFS_MAX_BYTES).unwrap()
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn selected_records_resolve_secrets_from_filtered_manifest() {
    let root = temp_dir("plugin-selected-secret-filter");
    let record = publish_multi_operation_record(
        &root,
        "analytics",
        &[
            ("profile", vec!["secret:VISIBLE"]),
            ("summarize", vec!["secret:BAD/NAME"]),
        ],
    )
    .await;

    let catalog = LocalPluginCatalog::load_selected_records_with_secret_resolver(
        root.clone(),
        vec![LocalPluginCatalogRecord::selected_operations(
            record,
            ["profile".to_string()],
        )],
        Vec::new(),
        Arc::new(StaticSecretResolver::new([("VISIBLE", "fixture-secret")])),
    )
    .await
    .unwrap();

    let operations = catalog.operations();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].manifest.operations.len(), 1);
    assert_eq!(operations[0].manifest.operations[0].name, "profile");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn catalog_load_fails_closed_when_selected_secret_is_missing() {
    let root = temp_dir("plugin-missing-secret");
    let record = publish_multi_operation_record(
        &root,
        "search",
        &[("search", vec!["secret:EXAMPLE_API_KEY"])],
    )
    .await;

    let result = LocalPluginCatalog::load_records_with_secret_resolver(
        root.clone(),
        vec![record],
        Vec::new(),
        Arc::new(EmptySecretResolver),
    )
    .await;
    let err = match result {
        Ok(_) => panic!("catalog load should fail closed when selected secrets are missing"),
        Err(err) => err,
    };
    let message = err.to_string();

    assert!(message.contains("missing required operation secrets: EXAMPLE_API_KEY"));
    assert!(message.contains("cooldis secret import <name> --from-env <ENV>"));
    assert!(!message.contains("fixture-secret"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn catalog_loads_published_manifest_without_describing_wasm_blob() {
    let root = temp_dir("plugin-published-manifest-no-describe");
    let registry = LocalOperationRegistry::new(&root);
    let artifact_hash = registry.blobs().put(b"not valid wasm").unwrap();
    let manifest: crate::WasmOperationManifest = serde_json::from_value(serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "echo",
            "input": "bytes",
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": []
        }]
    }))
    .unwrap();
    let registered = RegisteredOperation {
        name: "invalid".to_string(),
        manifest: manifest.clone(),
        capability_grants: BTreeSet::new(),
        metadata: Default::default(),
    };
    let record = PublishedOperationRecord {
        schema_version: 1,
        name: registered.name.clone(),
        active_artifact_hash: artifact_hash,
        manifest: manifest.clone(),
        projections: registered.projections(),
        interface: None,
        capability_grants: BTreeSet::new(),
        metadata: Default::default(),
        source: PublishedOperationSource::Wasm {
            bin_path: root.join("invalid.wasm"),
        },
        build: PublishedOperationBuild {
            artifact_path: root.join("invalid.wasm"),
            published_at_ms: 0,
        },
    };
    record.validate().unwrap();

    let catalog = LocalPluginCatalog::load_records(root.clone(), vec![record], Vec::new())
        .await
        .unwrap();

    assert_eq!(catalog.operations().len(), 1);
    assert_eq!(catalog.operations()[0].manifest, manifest);
    let _ = std::fs::remove_dir_all(root);
}

async fn publish_multi_operation_record(
    root: &Path,
    record_name: &str,
    operations: &[(&str, Vec<&str>)],
) -> PublishedOperationRecord {
    let wasm =
        wat::parse_str(multi_operation_guest_with_required_capabilities(operations)).unwrap();
    let artifact = root.join(format!("{record_name}.wasm"));
    std::fs::write(&artifact, wasm).unwrap();
    let capability_grants = operations
        .iter()
        .flat_map(|(_, capabilities)| {
            capabilities
                .iter()
                .map(|capability| (*capability).to_string())
        })
        .collect();
    LocalOperationRegistry::new(root)
        .publish_artifact(PublishOperationRequest {
            name: record_name.to_string(),
            artifact_path: artifact.clone(),
            source: PublishedOperationSource::Wasm { bin_path: artifact },
            interface: None,
            capability_grants,
            metadata: Default::default(),
        })
        .await
        .unwrap()
}

fn multi_operation_guest_with_required_capabilities(operations: &[(&str, Vec<&str>)]) -> String {
    let operations = operations
        .iter()
        .enumerate()
        .map(|(index, (name, required_capabilities))| {
            serde_json::json!({
                "id": index + 1,
                "name": name,
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": required_capabilities
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
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write
                drop
                i32.const 0)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
    )
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("cooldis-{label}-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&path).unwrap();
    path
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
