use crate::secret_store::SecretResolver as _;

#[tokio::test]
async fn sqlite_secret_store_persists_and_redacts_status() {
    let store = crate::secret_store::SqliteSecretStore::in_memory()
        .await
        .unwrap();

    let status = store
        .set_secret(
            "EXAMPLE_API_KEY",
            "fixture-secret",
            crate::secret_store::SecretSourceKind::Env,
            Some("EXAMPLE_API_KEY".to_string()),
        )
        .await
        .unwrap();

    assert_eq!(status.name, "EXAMPLE_API_KEY");
    assert_eq!(
        status.source_kind,
        crate::secret_store::SecretSourceKind::Env
    );
    assert_eq!(status.source_label.as_deref(), Some("EXAMPLE_API_KEY"));
    assert!(status.value.redacted);
    assert_eq!(store.list().await.unwrap()[0], status);
    let resolved = store
        .resolve_secret("EXAMPLE_API_KEY")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.value, "fixture-secret");
}

#[tokio::test]
async fn manifest_secret_resolution_reports_missing_refs_without_values() {
    let store = crate::secret_store::SqliteSecretStore::in_memory()
        .await
        .unwrap();
    store
        .set_secret(
            "VISIBLE",
            "fixture-secret",
            crate::secret_store::SecretSourceKind::Local,
            None,
        )
        .await
        .unwrap();
    let manifest: verlet_abi::WasmOperationManifest = serde_json::from_value(serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "search",
            "input": "json",
            "output": "json",
            "events": "none",
            "mode": "sync",
            "required_capabilities": ["secret:VISIBLE", "secret:MISSING"]
        }]
    }))
    .unwrap();

    let resolution = crate::secret_store::resolve_manifest_secret_resolution(&store, &manifest)
        .await
        .unwrap();

    assert!(!resolution.is_ready());
    assert_eq!(
        resolution.values,
        std::collections::BTreeMap::from([("VISIBLE".to_string(), "fixture-secret".to_string())])
    );
    assert_eq!(
        resolution.missing,
        std::collections::BTreeSet::from(["MISSING".to_string()])
    );
}

#[test]
fn secret_names_are_path_safe_but_allow_env_style_names() {
    assert_eq!(
        crate::secret_store::validate_secret_name("EXAMPLE_API_KEY").unwrap(),
        "EXAMPLE_API_KEY"
    );
    assert_eq!(
        crate::secret_store::validate_secret_name("provider.search-key").unwrap(),
        "provider.search-key"
    );
    assert!(crate::secret_store::validate_secret_name("../EXAMPLE_API_KEY").is_err());
    assert!(crate::secret_store::validate_secret_name("").is_err());
}
