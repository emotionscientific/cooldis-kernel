use crate::secret_store::SecretResolver as _;

/// These strings are the `source_kind` column of stored secret records, so a
/// variant rename must not silently change them.
#[test]
fn source_kind_strings_match_persisted_values() {
    assert_eq!(crate::secret_store::SecretSourceKind::Env.as_ref(), "env");
    assert_eq!(
        crate::secret_store::SecretSourceKind::Stdin.as_ref(),
        "stdin"
    );
    assert_eq!(
        crate::secret_store::SecretSourceKind::Local.as_ref(),
        "local"
    );
}

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
    assert!(
        resolved.value == "fixture-secret",
        "resolved secret value did not match the stored value"
    );
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
    assert!(
        resolution.values.get("VISIBLE").map(String::as_str) == Some("fixture-secret"),
        "manifest resolution returned the wrong stored value"
    );
    assert_eq!(
        resolution.missing,
        std::collections::BTreeSet::from(["MISSING".to_string()])
    );
}

#[test]
fn resolved_secret_debug_redacts_plaintext_values() {
    let plaintext = format!("debug-secret-{}", uuid::Uuid::now_v7());
    let resolved = crate::secret_store::ResolvedSecret {
        name: "EXAMPLE_API_KEY".to_string(),
        value: plaintext.clone(),
        source_kind: crate::secret_store::SecretSourceKind::Local,
        source_label: None,
        updated_at_ms: 1,
    };
    let resolution = crate::secret_store::ManifestSecretResolution {
        values: std::collections::BTreeMap::from([(
            "EXAMPLE_API_KEY".to_string(),
            plaintext.clone(),
        )]),
        missing: std::collections::BTreeSet::new(),
    };

    let resolved_debug = format!("{resolved:?}");
    let resolution_debug = format!("{resolution:?}");
    assert!(!resolved_debug.contains(&plaintext), "{resolved_debug}");
    assert!(!resolution_debug.contains(&plaintext), "{resolution_debug}");
    assert!(resolved_debug.contains("<redacted>"), "{resolved_debug}");
    assert!(
        resolution_debug.contains("<redacted>"),
        "{resolution_debug}"
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

#[tokio::test]
async fn secret_import_ignores_legacy_verlet_environment_name() {
    let store = crate::secret_store::SqliteSecretStore::in_memory()
        .await
        .unwrap();
    let suffix = uuid::Uuid::now_v7().simple().to_string();
    let canonical = format!("VERLET_TEST_SECRET_{suffix}");
    let legacy = format!("{}TEST_SECRET_{suffix}", concat!("COOL", "DIS_"));

    // SAFETY: both names are unique to this test invocation, so no parallel
    // test can read or mutate them.
    unsafe { std::env::set_var(&legacy, "legacy-secret") };
    let result = store
        .import_secret_from_env("TEST_SECRET", &canonical)
        .await;
    // SAFETY: the variable is unique to this test invocation.
    unsafe { std::env::remove_var(&legacy) };

    assert!(matches!(
        result,
        Err(crate::secret_store::SecretStoreError::MissingEnv { env_name, .. })
            if env_name == canonical
    ));
}
