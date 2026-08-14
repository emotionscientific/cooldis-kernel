#[test]
fn patch_bump_version_rejects_overflow() {
    let version = format!("1.0.{}", u64::MAX);
    let err =
        crate::adapters::app_server::default_manifest::patch_bump_version(&version).unwrap_err();
    assert!(err.to_string().contains("not a patch-bumpable semver"));
}

#[test]
fn synthesized_default_manifest_preserves_slash_bearing_model_ids() {
    let mut config = crate::adapters::app_server::VerletAppServerConfig::local(
        crate::adapters::app_server::AppServerListenAddr::Unix(
            std::env::temp_dir().join("verlet-test.sock"),
        ),
        std::env::temp_dir(),
    );
    config.model_provider = "anthropic".to_string();
    config.model = "bedrock/global.anthropic.claude-sonnet-4-5-20250929-v1:0".to_string();

    let manifest =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "0.1.0",
        )
        .unwrap();
    let profile = &manifest.model_profiles[0];

    assert_eq!(profile.provider_ref, "provider://anthropic");
    assert_eq!(
        profile.model_ref,
        "model://anthropic/bedrock/global.anthropic.claude-sonnet-4-5-20250929-v1:0"
    );
}

#[test]
fn synthesized_default_manifest_uses_configured_cwd_without_trailing_separator() {
    let cwd = std::env::temp_dir().join(format!(
        "verlet-default-manifest-cwd-{}",
        uuid::Uuid::now_v7()
    ));
    let config = crate::adapters::app_server::VerletAppServerConfig::local(
        crate::adapters::app_server::AppServerListenAddr::Unix(
            std::env::temp_dir().join("verlet-test.sock"),
        ),
        &cwd,
    );

    let manifest =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "0.1.0",
        )
        .unwrap();

    assert_eq!(manifest.runtime.default_cwd, cwd.to_string_lossy());
}

#[test]
fn existing_legacy_default_agent_record_requires_republish() {
    let root = std::env::temp_dir().join(format!(
        "verlet-default-manifest-legacy-{}",
        uuid::Uuid::now_v7()
    ));
    let mut config = crate::adapters::app_server::VerletAppServerConfig::local(
        crate::adapters::app_server::AppServerListenAddr::Unix(root.join("app-server.sock")),
        &root,
    );
    config.agent_registry_root = root.join("agents");

    let mut manifest =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap();
    manifest.identity.namespace = Some(concat!("cool", "dis").to_string());
    let source =
        crate::adapters::app_server::default_manifest::default_manifest_source(&manifest).unwrap();
    let old_record = crate::agent::manifest::LocalAgentRegistry::new(&config.agent_registry_root)
        .publish_plan(crate::agent::manifest::AgentPublishPlan::from_source(&source).unwrap())
        .unwrap();
    assert_eq!(
        old_record.namespace.as_deref(),
        Some(concat!("cool", "dis"))
    );

    assert_eq!(
        crate::adapters::app_server::default_manifest::ensure_default_manifest_published(
            &config, false,
        )
        .unwrap_err()
        .to_string(),
        format!(
            "runtime factory failed: default agent record {} uses unsupported namespace {:?}; republish the record with the current Verlet version",
            old_record.ref_uri, old_record.namespace
        )
    );

    let _ = std::fs::remove_dir_all(root);
}
