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

#[test]
fn installed_kits_synthesize_sorted_direct_rows_with_pinned_refs_and_attachments() {
    let root = default_manifest_kit_test_root("direct-rows");
    let config = default_manifest_kit_test_config(&root);
    let store = default_manifest_kit_store(&config);
    store
        .save(&installed_kit_record(
            "bravo",
            vec![installed_kit_tool(
                "write_file",
                'b',
                "at-most-once",
                &["secret:WRITE_TOKEN"],
            )],
        ))
        .unwrap();
    store
        .save(&installed_kit_record(
            "alpha",
            vec![
                installed_kit_tool("read_text", '2', "idempotent", &[]),
                installed_kit_tool(
                    "fetch_url",
                    '1',
                    "pure",
                    &[
                        "secret:FETCH_TOKEN",
                        "net.http.private:GET:https://internal.example",
                    ],
                ),
            ],
        ))
        .unwrap();

    let plan = crate::adapters::app_server::default_manifest::default_manifest_publish_plan(
        &config, false, "1.0.0",
    )
    .unwrap();
    let manifest: verlet_agent::manifest_schema::AgentManifestSchema =
        serde_json::from_value(plan.resolved_manifest).unwrap();
    let rows = manifest
        .tools
        .iter()
        .map(|tool| match tool {
            verlet_agent::manifest_schema::AgentManifestTool::Direct(tool) => tool,
            other => panic!("expected direct kit row, got {other:?}"),
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows.iter()
            .map(|row| (row.id.as_str(), row.tool_name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("kit.alpha.fetch_url", "fetch_url"),
            ("kit.alpha.read_text", "read_text"),
            ("kit.bravo.write_file", "write_file"),
        ]
    );
    assert_eq!(
        rows[0].operation_ref,
        format!("op://fixture/fetch_url@sha256:{}", "1".repeat(64))
    );
    assert_eq!(
        rows[0].effect_class,
        verlet_agent::manifest_schema::EffectClass::Pure
    );
    assert_eq!(
        rows[0].attachment.allowed_secrets,
        ["FETCH_TOKEN".to_string()].into_iter().collect()
    );
    assert_eq!(
        rows[0].attachment.allowed_private_network,
        [(
            "https://internal.example".to_string(),
            ["GET".to_string()].into_iter().collect()
        )]
        .into_iter()
        .collect()
    );
    assert_eq!(
        rows[1].effect_class,
        verlet_agent::manifest_schema::EffectClass::Idempotent
    );
    assert_eq!(
        rows[2].attachment.allowed_secrets,
        ["WRITE_TOKEN".to_string()].into_iter().collect()
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_add_and_remove_change_default_manifest_hash() {
    let root = default_manifest_kit_test_root("manifest-drift");
    let config = default_manifest_kit_test_config(&root);
    let store = default_manifest_kit_store(&config);

    let before = crate::adapters::app_server::default_manifest::default_manifest_publish_plan(
        &config, false, "1.0.0",
    )
    .unwrap();
    store
        .save(&installed_kit_record(
            "alpha",
            vec![installed_kit_tool("read_text", '1', "pure", &[])],
        ))
        .unwrap();
    let installed = crate::adapters::app_server::default_manifest::default_manifest_publish_plan(
        &config, false, "1.0.0",
    )
    .unwrap();
    assert_ne!(before.manifest_hash, installed.manifest_hash);

    assert!(store.remove("alpha").unwrap());
    let removed = crate::adapters::app_server::default_manifest::default_manifest_publish_plan(
        &config, false, "1.0.0",
    )
    .unwrap();
    assert_ne!(installed.manifest_hash, removed.manifest_hash);
    assert_eq!(before.manifest_hash, removed.manifest_hash);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn duplicate_installed_kit_tool_name_names_both_kits() {
    let root = default_manifest_kit_test_root("duplicate-tool-name");
    let config = default_manifest_kit_test_config(&root);
    let store = default_manifest_kit_store(&config);
    for (kit, hash_digit) in [("alpha", '1'), ("bravo", '2')] {
        store
            .save(&installed_kit_record(
                kit,
                vec![installed_kit_tool("read_text", hash_digit, "pure", &[])],
            ))
            .unwrap();
    }

    let error =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("alpha"), "{error}");
    assert!(error.contains("bravo"), "{error}");
    assert!(error.contains("tool_name"), "{error}");
    assert!(error.contains("read_text"), "{error}");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_kernel_tool_collision_names_kit_and_field() {
    let root = default_manifest_kit_test_root("kernel-tool-collision");
    let config = default_manifest_kit_test_config(&root);
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "alpha",
            vec![installed_kit_tool("thread_spawn", '1', "pure", &[])],
        ))
        .unwrap();

    let error =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("alpha"), "{error}");
    assert!(error.contains("tool_name"), "{error}");
    assert!(error.contains("thread_spawn"), "{error}");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_invalid_effect_class_names_kit_and_field() {
    let root = default_manifest_kit_test_root("invalid-effect-class");
    let config = default_manifest_kit_test_config(&root);
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "alpha",
            vec![installed_kit_tool("read_text", '1', "sometimes", &[])],
        ))
        .unwrap();

    let error =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("alpha"), "{error}");
    assert!(error.contains("tools.effect_class"), "{error}");
    assert!(error.contains("sometimes"), "{error}");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn corrupt_installed_kit_record_names_kit() {
    let root = default_manifest_kit_test_root("corrupt-record");
    let config = default_manifest_kit_test_config(&root);
    let store = default_manifest_kit_store(&config);
    std::fs::create_dir_all(store.root()).unwrap();
    std::fs::write(store.record_path("alpha"), b"{not json").unwrap();

    let error =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("alpha"), "{error}");
    assert!(error.contains("installed-kit record"), "{error}");

    let _ = std::fs::remove_dir_all(root);
}

fn default_manifest_kit_test_root(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "verlet-default-manifest-{label}-{}",
        uuid::Uuid::now_v7()
    ))
}

fn default_manifest_kit_test_config(
    root: &std::path::Path,
) -> crate::adapters::app_server::VerletAppServerConfig {
    let mut config = crate::adapters::app_server::VerletAppServerConfig::local(
        crate::adapters::app_server::AppServerListenAddr::Unix(root.join("app-server.sock")),
        root,
    );
    // lexicon-allow: capsule - existing app-server operation binding config field
    config.capsule_bindings.registry_root = Some(root.join("operations"));
    config
}

fn default_manifest_kit_store(
    config: &crate::adapters::app_server::VerletAppServerConfig,
) -> verlet_operations::kit_package::InstalledKitStore {
    // lexicon-allow: capsule - existing app-server operation binding config field
    let operations_root = config.capsule_bindings.registry_root.as_ref().unwrap();
    verlet_operations::kit_package::InstalledKitStore::new(
        verlet_operations::kit_package::kits_root_for_operations_registry_root(operations_root),
    )
}

fn installed_kit_record(
    name: &str,
    tools: Vec<verlet_operations::kit_package::InstalledKitTool>,
) -> verlet_operations::kit_package::InstalledKitRecord {
    verlet_operations::kit_package::InstalledKitRecord {
        schema_version: verlet_operations::kit_package::INSTALLED_KIT_SCHEMA_VERSION,
        name: name.to_string(),
        version: Some("1.0.0".to_string()),
        source: verlet_operations::kit_package::InstalledKitSource::Path {
            path: std::path::PathBuf::from("/fixture/kit"),
        },
        source_hash: "0".repeat(64),
        installed_at_ms: 1,
        tools,
    }
}

fn installed_kit_tool(
    tool_name: &str,
    hash_digit: char,
    effect_class: &str,
    required_capabilities: &[&str],
) -> verlet_operations::kit_package::InstalledKitTool {
    verlet_operations::kit_package::InstalledKitTool {
        tool_name: tool_name.to_string(),
        operation_ref: format!(
            "op://fixture/{tool_name}@sha256:{}",
            hash_digit.to_string().repeat(64)
        ),
        effect_class: effect_class.to_string(),
        required_capabilities: required_capabilities
            .iter()
            .map(|capability| (*capability).to_string())
            .collect(),
    }
}
