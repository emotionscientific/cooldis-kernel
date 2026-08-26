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
                &config,
                "fixture-write",
                "write_file",
                "at-most-once",
                &["secret:WRITE_TOKEN"],
            )],
        ))
        .unwrap();
    store
        .save(&installed_kit_record(
            "alpha",
            vec![
                installed_kit_tool(&config, "fixture-read", "read_text", "idempotent", &[]),
                installed_kit_tool(
                    &config,
                    "fixture-fetch",
                    "fetch_url",
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
    assert!(manifest.workspace.is_none());
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
    let fetch_record = verlet_operations::operation_store::LocalOperationRegistry::new(
        default_manifest_test_operations_root(&config),
    )
    .load_record("fixture-fetch")
    .unwrap();
    assert_eq!(
        rows[0].operation_ref,
        format!(
            "op://fixture-fetch/fetch_url@sha256:{}",
            fetch_record.active_artifact_hash
        )
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
fn installed_kit_surface_rows_bind_the_guest_workspace_root() {
    let root = default_manifest_kit_test_root("bound-root");
    let config = default_manifest_kit_test_config(&root);
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "pi",
            vec![installed_kit_surface_tool(
                &config,
                "pi-write",
                "write",
                "at-most-once",
                &["fs.write"],
            )],
        ))
        .unwrap();

    let plan = crate::adapters::app_server::default_manifest::default_manifest_publish_plan(
        &config, false, "1.0.0",
    )
    .unwrap();
    let manifest: verlet_agent::manifest_schema::AgentManifestSchema =
        serde_json::from_value(plan.resolved_manifest).unwrap();
    let row = manifest
        .tools
        .iter()
        .find_map(|tool| match tool {
            verlet_agent::manifest_schema::AgentManifestTool::Direct(tool)
                if tool.tool_name == "write" =>
            {
                Some(tool)
            }
            _ => None,
        })
        .unwrap();

    assert_eq!(
        row.attachment.bound_parameters,
        std::collections::BTreeMap::from([(
            "root".to_string(),
            serde_json::Value::String("/workspace".to_string()),
        )])
    );
    assert_eq!(
        manifest.workspace,
        Some(
            verlet_agent::manifest_schema::AgentManifestWorkspaceRequirement {
                guest_path: "/workspace".to_string(),
                min_mode: verlet_agent::manifest_schema::AgentManifestWorkspaceMode::ReadWrite,
            }
        )
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_surface_rows_reject_unsupported_bound_parameters() {
    let root = default_manifest_kit_test_root("unsupported-bound");
    let config = default_manifest_kit_test_config(&root);
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "custom",
            vec![installed_kit_tool_with_surface(
                &config,
                "custom-write",
                "write",
                "at-most-once",
                &[],
                Some(verlet_operations::tool_package::ToolSurfaceContract {
                    args_field: "args".to_string(),
                    bound: std::collections::BTreeSet::from(["tenant".to_string()]),
                }),
            )],
        ))
        .unwrap();

    let error = crate::adapters::app_server::default_manifest::default_manifest_publish_plan(
        &config, false, "1.0.0",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("custom"), "{error}");
    assert!(error.contains("tenant"), "{error}");
    assert!(error.contains("only root"), "{error}");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_surface_workspace_declaration_is_host_path_free() {
    let root = default_manifest_kit_test_root("host-path-free-workspace");
    let config = default_manifest_kit_test_config(&root);
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "pi",
            vec![installed_kit_surface_tool(
                &config,
                "pi-write",
                "write",
                "at-most-once",
                &[],
            )],
        ))
        .unwrap();

    let plan = crate::adapters::app_server::default_manifest::default_manifest_publish_plan(
        &config, false, "1.0.0",
    )
    .unwrap();
    assert!(
        plan.authored_source
            .contains("[workspace]\nguest_path = \"/workspace\"\nmin_mode = \"rw\""),
        "{}",
        plan.authored_source
    );
    assert!(!plan.authored_source.contains("host_path"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_rows_follow_kernel_tools_and_precede_config_operation_rows() {
    let root = default_manifest_kit_test_root("row-order");
    let mut config = default_manifest_kit_test_config(&root);
    let operations_root = default_manifest_test_operations_root(&config);
    crate::operations::kernel_packages::ensure_verlet_threads_published(Some(operations_root))
        .unwrap();
    crate::operations::kernel_packages::ensure_verlet_notify_published(Some(operations_root))
        .unwrap();
    default_manifest_configure_global_operation(
        &mut config,
        crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE,
    );
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "alpha",
            vec![installed_kit_tool(
                &config,
                "fixture-kit-lookup",
                "kit_lookup",
                "pure",
                &[],
            )],
        ))
        .unwrap();

    let manifest =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap();
    let rows = manifest
        .tools
        .iter()
        .map(|tool| match tool {
            verlet_agent::manifest_schema::AgentManifestTool::Direct(tool) => {
                ("direct", tool.id.as_str())
            }
            verlet_agent::manifest_schema::AgentManifestTool::Bash(tool) => {
                ("bash", tool.id.as_str())
            }
            verlet_agent::manifest_schema::AgentManifestTool::ProtocolImport(_) => {
                panic!("default manifest must not synthesize protocol imports")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("direct", "verlet-threads.thread_spawn"),
            ("direct", "verlet-threads.thread_submit"),
            ("direct", "verlet-threads.thread_wait"),
            ("direct", "verlet-threads.thread_status"),
            ("direct", "verlet-threads.thread_cancel"),
            ("direct", "kit.alpha.kit_lookup"),
            ("bash", "verlet-notify.notify_preview"),
            ("bash", "verlet-notify.channel_emit"),
        ]
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn preexisting_default_manifest_tool_rows_keep_byte_stable_effect_encoding() {
    let root = default_manifest_kit_test_root("legacy-effect-encoding");
    let mut config = default_manifest_kit_test_config(&root);
    let operations_root = default_manifest_test_operations_root(&config);
    crate::operations::kernel_packages::ensure_verlet_threads_published(Some(operations_root))
        .unwrap();
    crate::operations::kernel_packages::ensure_verlet_notify_published(Some(operations_root))
        .unwrap();
    default_manifest_configure_global_operation(
        &mut config,
        crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE,
    );

    let plan = crate::adapters::app_server::default_manifest::default_manifest_publish_plan(
        &config, false, "1.0.0",
    )
    .unwrap();
    let manifest: verlet_agent::manifest_schema::AgentManifestSchema =
        serde_json::from_value(plan.resolved_manifest).unwrap();

    assert!(manifest.tools.iter().all(|tool| match tool {
        verlet_agent::manifest_schema::AgentManifestTool::Bash(tool) => {
            tool.effect_class == verlet_agent::manifest_schema::EffectClass::AtMostOnce
        }
        verlet_agent::manifest_schema::AgentManifestTool::Direct(tool) => {
            tool.effect_class == verlet_agent::manifest_schema::EffectClass::AtMostOnce
        }
        verlet_agent::manifest_schema::AgentManifestTool::ProtocolImport(_) => false,
    }));
    assert!(!plan.authored_source.contains("effect_class"));

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
            vec![installed_kit_tool(
                &config,
                "fixture-drift",
                "read_text",
                "pure",
                &[],
            )],
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
    for (kit, operation_record) in [
        ("alpha", "fixture-alpha-read"),
        ("bravo", "fixture-bravo-read"),
    ] {
        store
            .save(&installed_kit_record(
                kit,
                vec![installed_kit_tool(
                    &config,
                    operation_record,
                    "read_text",
                    "pure",
                    &[],
                )],
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
fn duplicate_installed_kit_tool_name_within_one_record_names_the_kit() {
    let root = default_manifest_kit_test_root("duplicate-tool-name-within-record");
    let config = default_manifest_kit_test_config(&root);
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "alpha",
            vec![
                installed_kit_tool(&config, "fixture-read-one", "read_text", "pure", &[]),
                installed_kit_tool(&config, "fixture-read-two", "read_text", "pure", &[]),
            ],
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
    assert!(error.contains("read_text"), "{error}");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_config_operation_collision_names_both_sources() {
    let root = default_manifest_kit_test_root("config-operation-collision");
    let mut config = default_manifest_kit_test_config(&root);
    let operations_root = default_manifest_test_operations_root(&config);
    crate::operations::kernel_packages::ensure_verlet_notify_published(Some(operations_root))
        .unwrap();
    default_manifest_configure_global_operation(
        &mut config,
        crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE,
    );
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "alpha",
            vec![installed_kit_tool(
                &config,
                "fixture-kit-notify",
                "notify_preview",
                "pure",
                &[],
            )],
        ))
        .unwrap();

    let error =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("alpha"), "{error}");
    assert!(error.contains("notify_preview"), "{error}");
    assert!(error.contains("verlet-notify"), "{error}");
    assert!(error.contains("config-driven"), "{error}");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_kernel_tool_collision_names_kit_and_field() {
    let root = default_manifest_kit_test_root("kernel-tool-collision");
    let config = default_manifest_kit_test_config(&root);
    default_manifest_kit_store(&config)
        .save(&installed_kit_record(
            "alpha",
            vec![installed_kit_tool(
                &config,
                "fixture-thread-collision",
                "thread_spawn",
                "pure",
                &[],
            )],
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
            vec![installed_kit_tool(
                &config,
                "fixture-invalid-effect",
                "read_text",
                "sometimes",
                &[],
            )],
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
fn installed_kit_corrupt_capability_set_names_kit_field_and_grants() {
    let root = default_manifest_kit_test_root("corrupt-capabilities");
    let config = default_manifest_kit_test_config(&root);
    let mut tool = installed_kit_tool(
        &config,
        "fixture-corrupt-capabilities",
        "read_text",
        "pure",
        &[],
    );
    tool.required_capabilities = ["secret:", "unknown.capability"]
        .into_iter()
        .map(str::to_string)
        .collect();
    default_manifest_kit_store(&config)
        .save(&installed_kit_record("alpha", vec![tool]))
        .unwrap();

    let error =
        crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("alpha"), "{error}");
    assert!(error.contains("tools.required_capabilities"), "{error}");
    assert!(error.contains("secret:"), "{error}");
    assert!(error.contains("unknown.capability"), "{error}");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn installed_kit_malformed_attachment_capabilities_name_kit_field_and_grant() {
    for (label, grant) in [
        ("malformed-secret-capability", "secret:"),
        (
            "malformed-private-network-capability",
            "net.http.private:GET:",
        ),
    ] {
        let root = default_manifest_kit_test_root(label);
        let config = default_manifest_kit_test_config(&root);
        let tool = installed_kit_tool(
            &config,
            &format!("fixture-{label}"),
            "read_text",
            "pure",
            &[grant],
        );
        default_manifest_kit_store(&config)
            .save(&installed_kit_record("alpha", vec![tool]))
            .unwrap();

        let error = crate::adapters::app_server::default_manifest::synthesize_default_manifest_with_version(
            &config, false, "1.0.0",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("alpha"), "{error}");
        assert!(error.contains("tools.required_capabilities"), "{error}");
        assert!(error.contains(grant), "{error}");

        let _ = std::fs::remove_dir_all(root);
    }
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
    assert!(
        error.contains(&store.record_path("alpha").display().to_string()),
        "{error}"
    );

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
    let operations_root = default_manifest_test_operations_root(config);
    verlet_operations::kit_package::InstalledKitStore::new(
        verlet_operations::kit_package::kits_root_for_operations_registry_root(operations_root),
    )
}

fn default_manifest_test_operations_root(
    config: &crate::adapters::app_server::VerletAppServerConfig,
) -> &std::path::Path {
    // lexicon-allow: capsule - existing app-server operation binding config field
    config.capsule_bindings.registry_root.as_deref().unwrap()
}

fn default_manifest_configure_global_operation(
    config: &mut crate::adapters::app_server::VerletAppServerConfig,
    operation_name: &str,
) {
    // lexicon-allow: capsule - existing app-server operation binding config field
    config.capsule_bindings.global_operation_names = vec![operation_name.to_string()];
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
    config: &crate::adapters::app_server::VerletAppServerConfig,
    operation_record_name: &str,
    tool_name: &str,
    effect_class: &str,
    required_capabilities: &[&str],
) -> verlet_operations::kit_package::InstalledKitTool {
    installed_kit_tool_with_surface(
        config,
        operation_record_name,
        tool_name,
        effect_class,
        required_capabilities,
        None,
    )
}

fn installed_kit_surface_tool(
    config: &crate::adapters::app_server::VerletAppServerConfig,
    operation_record_name: &str,
    tool_name: &str,
    effect_class: &str,
    required_capabilities: &[&str],
) -> verlet_operations::kit_package::InstalledKitTool {
    installed_kit_tool_with_surface(
        config,
        operation_record_name,
        tool_name,
        effect_class,
        required_capabilities,
        Some(verlet_operations::tool_package::ToolSurfaceContract {
            args_field: "args".to_string(),
            bound: std::collections::BTreeSet::from(["root".to_string()]),
        }),
    )
}

fn installed_kit_tool_with_surface(
    config: &crate::adapters::app_server::VerletAppServerConfig,
    operation_record_name: &str,
    tool_name: &str,
    effect_class: &str,
    required_capabilities: &[&str],
    surface: Option<verlet_operations::tool_package::ToolSurfaceContract>,
) -> verlet_operations::kit_package::InstalledKitTool {
    let required_capabilities = required_capabilities
        .iter()
        .map(|capability| (*capability).to_string())
        .collect::<std::collections::BTreeSet<_>>();
    let manifest = verlet_abi::WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: vec![verlet_abi::WasmOperationDefinition {
            id: 1,
            name: tool_name.to_string(),
            input: verlet_abi::WasmOperationValueKind::Json,
            output: verlet_abi::WasmOperationValueKind::Json,
            events: verlet_abi::WasmOperationEventKind::None,
            mode: verlet_abi::WasmOperationMode::Sync,
            required_capabilities: required_capabilities.iter().cloned().collect(),
        }],
    };
    let interface = verlet_operations::tool_package::ToolInterfaceContract {
        schema_version: verlet_operations::tool_package::TOOL_PACKAGE_SCHEMA_VERSION,
        identity: verlet_operations::tool_package::ToolPackageIdentity {
            name: operation_record_name.to_string(),
            version: Some("1.0.0".to_string()),
            description: None,
            owner: None,
        },
        runtime: verlet_operations::tool_package::ToolRuntimeContract {
            kind: crate::operations::kernel_packages::KERNEL_RUNTIME_KIND.to_string(),
            state: None,
            module_path: None,
            bin_path: None,
            release: None,
            timeout_ms: None,
            max_input_bytes: None,
            max_output_bytes: None,
        },
        operations: vec![verlet_operations::tool_package::ToolOperationInterface {
            name: tool_name.to_string(),
            description: None,
            input_schema: surface.as_ref().map_or_else(
                || serde_json::json!({"type": "object"}),
                installed_kit_surface_input_schema,
            ),
            output_schema: serde_json::json!({"type": "object"}),
            required_capabilities: required_capabilities.clone(),
            surface,
            command: None,
            mcp: None,
            manual: None,
        }],
        fixtures: Vec::new(),
    };
    let operations_root = default_manifest_test_operations_root(config);
    let record = verlet_operations::operation_store::LocalOperationRegistry::new(operations_root)
        .publish_interface_record(
            verlet_operations::operation_store::PublishInterfaceOperationRequest {
                name: operation_record_name.to_string(),
                source: verlet_operations::operation_store::PublishedOperationSource::Kernel {
                    package: operation_record_name.to_string(),
                },
                manifest,
                interface,
                capability_grants: required_capabilities.clone(),
                metadata: std::collections::BTreeMap::new(),
            },
        )
        .unwrap();
    verlet_operations::kit_package::InstalledKitTool {
        tool_name: tool_name.to_string(),
        operation_ref: format!(
            "op://{operation_record_name}/{tool_name}@sha256:{}",
            record.active_artifact_hash
        ),
        effect_class: effect_class.to_string(),
        required_capabilities,
    }
}

fn installed_kit_surface_input_schema(
    surface: &verlet_operations::tool_package::ToolSurfaceContract,
) -> serde_json::Value {
    let mut properties = serde_json::Map::from_iter([(
        surface.args_field.clone(),
        serde_json::json!({
            "type": "object",
            "additionalProperties": false
        }),
    )]);
    for name in &surface.bound {
        properties.insert(name.clone(), serde_json::json!({"type": "string"}));
    }
    let mut required = surface.bound.iter().cloned().collect::<Vec<_>>();
    required.push(surface.args_field.clone());
    serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}
