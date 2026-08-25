#[path = "support/test_mount.rs"]
mod support;

#[tokio::test]
async fn path3_local_plugin_build_publish_mount_and_agent_confirm() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let temp = temp_dir("path3-plugin");
    let registry_root = temp.join("plugins");
    let workspace = temp.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("input.txt"), "published before mount\n").unwrap();

    let build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(&module_path),
    )
    .unwrap();
    verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: "tailcat".to_string(),
                artifact_path: build.artifact_path,
                source: verlet_operations::operation_store::PublishedOperationSource::Rust {
                    module_path,
                    release: true,
                },
                interface: None,
                capability_grants: std::collections::BTreeSet::from([
                    verlet_wasm::runner::FS_WRITE_CAPABILITY.to_string(),
                ]),
                metadata: std::collections::BTreeMap::new(),
            },
        )
        .await
        .unwrap();

    std::fs::write(workspace.join("input.txt"), "hello from live plugin fs\n").unwrap();
    let catalog = verlet::operations::plugins::LocalPluginCatalog::load(
        verlet::operations::plugins::LocalPluginCatalogConfig::new(&registry_root).with_mount(
            verlet::operations::plugins::PluginMount::host_read_only("/workspace", &workspace),
        ),
    )
    .await
    .unwrap();
    assert_eq!(catalog.operations().len(), 1);

    let direct = catalog
        .operation_registry()
        .invoke_bytes("tailcat", "cat", b"/workspace/input.txt".to_vec())
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(direct.output).unwrap(),
        "hello from live plugin fs\n"
    );

    let client = std::sync::Arc::new(
        crate::support::scripted_provider::ScriptedProviderClient::with_responses(vec![
            crate::support::scripted_provider::response_tool_call(
                "tailcat_cat",
                serde_json::json!({"input": "/workspace/input.txt"}),
            ),
            crate::support::scripted_provider::response_text(
                "confirmed: hello from live plugin fs",
            ),
        ]),
    );
    let mut config = verlet::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    let factory = verlet::adapters::agent_loop::AgentLoopFactory::new(config, client.clone())
        .with_operation_registry(catalog.operation_registry());
    let host = verlet::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(factory));
    let thread = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new(
                "tenant_a",
                "user_1",
                "plugin_session",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-plugin",
        "Use the tailcat plugin to read /workspace/input.txt and confirm the content.",
    )
    .await
    .unwrap();
    crate::support::event_trace::collect_until_output(
        &mut events,
        "confirmed: hello from live plugin fs",
    )
    .await;

    let requests = client.requests();
    assert!(
        requests[0]
            .tools
            .iter()
            .any(|tool| tool.name == "tailcat_cat")
    );
    let second_request_text = requests[1]
        .messages
        .iter()
        .map(crate::support::event_trace::text_from_message)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(second_request_text.contains("hello from live plugin fs\n"));

    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn catalog_workspace_vfs_is_shared_with_virtual_bash() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let temp = temp_dir("path3-catalog-bash-workspace");
    let registry_root = temp.join("plugins");

    publish_workspace_reader(&registry_root, &module_path).await;

    let catalog = verlet::operations::plugins::LocalPluginCatalog::load(
        verlet::operations::plugins::LocalPluginCatalogConfig::new(&registry_root),
    )
    .await
    .unwrap();
    let config = verlet::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(catalog.operation_registry())
        .with_workspace_vfs(catalog.vfs());
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute(
            "mkdir -p /workspace/shared \
             && printf 'hello from bash workspace\n' > /workspace/shared/marker.txt \
             && printf /workspace/shared/marker.txt | verlet run workspace-reader cat",
        )
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "hello from bash workspace\n");
}

#[tokio::test]
async fn shared_workspace_vfs_preserves_catalog_mounts() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let module_path = repo.join("tests/fixtures/wasm-vfs-tools");
    let temp = temp_dir("path3-shared-catalog-mount");
    let registry_root = temp.join("plugins");
    let workspace = temp.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("input.txt"), "catalog mount survives\n").unwrap();
    publish_workspace_reader(&registry_root, &module_path).await;

    let catalog = verlet::operations::plugins::LocalPluginCatalog::load(
        verlet::operations::plugins::LocalPluginCatalogConfig::new(&registry_root).with_mount(
            verlet::operations::plugins::PluginMount::host_read_only("/workspace", &workspace),
        ),
    )
    .await
    .unwrap();
    let config = verlet::capabilities::execution::VirtualBashRuntimeConfig::default()
        .with_operation_registry(catalog.operation_registry())
        .with_workspace_vfs(catalog.vfs());
    let mut harness = verlet_vbash::harness::BashkitExecutionHarness::new(config)
        .await
        .unwrap();

    let output = harness
        .execute("printf /workspace/input.txt | verlet run workspace-reader cat")
        .await
        .unwrap();

    assert!(output.success(), "{output:?}");
    assert_eq!(output.stdout, "catalog mount survives\n");
}

async fn publish_workspace_reader(registry_root: &std::path::Path, module_path: &std::path::Path) {
    let build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(module_path),
    )
    .unwrap();
    verlet_operations::operation_store::LocalOperationRegistry::new(registry_root)
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: "workspace-reader".to_string(),
                artifact_path: build.artifact_path,
                source: verlet_operations::operation_store::PublishedOperationSource::Rust {
                    module_path: module_path.to_path_buf(),
                    release: true,
                },
                interface: None,
                capability_grants: std::collections::BTreeSet::from([
                    verlet_wasm::runner::FS_WRITE_CAPABILITY.to_string(),
                ]),
                metadata: std::collections::BTreeMap::new(),
            },
        )
        .await
        .unwrap();
}

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
