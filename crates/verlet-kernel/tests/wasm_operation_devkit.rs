#[path = "support/test_mount.rs"]
mod support;

#[tokio::test]
async fn agent_authored_wasm_operation_is_published_and_provider_invoked() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let authored_module = repo.join("tests/fixtures/wasm-csv-profile");
    let temp = temp_dir("wasm-devkit");
    let registry_root = temp.join("operations");

    let build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(&authored_module),
    )
    .unwrap();
    let record = verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: "data".to_string(),
                artifact_path: build.artifact_path,
                source: verlet_operations::operation_store::PublishedOperationSource::Rust {
                    module_path: authored_module,
                    release: true,
                },
                interface: None,
                capability_grants: std::collections::BTreeSet::new(),
                metadata: std::collections::BTreeMap::from([
                    (
                        "devkit.example".to_string(),
                        serde_json::json!("csv-profile"),
                    ),
                    (
                        "authored_by".to_string(),
                        serde_json::json!("subagent-fixture"),
                    ),
                ]),
            },
        )
        .await
        .unwrap();

    assert_eq!(record.name, "data");
    assert_eq!(record.manifest.operations[0].name, "csv_profile");
    assert!(
        record
            .projections
            .operations
            .iter()
            .any(|projection| projection.llm_tool.name == "data_csv_profile")
    );
    assert!(
        verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
            .blobs()
            .artifact_path(&record.active_artifact_hash)
            .unwrap()
            .exists()
    );

    let catalog = verlet::operations::plugins::LocalPluginCatalog::load(
        verlet::operations::plugins::LocalPluginCatalogConfig::new(&registry_root),
    )
    .await
    .unwrap();
    assert_eq!(catalog.operations().len(), 1);

    let csv = "name,score,risk\nAda,10,low\nLinus,8,\nGrace,13,high\n";
    let direct = catalog
        .operation_registry()
        .invoke_bytes(
            "data",
            "csv_profile",
            serde_json::to_vec(&serde_json::json!({"csv": csv, "has_header": true})).unwrap(),
        )
        .await
        .unwrap();
    let direct_json: serde_json::Value = serde_json::from_slice(&direct.output).unwrap();
    assert_eq!(direct_json["rows"], 3);
    assert_eq!(direct_json["columns"][1]["name"], "score");
    assert_eq!(direct_json["columns"][1]["numeric_count"], 3);
    assert_eq!(direct_json["columns"][1]["mean"], 31.0 / 3.0);
    assert_eq!(direct_json["columns"][2]["empty"], 1);

    let client = std::sync::Arc::new(
        crate::support::scripted_provider::ScriptedProviderClient::with_responses(vec![
            crate::support::scripted_provider::response_tool_call(
                "data_csv_profile",
                serde_json::json!({
                    "csv": csv,
                    "has_header": true
                }),
            ),
            crate::support::scripted_provider::response_text(
                "profiled: score mean 10.333333333333334 and risk has 1 empty cell",
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
                "devkit_session",
            ),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-devkit-profile",
        "Use the installed data_csv_profile operation to profile the CSV and identify the risky column.",
    )
    .await
    .unwrap();
    let trace = crate::support::event_trace::collect_until_output(
        &mut events,
        "profiled: score mean 10.333333333333334 and risk has 1 empty cell",
    )
    .await;

    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted { name, .. } if name == "data_csv_profile"
    )));
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
            output,
            success: true,
            ..
        } if output.contains("\"rows\":3") && output.contains("\"risk\"")
    )));

    let requests = client.requests();
    assert!(
        requests[0]
            .tools
            .iter()
            .any(|tool| tool.name == "data_csv_profile")
    );
    let second_request_text = requests[1]
        .messages
        .iter()
        .map(crate::support::event_trace::text_from_message)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(second_request_text.contains("\"numeric_count\":3"));
    assert!(second_request_text.contains("\"empty\":1"));

    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn example_counter_coupling_builds_and_emits_discharge() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let example_module = repo.join("../../examples/wasm-counter-coupling");
    let build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(&example_module),
    )
    .unwrap();
    let factory = verlet::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::path(
            build.artifact_path,
        )),
    )
    .unwrap();
    let invocation = counter_invocation();

    let output = factory
        .invoke_operation_bytes("fold_counter", serde_json::to_vec(&invocation).unwrap())
        .await
        .unwrap();
    let discharge: serde_json::Value = serde_json::from_slice(&output.output).unwrap();

    assert_eq!(discharge["abi"], "cooldis.coupling.discharge/0.1");
    assert_eq!(discharge["events"][0]["stream"], "derived:counter");
    assert_eq!(discharge["events"][0]["kind"], "placement.decision");
    assert_eq!(discharge["events"][0]["payload"]["count"], 3);
}

#[tokio::test]
async fn macro_counter_coupling_matches_handrolled_envelope_bytes() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let macro_module = repo.join("../../examples/wasm-counter-coupling");
    let handrolled_module = repo.join("tests/fixtures/wasm-counter-coupling-handrolled");
    let macro_build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(&macro_module),
    )
    .unwrap();
    let handrolled_build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(&handrolled_module),
    )
    .unwrap();
    let input = serde_json::to_vec(&counter_invocation()).unwrap();
    let macro_factory = verlet::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::path(
            macro_build.artifact_path,
        )),
    )
    .unwrap();
    let handrolled_factory = verlet::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::path(
            handrolled_build.artifact_path,
        )),
    )
    .unwrap();

    let macro_output = macro_factory
        .invoke_operation_bytes("fold_counter", input.clone())
        .await
        .unwrap();
    let handrolled_output = handrolled_factory
        .invoke_operation_bytes("fold_counter", input)
        .await
        .unwrap();

    assert_eq!(macro_output.output, handrolled_output.output);
}

fn counter_invocation() -> serde_json::Value {
    serde_json::json!({
        "abi": "cooldis.coupling.invocation/0.1",
        "trigger_event": {
            "id": "event-3",
            "stream_id": "thread:session",
            "sequence": 3,
            "kind": "turn.completed",
            "origin": "witnessed",
            "payload": {}
        },
        "selected_events": [
            {"id": "event-1", "stream_id": "thread:session", "sequence": 1, "kind": "turn.completed", "origin": "witnessed", "payload": {}},
            {"id": "event-2", "stream_id": "thread:session", "sequence": 2, "kind": "turn.completed", "origin": "witnessed", "payload": {}},
            {"id": "event-3", "stream_id": "thread:session", "sequence": 3, "kind": "turn.completed", "origin": "witnessed", "payload": {}}
        ],
        "config": {
            "every": 3,
            "sink_stream": "derived:counter",
            "sink_kind": "placement.decision"
        },
        "invocation_meta": {
            "coupling_id": "org.example.counter",
            "thread_id": "session",
            "depth": 0
        }
    })
}

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
