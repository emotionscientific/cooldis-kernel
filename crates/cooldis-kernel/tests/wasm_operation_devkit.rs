mod support;

use cooldis::{
    CanonicalProviderRuntimeConfig, CanonicalProviderRuntimeFactory, LocalOperationRegistry,
    LocalPluginCatalog, LocalPluginCatalogConfig, ProviderApi, PublishOperationRequest,
    PublishedOperationSource, RuntimeEventKind, RuntimeHost, RustWasmBuildOptions,
    ThreadCoordinates, ThreadTopology, build_rust_wasm_module,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use support::{ScriptedProviderClient, collect_until_output, response_text, response_tool_call};
use uuid::Uuid;

#[tokio::test]
async fn agent_authored_wasm_operation_is_published_and_provider_invoked() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let authored_module = repo.join("tests/fixtures/wasm-csv-profile");
    let temp = temp_dir("wasm-devkit");
    let registry_root = temp.join("operations");

    let build = build_rust_wasm_module(RustWasmBuildOptions::new(&authored_module)).unwrap();
    let record = LocalOperationRegistry::new(&registry_root)
        .publish_artifact(PublishOperationRequest {
            name: "data".to_string(),
            artifact_path: build.artifact_path,
            source: PublishedOperationSource::Rust {
                module_path: authored_module,
                release: true,
            },
            interface: None,
            capability_grants: BTreeSet::new(),
            metadata: BTreeMap::from([
                ("devkit.example".to_string(), json!("csv-profile")),
                ("authored_by".to_string(), json!("subagent-fixture")),
            ]),
        })
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
        LocalOperationRegistry::new(&registry_root)
            .blobs()
            .artifact_path(&record.active_artifact_hash)
            .unwrap()
            .exists()
    );

    let catalog = LocalPluginCatalog::load(LocalPluginCatalogConfig::new(&registry_root))
        .await
        .unwrap();
    assert_eq!(catalog.operations().len(), 1);

    let csv = "name,score,risk\nAda,10,low\nLinus,8,\nGrace,13,high\n";
    let direct = catalog
        .operation_registry()
        .invoke_bytes(
            "data",
            "csv_profile",
            serde_json::to_vec(&json!({"csv": csv, "has_header": true})).unwrap(),
        )
        .await
        .unwrap();
    let direct_json: Value = serde_json::from_slice(&direct.output).unwrap();
    assert_eq!(direct_json["rows"], 3);
    assert_eq!(direct_json["columns"][1]["name"], "score");
    assert_eq!(direct_json["columns"][1]["numeric_count"], 3);
    assert_eq!(direct_json["columns"][1]["mean"], 31.0 / 3.0);
    assert_eq!(direct_json["columns"][2]["empty"], 1);

    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call(
            "data_csv_profile",
            json!({
                "csv": csv,
                "has_header": true
            }),
        ),
        response_text("profiled: score mean 10.333333333333334 and risk has 1 empty cell"),
    ]));
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let factory = CanonicalProviderRuntimeFactory::new(config, client.clone())
        .with_operation_registry(catalog.operation_registry());
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "devkit_session"),
            ThreadTopology::root(),
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
    let trace = collect_until_output(
        &mut events,
        "profiled: score mean 10.333333333333334 and risk has 1 empty cell",
    )
    .await;

    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolCallStarted { name, .. } if name == "data_csv_profile"
    )));
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolCallResult {
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
        .map(support::text_from_message)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(second_request_text.contains("\"numeric_count\":3"));
    assert!(second_request_text.contains("\"empty\":1"));

    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();
}

fn temp_dir(prefix: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()));
    fs::create_dir_all(&path).unwrap();
    path
}
