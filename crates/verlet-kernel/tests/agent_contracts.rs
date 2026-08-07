mod support;

const RELEASE_VERIFIER_CONTRACT: &str = r#"---
name: release-verifier
kind: thread
version: 0
---

### Requires

- `branch`: git branch or ref to inspect
- `checks`: required verification commands as JSON array

### Ensures

- `verdict`: ship, hold, or needs-review
- `report`: concise evidence summary

### Tools

- `cli`: printf
- `verlet`: spawn_thread

### Effects

- `artifact.report`: host-allocated text artifact

### Runtime

- `model`: virtual-bash
- `propagator`: llm
- `budget`: bounded

### Instructions

Run the declared checks and print the verdict.
"#;

#[tokio::test]
async fn kernel_declaration_runs_thread_contract_and_records_child_history() {
    let host = verlet::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        verlet::capabilities::execution::VirtualBashRuntimeFactory::default(),
    ));
    let root = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();

    let mut declaration = verlet_agent::contracts::ThreadDeclaration::new(
        verlet_agent::contracts::ThreadContractReference::inline_markdown(
            RELEASE_VERIFIER_CONTRACT,
        ),
        verlet_agent::contracts::ThreadInitialTurn::user(
            "printf 'verdict=ship\\nreport=direct-run\\n'",
        ),
    );
    declaration.inputs = serde_json::json!({
        "branch": "main",
        "checks": ["printf ok"],
    });
    declaration.propagator = Some(verlet_agent::contracts::ThreadPropagatorSelection::named(
        "llm",
        "virtual-bash",
    ));
    declaration
        .metadata
        .insert("task_name".to_string(), "release-verifier".to_string());

    let handle = host
        .kernel_control()
        .declare_thread(root.context(), declaration)
        .await
        .unwrap();

    assert_eq!(handle.kind, verlet_agent::contracts::THREAD_HANDLE_KIND);
    assert_eq!(handle.propagator.kind, "llm");
    assert_eq!(handle.propagator.name.as_deref(), Some("virtual-bash"));
    assert!(handle.contract_hash.starts_with("sha256:"));
    assert!(handle.receipts.compile.starts_with("sha256:"));
    assert!(handle.receipts.spawn.starts_with("sha256:"));

    let wait = host
        .kernel_control()
        .wait_thread(root.context(), handle.thread_id, Some(1_000))
        .await
        .unwrap();
    assert!(!wait.timed_out);
    let latest_output = wait.latest_output.expect("thread contract child output");
    assert!(latest_output.contains("verdict=ship"));

    let child = host.get_thread(handle.thread_id).await.unwrap();
    assert_eq!(
        child.context().parent_thread_id,
        Some(root.context().coordinates.thread_id)
    );

    let lifecycle = child.lifecycle_record().await;
    assert_eq!(
        lifecycle
            .metadata
            .get("thread_contract_v0")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        lifecycle
            .metadata
            .get("thread_contract_name")
            .map(String::as_str),
        Some("release-verifier")
    );
    assert_eq!(
        lifecycle
            .metadata
            .get("thread_contract_hash")
            .map(String::as_str),
        Some(handle.contract_hash.as_str())
    );
    assert_eq!(
        lifecycle
            .metadata
            .get("thread_propagator_kind")
            .map(String::as_str),
        Some("llm")
    );
    assert_eq!(
        lifecycle
            .metadata
            .get("thread_propagator_name")
            .map(String::as_str),
        Some("virtual-bash")
    );
    assert_eq!(
        lifecycle
            .metadata
            .get("agent_contract_v0")
            .map(String::as_str),
        Some("true")
    );

    let context = child.session_context().await.unwrap();
    let messages = context
        .messages
        .iter()
        .map(crate::support::event_trace::text_from_message)
        .collect::<Vec<_>>();
    assert_eq!(messages[0], "printf 'verdict=ship\\nreport=direct-run\\n'");
    assert!(messages[1].contains("verdict=ship"));

    host.shutdown_all().await.unwrap();
}
