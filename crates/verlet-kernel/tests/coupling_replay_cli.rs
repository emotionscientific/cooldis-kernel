use verlet_history::EventStore as _;

#[tokio::test]
async fn coupling_replay_reports_counter_proposals_without_mutating_journal() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let example_module = repo.join("../../examples/wasm-counter-coupling");
    let root = temp_dir("coupling-replay-counter");
    let registry_root = root.join("operations");
    let recording_path = root.join("session_history.turso");
    let coupling_path = root.join("coupling.json");
    let quota_coupling_path = root.join("quota-coupling.json");

    let build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(&example_module),
    )
    .unwrap();
    let operation = verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root)
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: "counter".to_string(),
                artifact_path: build.artifact_path.clone(),
                source: verlet_operations::operation_store::PublishedOperationSource::Rust {
                    module_path: example_module,
                    release: true,
                },
                interface: None,
                capability_grants: std::collections::BTreeSet::new(),
                metadata: std::collections::BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    let artifact_ref = format!(
        "op://counter/fold_counter@sha256:{}",
        operation.active_artifact_hash
    );

    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "counter-session");
    let store = verlet_history_sqlite::SqliteSessionStore::open(&recording_path)
        .await
        .unwrap();
    let coupling = counter_coupling("org.example.counter", &artifact_ref, None);
    std::fs::write(
        &coupling_path,
        serde_json::to_vec_pretty(&verlet::agent::manifest_bind::BoundCouplingSet::new(
            "snapshot-counter",
            vec![coupling.clone()],
        ))
        .unwrap(),
    )
    .unwrap();
    let executor = verlet::kernel::wasm_couplings::WasmCouplingExecutor::new(&registry_root);
    let scheduler = verlet::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
    for index in 1..=3 {
        let appended = store
            .append_events(
                &verlet_history::EventStreamId::for_thread(&coordinates),
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnCompleted,
                    serde_json::json!({"turn_id": format!("turn-{index}")}),
                )],
            )
            .await
            .unwrap();
        scheduler
            .run_batch(
                &verlet::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-counter",
                    vec![coupling.clone()],
                ),
                appended,
            )
            .await
            .unwrap();
    }
    let live_derived = store
        .read_events(
            &verlet_history::EventStreamId::new(format!(
                "derived:counter:{}",
                coordinates.thread_id
            )),
            None,
        )
        .await
        .unwrap();
    assert_eq!(live_derived.len(), 1);
    assert_eq!(live_derived[0].payload["count"], serde_json::json!(3));
    drop(store);
    let before = std::fs::read(&recording_path).unwrap();

    let replay = run_verlet([
        "coupling",
        "run",
        "--replay",
        "--artifact",
        build.artifact_path.to_str().unwrap(),
        "--coupling-file",
        coupling_path.to_str().unwrap(),
        "--thread-id",
        &coordinates.thread_id.to_string(),
        "--journal",
        recording_path.to_str().unwrap(),
        "--json",
    ]);
    let after = std::fs::read(&recording_path).unwrap();
    assert_eq!(after, before, "replay must not mutate the source journal");

    let replay: serde_json::Value = serde_json::from_str(&replay).unwrap();
    assert_eq!(replay["mode"], "replay");
    assert_eq!(replay["dryRun"], true);
    assert_eq!(replay["runs"].as_array().unwrap().len(), 3);
    assert_eq!(replay["proposalEvents"].as_array().unwrap().len(), 1);
    assert_eq!(
        replay["proposalEvents"][0]["stream"],
        serde_json::json!("derived:counter")
    );
    assert_eq!(
        replay["proposalEvents"][0]["kind"],
        serde_json::json!("placement.decision")
    );
    assert_eq!(
        replay["proposalEvents"][0]["payload"],
        live_derived[0].payload
    );

    let quota_coupling = counter_coupling(
        "org.example.counter.quota",
        &artifact_ref,
        Some(verlet_agent::manifest_schema::AgentManifestCouplingQuota {
            per_turn: None,
            per_thread: Some(1),
        }),
    );
    std::fs::write(
        &quota_coupling_path,
        serde_json::to_vec_pretty(&verlet::agent::manifest_bind::BoundCouplingSet::new(
            "snapshot-quota",
            vec![quota_coupling],
        ))
        .unwrap(),
    )
    .unwrap();
    let quota_replay = run_verlet([
        "coupling",
        "run",
        "--replay",
        "--artifact",
        &artifact_ref,
        "--registry-root",
        registry_root.to_str().unwrap(),
        "--coupling-file",
        quota_coupling_path.to_str().unwrap(),
        "--thread-id",
        &coordinates.thread_id.to_string(),
        "--journal",
        recording_path.to_str().unwrap(),
        "--json",
    ]);
    let quota_replay: serde_json::Value = serde_json::from_str(&quota_replay).unwrap();
    let blocked = quota_replay["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|run| run["blocked"].as_bool() == Some(true))
        .collect::<Vec<_>>();
    assert_eq!(blocked.len(), 2);
    assert!(blocked.iter().all(|run| {
        run["status"] == serde_json::json!("blocked")
            && run["reason"] == serde_json::json!("quota_exhausted")
    }));
}

#[tokio::test]
async fn coupling_replay_guides_user_when_daemon_holds_journal() {
    let repo = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let example_module = repo.join("../../examples/wasm-counter-coupling");
    let root = temp_dir("coupling-replay-held-journal");
    let journal_path = root.join("session_history.turso");
    let coupling_path = root.join("coupling.json");

    let build = verlet::operations::operation_builder::build_rust_wasm_module(
        verlet::operations::operation_builder::RustWasmBuildOptions::new(&example_module),
    )
    .unwrap();
    let coupling = counter_coupling(
        "org.example.counter",
        "op://counter/fold_counter@sha256:0000000000000000000000000000000000000000000000000000000000000000",
        None,
    );
    std::fs::write(
        &coupling_path,
        serde_json::to_vec_pretty(&verlet::agent::manifest_bind::BoundCouplingSet::new(
            "snapshot",
            vec![coupling],
        ))
        .unwrap(),
    )
    .unwrap();

    let held_store = verlet_history_sqlite::SqliteSessionStore::open(&journal_path)
        .await
        .unwrap();
    let thread_id =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "held-journal")
            .thread_id;
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_verlet"))
        .args([
            "coupling",
            "run",
            "--replay",
            "--artifact",
            build.artifact_path.to_str().unwrap(),
            "--coupling-file",
            coupling_path.to_str().unwrap(),
            "--thread-id",
            &thread_id.to_string(),
            "--journal",
            journal_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run verlet cli");
    drop(held_store);

    assert!(
        !output.status.success(),
        "contended replay unexpectedly succeeded"
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(
            "another process holds this database (most likely a running Verlet instance); stop that instance and retry"
        ),
        "missing lock guidance in stderr:\n{stderr}"
    );
}

fn counter_coupling(
    id: &str,
    artifact_ref: &str,
    quota: Option<verlet_agent::manifest_schema::AgentManifestCouplingQuota>,
) -> verlet::agent::manifest_bind::BoundCoupling {
    let hash = artifact_ref.rsplit_once("@sha256:").unwrap().1;
    verlet::agent::manifest_bind::BoundCoupling {
        id: id.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Projection,
        trigger_kind: verlet_history::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: quota.unwrap_or_default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet_history::EventKind::TurnCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "derived:counter".to_string(),
            kinds: vec![verlet_history::EventKind::PlacementDecision],
        },
        function_ref: artifact_ref.to_string(),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "counter".to_string(),
            artifact_hash: hash.to_string(),
            operation_name: Some("fold_counter".to_string()),
        },
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "every": 3,
            "sink_stream": "derived:counter",
            "sink_kind": "placement.decision",
        }),
        config_hash: "sha256:test-counter".to_string(),
    }
}

fn run_verlet<const N: usize>(args: [&str; N]) -> String {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_verlet"))
        .args(args)
        .output()
        .expect("failed to run verlet cli");
    assert!(
        output.status.success(),
        "verlet cli failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("verlet output should be utf8")
}

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
