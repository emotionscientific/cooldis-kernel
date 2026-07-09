use cooldis::{
    AgentManifestCouplingBudget, AgentManifestCouplingQuota, BoundCoupling, BoundCouplingFunction,
    BoundCouplingSelector, BoundCouplingSet, BoundCouplingSink, CouplingRole, CouplingScheduler,
    EventKind, EventStore, EventStreamId, LocalOperationRegistry, NewEventRecord,
    PublishOperationRequest, PublishedOperationSource, RustWasmBuildOptions, SqliteSessionStore,
    ThreadCoordinates, WasmCouplingExecutor, build_rust_wasm_module,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use uuid::Uuid;

#[tokio::test]
async fn coupling_replay_reports_counter_proposals_without_mutating_journal() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let example_module = repo.join("../../examples/wasm-counter-coupling");
    let root = temp_dir("coupling-replay-counter");
    let registry_root = root.join("operations");
    let recording_path = root.join("session_history.sqlite3");
    let coupling_path = root.join("coupling.json");
    let quota_coupling_path = root.join("quota-coupling.json");

    let build = build_rust_wasm_module(RustWasmBuildOptions::new(&example_module)).unwrap();
    let operation = LocalOperationRegistry::new(&registry_root)
        .publish_artifact(PublishOperationRequest {
            name: "counter".to_string(),
            artifact_path: build.artifact_path.clone(),
            source: PublishedOperationSource::Rust {
                module_path: example_module,
                release: true,
            },
            interface: None,
            capability_grants: BTreeSet::new(),
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap();
    let artifact_ref = format!(
        "op://counter/fold_counter@sha256:{}",
        operation.active_artifact_hash
    );

    let coordinates = ThreadCoordinates::new("tenant", "user", "counter-session");
    let store = SqliteSessionStore::open(&recording_path).unwrap();
    let coupling = counter_coupling("org.example.counter", &artifact_ref, None);
    fs::write(
        &coupling_path,
        serde_json::to_vec_pretty(&BoundCouplingSet::new(
            "snapshot-counter",
            vec![coupling.clone()],
        ))
        .unwrap(),
    )
    .unwrap();
    let executor = WasmCouplingExecutor::new(&registry_root);
    let scheduler = CouplingScheduler::new(&store, &executor);
    for index in 1..=3 {
        let appended = store
            .append_events(
                &EventStreamId::for_thread(&coordinates),
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({"turn_id": format!("turn-{index}")}),
                )],
            )
            .await
            .unwrap();
        scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-counter", vec![coupling.clone()]),
                appended,
            )
            .await
            .unwrap();
    }
    let live_derived = store
        .read_events(
            &EventStreamId::new(format!("derived:counter:{}", coordinates.thread_id)),
            None,
        )
        .await
        .unwrap();
    assert_eq!(live_derived.len(), 1);
    assert_eq!(live_derived[0].payload["count"], json!(3));
    drop(store);
    let before = fs::read(&recording_path).unwrap();

    let replay = run_cooldis([
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
    let after = fs::read(&recording_path).unwrap();
    assert_eq!(after, before, "replay must not mutate the source journal");

    let replay: Value = serde_json::from_str(&replay).unwrap();
    assert_eq!(replay["mode"], "replay");
    assert_eq!(replay["dryRun"], true);
    assert_eq!(replay["runs"].as_array().unwrap().len(), 3);
    assert_eq!(replay["proposalEvents"].as_array().unwrap().len(), 1);
    assert_eq!(
        replay["proposalEvents"][0]["stream"],
        json!("derived:counter")
    );
    assert_eq!(
        replay["proposalEvents"][0]["kind"],
        json!("placement.decision")
    );
    assert_eq!(
        replay["proposalEvents"][0]["payload"],
        live_derived[0].payload
    );

    let quota_coupling = counter_coupling(
        "org.example.counter.quota",
        &artifact_ref,
        Some(AgentManifestCouplingQuota {
            per_turn: None,
            per_thread: Some(1),
        }),
    );
    fs::write(
        &quota_coupling_path,
        serde_json::to_vec_pretty(&BoundCouplingSet::new(
            "snapshot-quota",
            vec![quota_coupling],
        ))
        .unwrap(),
    )
    .unwrap();
    let quota_replay = run_cooldis([
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
    let quota_replay: Value = serde_json::from_str(&quota_replay).unwrap();
    let blocked = quota_replay["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|run| run["blocked"].as_bool() == Some(true))
        .collect::<Vec<_>>();
    assert_eq!(blocked.len(), 2);
    assert!(blocked.iter().all(|run| {
        run["status"] == json!("blocked") && run["reason"] == json!("quota_exhausted")
    }));
}

fn counter_coupling(
    id: &str,
    artifact_ref: &str,
    quota: Option<AgentManifestCouplingQuota>,
) -> BoundCoupling {
    let hash = artifact_ref.rsplit_once("@sha256:").unwrap().1;
    BoundCoupling {
        id: id.to_string(),
        role: CouplingRole::Projection,
        trigger_kind: EventKind::TurnCompleted,
        trigger_match: BTreeMap::new(),
        trigger_quota: quota.unwrap_or_default(),
        source_selectors: vec![BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![EventKind::TurnCompleted],
            scope: None,
            since: None,
        }],
        sink: BoundCouplingSink {
            stream: "derived:counter".to_string(),
            kinds: vec![EventKind::PlacementDecision],
        },
        function_ref: artifact_ref.to_string(),
        function: BoundCouplingFunction {
            name: "counter".to_string(),
            artifact_hash: hash.to_string(),
            operation_name: Some("fold_counter".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:counter".to_string(),
        ],
        budget: AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: json!({
            "every": 3,
            "sink_stream": "derived:counter",
            "sink_kind": "placement.decision",
        }),
        config_hash: "sha256:test-counter".to_string(),
    }
}

fn run_cooldis<const N: usize>(args: [&str; N]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_cooldis"))
        .args(args)
        .output()
        .expect("failed to run cooldis cli");
    assert!(
        output.status.success(),
        "cooldis cli failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("cooldis output should be utf8")
}

fn temp_dir(prefix: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()));
    fs::create_dir_all(&path).unwrap();
    path
}
