use super::*;

fn temp_root(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("cooldis-agent-{label}-{}", Uuid::now_v7()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn hash() -> &'static str {
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

fn alternate_hash() -> &'static str {
    "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
}

fn manifest_source(name: &str, version: &str, mutable_resource: bool) -> String {
    manifest_source_with_tool(
        name,
        version,
        &format!("op://tailcat@sha256:{}", hash()),
        &[],
        mutable_resource,
    )
}

fn manifest_source_with_tool(
    name: &str,
    version: &str,
    operation_ref: &str,
    grants: &[&str],
    mutable_resource: bool,
) -> String {
    let resource_ref = if mutable_resource {
        "resource://guide"
    } else {
        "resource://guide@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    };
    let grants = if grants.is_empty() {
        String::new()
    } else {
        format!(
            "grants = [{}]\n",
            grants
                .iter()
                .map(|grant| format!("{grant:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        r#"
[agent]
name = "{name}"
version = "{version}"
description = "Checks a release branch."
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"

[[tools]]
type = "bash_tool"
id = "tailcat"
command = "tailcat"
operation_ref = "{operation_ref}"
{grants}

[[resources]]
name = "guide"
kind = "blob"
ref = "{resource_ref}"
"#
    )
}

fn seed_operation_record(
    root: &Path,
    name: &str,
    artifact_hash: &str,
    operations: &[(&str, &[&str])],
) {
    let registry = crate::LocalOperationRegistry::new(root);
    let manifest = crate::WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: operations
            .iter()
            .enumerate()
            .map(|(index, (operation_name, required_capabilities))| {
                crate::WasmOperationDefinition {
                    id: (index + 1) as u32,
                    name: (*operation_name).to_string(),
                    input: crate::WasmOperationValueKind::Text,
                    output: crate::WasmOperationValueKind::Text,
                    events: Default::default(),
                    mode: Default::default(),
                    required_capabilities: required_capabilities
                        .iter()
                        .map(|capability| (*capability).to_string())
                        .collect(),
                }
            })
            .collect(),
    };
    let capability_grants = operations
        .iter()
        .flat_map(|(_, required_capabilities)| {
            required_capabilities
                .iter()
                .map(|capability| (*capability).to_string())
        })
        .collect::<std::collections::BTreeSet<_>>();
    let registered = crate::RegisteredOperation {
        name: name.to_string(),
        manifest: manifest.clone(),
        capability_grants: capability_grants.clone(),
        metadata: Default::default(),
    };
    let record = crate::PublishedOperationRecord {
        schema_version: 1,
        name: name.to_string(),
        active_artifact_hash: artifact_hash.to_string(),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants,
        metadata: Default::default(),
        source: crate::PublishedOperationSource::Kernel {
            package: "test".to_string(),
        },
        build: crate::PublishedOperationBuild {
            artifact_path: PathBuf::from("<test>"),
            published_at_ms: now_ms(),
        },
    };
    record.validate().unwrap();
    write_json_atomically(
        &registry.record_path(name).unwrap(),
        format!("operation record {name:?}"),
        &record,
    )
    .unwrap();
    write_json_atomically(
        &registry.version_record_path(name, artifact_hash).unwrap(),
        format!("operation version record {name:?}@{artifact_hash}"),
        &record,
    )
    .unwrap();
}

fn seed_tailcat_operation_root(label: &str) -> PathBuf {
    let operation_root = temp_root(label);
    seed_operation_record(&operation_root, "tailcat", hash(), &[("cat", &[])]);
    operation_root
}

#[test]
fn plan_records_resolved_and_unresolved_refs_and_publish_rejects_unresolved() {
    let plan =
        AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", true)).unwrap();

    assert_eq!(plan.resolved_refs.len(), 2);
    assert_eq!(
        plan.resolved_refs[0].status,
        AgentManifestRefStatus::Resolved
    );
    let expected_hash = format!("sha256:{}", hash());
    assert_eq!(
        plan.resolved_refs[0].content_hash.as_deref(),
        Some(expected_hash.as_str())
    );
    assert_eq!(
        plan.resolved_refs[1].status,
        AgentManifestRefStatus::UnresolvedOffline
    );

    let err = LocalAgentRegistry::new(temp_root("unresolved"))
        .publish_plan(plan)
        .unwrap_err();
    assert!(err.to_string().contains("unresolved artifact ref"));
}

#[test]
fn publish_verifies_operation_ref_exists_in_registry() {
    let agent_root = temp_root("verified-agent");
    let operation_root = temp_root("verified-operations");
    seed_operation_record(&operation_root, "tailcat", hash(), &[("cat", &[])]);
    let record = LocalAgentRegistry::new(agent_root)
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap();

    assert_eq!(record.name, "release-verifier");
    assert_eq!(
        record.resolved_refs[0].status,
        AgentManifestRefStatus::Resolved
    );
}

#[test]
fn publish_rejects_missing_operation_record() {
    let operation_root = temp_root("missing-operation-record");
    let err = LocalAgentRegistry::new(temp_root("missing-operation-agent"))
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("tool \"tailcat\""));
    assert!(text.contains(&format!("op://tailcat@sha256:{}", hash())));
    assert!(text.contains("seed the operation registry"));
}

#[test]
fn publish_rejects_operation_ref_hash_that_is_not_a_published_version() {
    let operation_root = temp_root("wrong-operation-version");
    seed_operation_record(
        &operation_root,
        "tailcat",
        alternate_hash(),
        &[("cat", &[])],
    );

    let err = LocalAgentRegistry::new(temp_root("wrong-operation-agent"))
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap_err();

    let text = err.to_string();
    assert!(text.contains(&format!("op://tailcat@sha256:{}", hash())));
    assert!(text.contains("not a published version"));
    assert!(text.contains("replace the ref with a hash from the registry"));
}

#[test]
fn publish_rejects_two_segment_ref_for_undeclared_operation() {
    let operation_root = temp_root("unknown-operation-segment");
    seed_operation_record(&operation_root, "tailcat", hash(), &[("cat", &[])]);
    let source = manifest_source_with_tool(
        "release-verifier",
        "1.0.0",
        &format!("op://tailcat/export@sha256:{}", hash()),
        &[],
        false,
    );

    let err = LocalAgentRegistry::new(temp_root("unknown-operation-agent"))
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&source).unwrap(),
            &operation_root,
        )
        .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("op://<record>/<operation>@sha256:<hash>"));
    assert!(text.contains("available operations: cat"));
}

#[test]
fn publish_rejects_single_segment_ref_when_any_operation_grant_is_missing() {
    let operation_root = temp_root("single-grant-shortfall");
    seed_operation_record(
        &operation_root,
        "tailcat",
        hash(),
        &[
            ("cat", &["fs.read:/workspace"]),
            ("tail", &["net:https://example.com"]),
        ],
    );
    let source = manifest_source_with_tool(
        "release-verifier",
        "1.0.0",
        &format!("op://tailcat@sha256:{}", hash()),
        &["fs.read:/workspace"],
        false,
    );

    let err = LocalAgentRegistry::new(temp_root("single-grant-agent"))
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&source).unwrap(),
            &operation_root,
        )
        .unwrap_err();

    assert!(err.to_string().contains("tail:net:https://example.com"));
}

#[test]
fn publish_rejects_two_segment_ref_when_selected_operation_grant_is_missing() {
    let operation_root = temp_root("selected-grant-shortfall");
    seed_operation_record(
        &operation_root,
        "tailcat",
        hash(),
        &[
            ("cat", &["fs.read:/workspace"]),
            ("tail", &["net:https://example.com"]),
        ],
    );
    let source = manifest_source_with_tool(
        "release-verifier",
        "1.0.0",
        &format!("op://tailcat/tail@sha256:{}", hash()),
        &[],
        false,
    );

    let err = LocalAgentRegistry::new(temp_root("selected-grant-agent"))
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&source).unwrap(),
            &operation_root,
        )
        .unwrap_err();

    assert!(err.to_string().contains("tail:net:https://example.com"));
}

#[test]
fn content_addressed_refs_must_end_after_sha256_digest() {
    let source = manifest_source("release-verifier", "1.0.0", false).replace(
        &format!("operation_ref = \"op://tailcat@sha256:{}\"", hash()),
        &format!("operation_ref = \"op://tailcat@sha256:{}extra\"", hash()),
    );
    let plan = AgentPublishPlan::from_source(&source).unwrap();

    assert_eq!(
        plan.resolved_refs[0].status,
        AgentManifestRefStatus::UnresolvedOffline
    );
    assert!(plan.resolved_refs[0].content_hash.is_none());

    let err = LocalAgentRegistry::new(temp_root("trailing-junk"))
        .publish_plan(plan)
        .unwrap_err();
    assert!(err.to_string().contains("unresolved artifact ref"));
}

#[test]
fn resolved_ref_validation_checks_recorded_content_hash() {
    let mut record =
        AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
            .unwrap()
            .into_record(now_ms());
    record.resolved_refs[0].content_hash =
        Some("sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string());

    let err = record.validate().unwrap_err();
    assert!(err.to_string().contains("expected sha256"));
}

#[test]
fn publish_maintains_latest_alias_and_load_ref_resolves_alias() {
    let registry = LocalAgentRegistry::new(temp_root("latest"));
    let operation_root = seed_tailcat_operation_root("latest-operations");
    let first = registry
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap();

    assert_eq!(first.resolved_refs.len(), 2);
    assert!(
        registry
            .alias_record_path("release-verifier", "latest")
            .unwrap()
            .exists()
    );
    let (resolved, receipt) = registry
        .resolve_alias("release-verifier", "latest")
        .unwrap();
    assert_eq!(resolved.version, "1.0.0");
    assert_eq!(receipt.alias, "latest");
    assert_eq!(receipt.version, "1.0.0");
    assert_eq!(receipt.manifest_hash, first.manifest_hash);
    assert_eq!(
        registry
            .load_ref("release-verifier@latest")
            .unwrap()
            .version,
        "1.0.0"
    );

    registry
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.1.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap();
    assert_eq!(
        registry
            .load_ref("agent://release-verifier@latest")
            .unwrap()
            .version,
        "1.1.0"
    );
}

#[test]
fn resolve_alias_fails_closed_on_hash_mismatch() {
    let registry = LocalAgentRegistry::new(temp_root("alias-mismatch"));
    let operation_root = seed_tailcat_operation_root("alias-mismatch-operations");
    registry
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap();
    let path = registry
        .alias_record_path("release-verifier", "latest")
        .unwrap();
    let mut alias: AgentAliasRecord = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    alias.manifest_hash =
        "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string();
    fs::write(&path, serde_json::to_vec_pretty(&alias).unwrap()).unwrap();

    let err = registry
        .resolve_alias("release-verifier", "latest")
        .unwrap_err();
    assert!(err.to_string().contains("version record"));
}

#[test]
fn alias_and_version_collisions_are_refused() {
    let registry = LocalAgentRegistry::new(temp_root("alias-collision"));
    let operation_root = seed_tailcat_operation_root("alias-collision-operations");
    registry
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap();

    let latest_plan =
        AgentPublishPlan::from_source(&manifest_source("release-verifier", "latest", false))
            .unwrap();
    let err = registry
        .publish_plan_with_operation_registry(latest_plan, &operation_root)
        .unwrap_err();
    assert!(err.to_string().contains("reserved alias"));

    registry
        .write_alias_record_atomically(&AgentAliasRecord {
            schema_version: AGENT_RECORD_SCHEMA_VERSION,
            name: "release-verifier".to_string(),
            alias: "stable".to_string(),
            version: "1.0.0".to_string(),
            manifest_hash:
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
            updated_at_ms: now_ms(),
        })
        .unwrap();
    let stable_plan =
        AgentPublishPlan::from_source(&manifest_source("release-verifier", "stable", false))
            .unwrap();
    let err = registry
        .publish_plan_with_operation_registry(stable_plan, &operation_root)
        .unwrap_err();
    assert!(err.to_string().contains("collides with an existing alias"));

    let err = registry
        .write_alias_record_atomically(&AgentAliasRecord {
            schema_version: AGENT_RECORD_SCHEMA_VERSION,
            name: "release-verifier".to_string(),
            alias: "1.0.0".to_string(),
            version: "1.0.0".to_string(),
            manifest_hash:
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
            updated_at_ms: now_ms(),
        })
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("collides with an existing version")
    );
}

#[test]
fn publish_preflights_latest_alias_collision_before_writing_records() {
    let registry = LocalAgentRegistry::new(temp_root("legacy-latest-version"));
    let operation_root = seed_tailcat_operation_root("legacy-latest-operations");
    let mut legacy_latest =
        AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
            .unwrap()
            .into_record(now_ms());
    legacy_latest.version = "latest".to_string();
    legacy_latest.ref_uri = agent_ref_uri(None, &legacy_latest.name, &legacy_latest.version);
    registry
        .write_version_record_atomically(&legacy_latest)
        .unwrap();

    let err = registry
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("collides with an existing version")
    );
    assert!(!registry.record_path("release-verifier").unwrap().exists());
}

#[test]
fn legacy_records_without_resolved_refs_still_load() {
    let registry = LocalAgentRegistry::new(temp_root("legacy-record"));
    let operation_root = seed_tailcat_operation_root("legacy-record-operations");
    let record = registry
        .publish_plan_with_operation_registry(
            AgentPublishPlan::from_source(&manifest_source("release-verifier", "1.0.0", false))
                .unwrap(),
            &operation_root,
        )
        .unwrap();
    let path = registry.record_path("release-verifier").unwrap();
    let mut json = serde_json::to_value(&record).unwrap();
    json.as_object_mut().unwrap().remove("resolved_refs");
    fs::write(&path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();

    let loaded = registry.load_record("release-verifier").unwrap();
    assert!(loaded.resolved_refs.is_empty());
}
