use super::*;
use crate::agent::manifest_schema::{
    AgentManifestCouplingSource, AgentManifestCouplingTrigger, KERNEL_ASSEMBLER_STATIC,
};
use crate::{
    AgentManifestCompactionDefaults, AgentManifestRuntimeOverridePolicy, AgentManifestToolProtocol,
    LocalSkillRegistry, PublishSkillPackageRequest, STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
    THREADS_SPAWN_CAPABILITY, ToolDefinition, ToolUniverseDiscovery, WitnessedToolContract,
};
use async_trait::async_trait;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

struct StaticToolUniverseDiscoverer {
    discovery: ToolUniverseDiscovery,
}

#[async_trait]
impl ToolUniverseDiscoverer for StaticToolUniverseDiscoverer {
    async fn discover(&self, _server_ref: &str) -> CooldisResult<ToolUniverseDiscovery> {
        Ok(self.discovery.clone())
    }
}

fn defaults_with_allow(
    allow: Vec<AgentManifestRuntimeOverrideKey>,
) -> AgentManifestRuntimeDefaults {
    AgentManifestRuntimeDefaults {
        default_cwd: "workspace".to_string(),
        streaming: true,
        turn_timeout_ms: Some(1000),
        cancellation_grace_ms: None,
        compaction: AgentManifestCompactionDefaults {
            auto_at_text_bytes: Some(500),
        },
        overrides: AgentManifestRuntimeOverridePolicy { allow },
    }
}

#[test]
fn runtime_overrides_are_denied_by_default() {
    let defaults = AgentManifestRuntimeDefaults::default();
    let overrides = AgentManifestBindOverrides {
        streaming: Some(false),
        ..AgentManifestBindOverrides::default()
    };

    let err = apply_runtime_overrides(&defaults, &overrides).unwrap_err();

    assert!(err.to_string().contains("streaming"));
    assert!(err.to_string().contains("not allowlisted"));
}

#[test]
fn runtime_overrides_apply_when_allowlisted() {
    let defaults = defaults_with_allow(vec![
        AgentManifestRuntimeOverrideKey::DefaultCwd,
        AgentManifestRuntimeOverrideKey::Streaming,
        AgentManifestRuntimeOverrideKey::CompactionAutoAtTextBytes,
    ]);
    let overrides = AgentManifestBindOverrides {
        default_cwd: Some("repo".to_string()),
        streaming: Some(false),
        compaction_auto_at_text_bytes: Some(2048),
        ..AgentManifestBindOverrides::default()
    };

    let (effective, overridden_keys) = apply_runtime_overrides(&defaults, &overrides).unwrap();

    assert_eq!(effective.default_cwd, "repo");
    assert!(!effective.streaming);
    assert_eq!(effective.compaction.auto_at_text_bytes, Some(2048));
    assert_eq!(
        overridden_keys,
        vec![
            "default_cwd".to_string(),
            "streaming".to_string(),
            "compaction.auto_at_text_bytes".to_string(),
        ]
    );
}

#[test]
fn runtime_overrides_report_only_keys_that_changed() {
    let defaults = defaults_with_allow(vec![
        AgentManifestRuntimeOverrideKey::DefaultCwd,
        AgentManifestRuntimeOverrideKey::Streaming,
    ]);
    let overrides = AgentManifestBindOverrides {
        default_cwd: Some("repo".to_string()),
        ..AgentManifestBindOverrides::default()
    };

    let (_effective, overridden_keys) = apply_runtime_overrides(&defaults, &overrides).unwrap();

    assert_eq!(overridden_keys, vec!["default_cwd".to_string()]);
}

#[test]
fn provider_and_model_refs_fail_closed() {
    let surface = AgentManifestProviderSurface::single("local_offline", "echo");
    assert_eq!(
        model_id_from_ref("model://local_offline/echo", "local_offline").unwrap(),
        "echo"
    );
    let provider_err = provider_id_from_ref("provider://openai_compatible")
        .and_then(|provider| {
            if provider == surface.provider_id {
                Ok(())
            } else {
                Err(CooldisError::RuntimeFactory("unknown provider".to_string()))
            }
        })
        .unwrap_err();
    assert!(provider_err.to_string().contains("unknown provider"));
    let model_err = model_id_from_ref(
        "model://openai_compatible/example-chat-model",
        "local_offline",
    )
    .unwrap_err();
    assert!(model_err.to_string().contains("expected"));
}

#[test]
fn operation_ref_parser_accepts_record_and_operation_segment() {
    let hash = "a".repeat(64);

    let whole = parse_operation_ref(&format!("op://analytics@sha256:{hash}")).unwrap();
    assert_eq!(whole.name, "analytics");
    assert_eq!(whole.operation.as_deref(), None);
    assert_eq!(whole.artifact_hash.as_deref(), Some(hash.as_str()));

    let selected = parse_operation_ref(&format!("op://analytics/profile@sha256:{hash}")).unwrap();
    assert_eq!(selected.name, "analytics");
    assert_eq!(selected.operation.as_deref(), Some("profile"));
    assert_eq!(selected.artifact_hash.as_deref(), Some(hash.as_str()));

    let selected_without_hash = parse_operation_ref("op://analytics/profile").unwrap();
    assert_eq!(selected_without_hash.name, "analytics");
    assert_eq!(selected_without_hash.operation.as_deref(), Some("profile"));
    assert_eq!(selected_without_hash.artifact_hash.as_deref(), None);

    for malformed in [
        "op://".to_string(),
        format!("op:///profile@sha256:{hash}"),
        format!("op://analytics/@sha256:{hash}"),
        format!("op://analytics//profile@sha256:{hash}"),
        format!("op://analytics/profile/deep@sha256:{hash}"),
    ] {
        let err = parse_operation_ref(&malformed).unwrap_err();
        assert!(err.to_string().contains("op://<record>/<operation>"));
    }
}

#[tokio::test]
async fn bind_rejects_streaming_when_provider_cannot_stream() {
    let root = temp_dir("manifest-bind-streaming");
    let manifest_path = root.join("streaming.cooldis.agent.toml");
    fs::write(
        &manifest_path,
        r#"
[agent]
name = "streaming"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = true
"#,
    )
    .unwrap();
    let record = crate::LocalAgentRegistry::new(root.join("agents"))
        .publish_manifest_path(&manifest_path)
        .unwrap();
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let err = bind_published_agent_record(
        &record,
        None,
        &surface,
        None,
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("runtime.streaming"));
    assert!(err.to_string().contains("support streaming"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn blob_static_source_binds_prompt_text_and_hash() {
    let root = temp_dir("manifest-bind-blob");
    let blob_root = root.join("blobs");
    let prompt_path = root.join("system.md");
    fs::write(&prompt_path, "You are the release verifier.\n").unwrap();
    let blob = LocalBlobRegistry::new(&blob_root)
        .publish_file(&prompt_path, Some("system_prompt"))
        .unwrap();
    let manifest_path = root.join("blob.cooldis.agent.toml");
    fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "blob-runner"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[resources]]
name = "system_prompt"
kind = "blob"
ref = "{}"

[context]
[[context.pipelines]]
id = "default"

[[context.pipelines.sources]]
id = "identity"
assembler = "{KERNEL_ASSEMBLER_STATIC}"
input = "system_prompt"
pinned = true
"#,
            blob.ref_uri
        ),
    )
    .unwrap();
    let record = crate::LocalAgentRegistry::new(root.join("agents"))
        .publish_manifest_path(&manifest_path)
        .unwrap();
    let surface = AgentManifestProviderSurface::single("local_offline", "echo");

    let bound = bind_published_agent_record(
        &record,
        None,
        &surface,
        None,
        Some(&blob_root),
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap();

    assert_eq!(bound.static_context_segments.len(), 1);
    let segment = &bound.static_context_segments[0];
    assert_eq!(segment.id, "identity");
    assert_eq!(segment.ref_uri, blob.ref_uri);
    assert_eq!(segment.content, "You are the release verifier.\n");
    assert_eq!(segment.content_sha256, blob.content_sha256);
    assert_eq!(
        bound.bind_receipt.static_context_segments[0].content_sha256,
        blob.content_sha256
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn missing_blob_resource_fails_bind_with_publish_hint() {
    let root = temp_dir("manifest-bind-missing-blob");
    let missing_hash = "a".repeat(64);
    let manifest_path = root.join("missing-blob.cooldis.agent.toml");
    fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "missing-blob"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[resources]]
name = "system_prompt"
kind = "blob"
ref = "resource://artifact/sha256:{missing_hash}"

[context]
[[context.pipelines]]
id = "default"

[[context.pipelines.sources]]
id = "identity"
assembler = "{KERNEL_ASSEMBLER_STATIC}"
input = "system_prompt"
pinned = true
"#
        ),
    )
    .unwrap();
    let record = crate::LocalAgentRegistry::new(root.join("agents"))
        .publish_manifest_path(&manifest_path)
        .unwrap();
    let surface = AgentManifestProviderSurface::single("local_offline", "echo");
    let blob_root = root.join("blobs");

    let err = bind_published_agent_record(
        &record,
        None,
        &surface,
        None,
        Some(blob_root.as_path()),
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("blob resource \"system_prompt\""));
    assert!(text.contains("resource://artifact/sha256:"));
    assert!(text.contains("cooldis blob publish"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn protocol_tool_import_requires_discovery_path() {
    let tool = protocol_import("mcp://arcade", None, None);

    let err = bind_protocol_tool_import(&tool, None).await.unwrap_err();

    assert!(
        err.to_string()
            .contains("requires a tool universe discoverer")
    );
    assert!(err.to_string().contains("fail closed"));
}

#[tokio::test]
async fn protocol_tool_import_binds_filtered_discovery() {
    let discovery = ToolUniverseDiscovery::witness(
        "mcp://arcade",
        vec![
            witnessed_tool("cooldis_mcp_echo", "string"),
            witnessed_tool("other.echo", "string"),
        ],
        1,
    )
    .unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let tool = protocol_import(
        "mcp://arcade",
        Some(vec!["cooldis_mcp_echo".to_string()]),
        None,
    );

    let binding = bind_protocol_tool_import(&tool, Some(&discoverer))
        .await
        .unwrap();

    assert_eq!(binding.discovery.tools.len(), 1);
    assert!(binding.discovery.contract("cooldis_mcp_echo").is_some());
    assert!(binding.discovery.contract("other.echo").is_none());
    assert_eq!(
        binding.include_tools,
        Some(BTreeSet::from(["cooldis_mcp_echo".to_string()]))
    );
}

#[tokio::test]
async fn protocol_tool_import_pin_drift_fails_bind_with_both_hashes() {
    let witnessed = witnessed_tool("cooldis_mcp_echo", "string");
    let expected_hash = format!("sha256:{}", "f".repeat(64));
    let discovery =
        ToolUniverseDiscovery::witness("mcp://arcade", vec![witnessed.clone()], 1).unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let tool = protocol_import(
        "mcp://arcade",
        None,
        Some(format!(
            "mcptool://arcade/cooldis_mcp_echo@{}",
            expected_hash
        )),
    );

    let err = bind_protocol_tool_import(&tool, Some(&discoverer))
        .await
        .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("pin drift"));
    assert!(text.contains(&expected_hash));
    assert!(text.contains(&witnessed.schema_hash));
}

#[tokio::test]
async fn protocol_tool_import_pin_missing_after_filter_is_drift() {
    let witnessed = witnessed_tool("cooldis_mcp_echo", "string");
    let pin_hash = witnessed
        .schema_hash
        .trim_start_matches("sha256:")
        .to_string();
    let discovery = ToolUniverseDiscovery::witness("mcp://arcade", vec![witnessed], 1).unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let tool = protocol_import(
        "mcp://arcade",
        Some(vec!["other.echo".to_string()]),
        Some(format!(
            "mcptool://arcade/cooldis_mcp_echo@sha256:{pin_hash}"
        )),
    );

    let err = bind_protocol_tool_import(&tool, Some(&discoverer))
        .await
        .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("pin drift"));
    assert!(text.contains(&format!("sha256:{pin_hash}")));
    assert!(text.contains("<missing>"));
}

#[tokio::test]
async fn protocol_tool_import_rejects_pin_without_direct_exposure() {
    let witnessed = witnessed_tool("cooldis_mcp_echo", "string");
    let pin_hash = witnessed
        .schema_hash
        .trim_start_matches("sha256:")
        .to_string();
    let discovery = ToolUniverseDiscovery::witness("mcp://arcade", vec![witnessed], 1).unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let mut tool = protocol_import(
        "mcp://arcade",
        None,
        Some(format!(
            "mcptool://arcade/cooldis_mcp_echo@sha256:{pin_hash}"
        )),
    );
    tool.expose.clear();

    let err = bind_protocol_tool_import(&tool, Some(&discoverer))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("without expose"));
}

#[tokio::test]
async fn protocol_tool_import_rejects_discovery_server_ref_mismatch() {
    let discovery = ToolUniverseDiscovery::witness(
        "mcp://wrong",
        vec![witnessed_tool("cooldis_mcp_echo", "string")],
        1,
    )
    .unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let tool = protocol_import("mcp://arcade", None, None);

    let err = bind_protocol_tool_import(&tool, Some(&discoverer))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("expected \"mcp://arcade\""));
}

#[tokio::test]
async fn protocol_tool_import_direct_pins_must_not_duplicate_tool_rows() {
    let witnessed = witnessed_tool("cooldis_mcp_echo", "string");
    let pin_hash = witnessed
        .schema_hash
        .trim_start_matches("sha256:")
        .to_string();
    let discovery = ToolUniverseDiscovery::witness("mcp://arcade", vec![witnessed], 1).unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let pin = format!("mcptool://arcade/cooldis_mcp_echo@sha256:{pin_hash}");
    let first =
        AgentManifestTool::ProtocolImport(protocol_import("mcp://arcade", None, Some(pin.clone())));
    let mut second = protocol_import("mcp://arcade", None, Some(pin));
    second.id = "mcp_echo_two".to_string();
    let second = AgentManifestTool::ProtocolImport(second);

    let result = bind_tools(
        &[first, second],
        None,
        &BTreeSet::from(["mcp://arcade".to_string()]),
        Some(&discoverer),
    )
    .await;
    let err = match result {
        Ok(_) => panic!("duplicate pinned direct rows should fail bind"),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("duplicate direct tool_name surface")
    );
    assert!(err.to_string().contains("cooldis_mcp_echo"));
}

#[tokio::test]
async fn protocol_tool_import_unconfigured_server_ref_error_is_preserved() {
    let root = temp_dir("manifest-bind-unconfigured-mcp");
    let manifest_path = root.join("mcp.cooldis.agent.toml");
    fs::write(
        &manifest_path,
        r#"
[agent]
name = "mcp"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "protocol_tool_import"
id = "echo"
protocol = "mcp"
server_ref = "mcp://arcade"

[runtime]
default_cwd = "."
streaming = false
"#,
    )
    .unwrap();
    let record = crate::LocalAgentRegistry::new(root.join("agents"))
        .publish_manifest_path(&manifest_path)
        .unwrap();
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let err = bind_published_agent_record(
        &record,
        None,
        &surface,
        None,
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("server_ref \"mcp://arcade\" is not configured")
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_coupling_binds_controller_receipt() {
    let root = temp_dir("manifest-bind-controller-coupling");
    let operation_root = root.join("operations");
    let operation = publish_multi_operation_record(
        &operation_root,
        "hitl_gate",
        &[("pre_tool_gate", vec!["thread.pause"])],
    )
    .await;
    let record = publish_agent_manifest(
        &root,
        &manifest_with_coupling(
            "controller_agent",
            "std::permission.approval_gate",
            &format!(
                "op://hitl_gate/pre_tool_gate@sha256:{}",
                operation.active_artifact_hash
            ),
            "thread.pause",
            "tool.call.requested",
            "thread",
            "tool.call.requested",
            "control",
            r#"["tool.call.suspended", "approval.requested"]"#,
            r#"pattern = "rm -rf""#,
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let bound = bind_published_agent_record(
        &record,
        None,
        &surface,
        Some(&operation_root),
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap();

    assert_eq!(bound.couplings.len(), 1);
    let coupling = &bound.couplings[0];
    assert_eq!(coupling.id, "std::permission.approval_gate");
    assert_eq!(coupling.role, CouplingRole::Controller);
    assert_eq!(coupling.trigger_kind, EventKind::ToolCallRequested);
    assert_eq!(
        coupling.source_selectors[0].kinds,
        vec![EventKind::ToolCallRequested]
    );
    assert_eq!(coupling.sink.stream, "control");
    assert_eq!(
        coupling.sink.kinds,
        vec![EventKind::ToolCallSuspended, EventKind::ApprovalRequested]
    );
    assert_eq!(
        coupling.function.artifact_hash,
        operation.active_artifact_hash
    );
    assert_eq!(
        coupling.function.operation_name.as_deref(),
        Some("pre_tool_gate")
    );
    assert_eq!(coupling.grants, vec!["thread.pause".to_string()]);
    assert_eq!(coupling.budget.max_ms, Some(250));
    assert_eq!(coupling.budget.max_discharge_events, Some(4));
    assert_eq!(
        coupling.config_hash,
        coupling_config_hash(&serde_json::json!({"pattern": "rm -rf"})).unwrap()
    );

    assert_eq!(bound.bind_receipt.couplings.len(), 1);
    assert_eq!(
        bound.bind_receipt.couplings[0],
        AgentManifestCouplingBinding {
            id: "std::permission.approval_gate".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: "tool.call.requested".to_string(),
            trigger_match: BTreeMap::from([("tool".to_string(), serde_json::json!("bash"))]),
            source_streams: vec!["thread".to_string()],
            source_kinds: vec!["tool.call.requested".to_string()],
            sink_stream: "control".to_string(),
            sink_kinds: vec![
                "tool.call.suspended".to_string(),
                "approval.requested".to_string(),
            ],
            function_ref: format!(
                "op://hitl_gate/pre_tool_gate@sha256:{}",
                operation.active_artifact_hash
            ),
            artifact_hash: operation.active_artifact_hash,
            operation_name: Some("pre_tool_gate".to_string()),
            grants: vec!["thread.pause".to_string()],
            budget: AgentManifestCouplingBudget {
                max_ms: Some(250),
                max_discharge_events: Some(4),
            },
            config_hash: coupling_config_hash(&serde_json::json!({"pattern": "rm -rf"})).unwrap(),
        }
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_coupling_infers_projection_for_distinct_derived_sink() {
    let root = temp_dir("manifest-bind-projection-coupling");
    let operation_root = root.join("operations");
    let operation = publish_multi_operation_record(
        &operation_root,
        "memory_writer",
        &[("extract", vec!["stream.write:derived:memory"])],
    )
    .await;
    let record = publish_agent_manifest(
        &root,
        &manifest_with_coupling(
            "projection_agent",
            "std::memory.extract",
            &format!(
                "op://memory_writer/extract@sha256:{}",
                operation.active_artifact_hash
            ),
            "stream.write:derived:memory",
            "turn.completed",
            "thread",
            "turn.completed",
            "derived:memory",
            r#""placement.decision""#,
            r#"max_notes = 3"#,
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let bound = bind_published_agent_record(
        &record,
        None,
        &surface,
        Some(&operation_root),
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap();

    assert_eq!(bound.couplings.len(), 1);
    assert_eq!(bound.couplings[0].role, CouplingRole::Projection);
    assert_eq!(bound.couplings[0].sink.stream, "derived:memory");
    assert_eq!(
        bound.bind_receipt.couplings[0].sink_kinds,
        vec!["placement.decision".to_string()]
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_coupling_requires_content_addressed_function_ref() {
    let root = temp_dir("manifest-bind-coupling-unpinned");
    let operation_root = root.join("operations");
    fs::create_dir_all(&operation_root).unwrap();
    let record = publish_agent_manifest(
        &root,
        &manifest_with_coupling(
            "unpinned_agent",
            "std::permission.approval_gate",
            "op://hitl_gate/pre_tool_gate",
            "thread.pause",
            "tool.call.requested",
            "thread",
            "tool.call.requested",
            "control",
            r#""tool.call.suspended""#,
            "",
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let err = bind_published_agent_record(
        &record,
        None,
        &surface,
        Some(&operation_root),
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("content-addressed"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_coupling_requires_declared_function_grants() {
    let root = temp_dir("manifest-bind-coupling-grants");
    let operation_root = root.join("operations");
    let operation = publish_multi_operation_record(
        &operation_root,
        "hitl_gate",
        &[("pre_tool_gate", vec!["thread.pause"])],
    )
    .await;
    let record = publish_agent_manifest(
        &root,
        &manifest_with_coupling(
            "grantless_agent",
            "std::permission.approval_gate",
            &format!(
                "op://hitl_gate/pre_tool_gate@sha256:{}",
                operation.active_artifact_hash
            ),
            "",
            "tool.call.requested",
            "thread",
            "tool.call.requested",
            "control",
            r#""tool.call.suspended""#,
            "",
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let err = bind_published_agent_record(
        &record,
        None,
        &surface,
        Some(&operation_root),
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("requires grants"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_coupling_event_kinds_fail_closed_at_bind() {
    let root = temp_dir("manifest-bind-coupling-kind");
    let operation_root = root.join("operations");
    let operation = publish_multi_operation_record(
        &operation_root,
        "hitl_gate",
        &[("pre_tool_gate", vec!["thread.pause"])],
    )
    .await;
    let record = publish_agent_manifest(
        &root,
        &manifest_with_coupling(
            "unknown_kind_agent",
            "std::permission.approval_gate",
            &format!(
                "op://hitl_gate/pre_tool_gate@sha256:{}",
                operation.active_artifact_hash
            ),
            "thread.pause",
            "tool.call.requested",
            "thread",
            "tool.call.promised",
            "control",
            r#""tool.call.suspended""#,
            "",
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let err = bind_published_agent_record(
        &record,
        None,
        &surface,
        Some(&operation_root),
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("unknown event kind"));
    assert!(err.to_string().contains("tool.call.promised"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn manifest_coupling_source_sink_identity_fails_closed_at_bind() {
    let root = temp_dir("manifest-bind-coupling-identity");
    let coupling = AgentManifestCoupling {
        id: "std::prompt.steer".to_string(),
        function_ref: format!("op://gate/check@sha256:{}", "a".repeat(64)),
        grants: Vec::new(),
        trigger: AgentManifestCouplingTrigger {
            kind: "turn.completed".to_string(),
            match_fields: BTreeMap::new(),
            quota: AgentManifestCouplingQuota::default(),
        },
        source: AgentManifestCouplingSource {
            selectors: vec![AgentManifestCouplingSelector {
                stream: "thread".to_string(),
                kind: vec!["turn.completed".to_string()],
                scope: None,
                since: None,
            }],
        },
        sink: AgentManifestCouplingSink {
            stream: "thread".to_string(),
            kind: vec!["loop.completed".to_string()],
        },
        budget: AgentManifestCouplingBudget::default(),
        config: serde_json::Value::Null,
    };

    let err = bind_coupling(&coupling, Some(&root)).unwrap_err();

    assert!(
        err.to_string()
            .contains("sink must not equal selected source")
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn coupling_config_hash_is_canonical_for_object_key_order() {
    let left: serde_json::Value = serde_json::from_str(r#"{"b":true,"a":1}"#).unwrap();
    let right: serde_json::Value = serde_json::from_str(r#"{"a":1,"b":true}"#).unwrap();

    assert_eq!(
        coupling_config_hash(&left).unwrap(),
        coupling_config_hash(&right).unwrap()
    );
}

#[tokio::test]
async fn manifest_coupling_custom_id_binds_to_wasm_executor() {
    let root = temp_dir("manifest-bind-custom-coupling-id");
    let operation_root = root.join("operations");
    let operation = publish_json_operation_record(&operation_root, "custom_policy", "check").await;
    let record = publish_agent_manifest(
        &root,
        &manifest_with_coupling(
            "custom_policy_agent",
            "org.example.custom_policy",
            &format!(
                "op://custom_policy/check@sha256:{}",
                operation.active_artifact_hash
            ),
            "",
            "turn.completed",
            "thread",
            "turn.completed",
            "control",
            r#""turn.continue.requested""#,
            "",
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let bound = bind_published_agent_record(
        &record,
        None,
        &surface,
        Some(&operation_root),
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap();

    assert_eq!(bound.couplings.len(), 1);
    let coupling = &bound.couplings[0];
    assert_eq!(coupling.id, "org.example.custom_policy");
    assert_eq!(coupling.function.name, "custom_policy");
    assert_eq!(coupling.function.operation_name.as_deref(), Some("check"));
    assert_eq!(
        coupling.function.artifact_hash,
        operation.active_artifact_hash
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_coupling_all_runtime_executable_std_templates_bind() {
    let root = temp_dir("manifest-bind-runtime-stdlib-couplings");
    let operation_root = root.join("operations");
    let operation =
        publish_multi_operation_record(&operation_root, "stdlib_policy", &[("run", vec![])]).await;
    let spawn_operation = publish_multi_operation_record(
        &operation_root,
        "stdlib_supervisor_spawn",
        &[("run", vec![THREADS_SPAWN_CAPABILITY])],
    )
    .await;
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    for template in crate::coupling_template_catalog_v1()
        .templates
        .into_iter()
        .filter(|template| template.runtime_executable)
    {
        let source_stream = if template.source.stream == template.sink.stream {
            "thread"
        } else {
            &template.source.stream
        };
        let (function_ref, grant, policy) = if template.id == STD_SUPERVISOR_SPAWN_TEMPLATE_ID {
            (
                format!(
                    "op://stdlib_supervisor_spawn/run@sha256:{}",
                    spawn_operation.active_artifact_hash
                ),
                THREADS_SPAWN_CAPABILITY,
                "\n[policies]\nallow_child_agents = true\n",
            )
        } else {
            (
                format!(
                    "op://stdlib_policy/run@sha256:{}",
                    operation.active_artifact_hash
                ),
                "",
                "",
            )
        };
        let record = publish_agent_manifest(
            &root,
            &(manifest_with_coupling(
                &format!(
                    "runtime_{}",
                    template.id.replace("std::", "").replace(['.', ':'], "_")
                ),
                &template.id,
                &function_ref,
                grant,
                &template.trigger_kinds[0].to_string(),
                source_stream,
                &template.source.kinds[0].to_string(),
                &template.sink.stream,
                &serde_json::to_string(
                    &template
                        .sink
                        .kinds
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
                "",
            ) + policy),
        );

        let bound = bind_published_agent_record(
            &record,
            None,
            &surface,
            Some(&operation_root),
            None,
            None,
            &BTreeSet::new(),
            None,
            &AgentManifestModelProfileSelection::default(),
            &AgentManifestBindOverrides::default(),
        )
        .await
        .unwrap_or_else(|err| panic!("{} should bind: {err}", template.id));

        assert_eq!(bound.couplings.len(), 1, "{}", template.id);
        assert_eq!(bound.couplings[0].id, template.id);
    }
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_coupling_non_runtime_executable_std_templates_fail_closed_at_bind() {
    let root = temp_dir("manifest-bind-non-runtime-stdlib-couplings");
    let operation_root = root.join("operations");
    let operation =
        publish_multi_operation_record(&operation_root, "reference_policy", &[("run", vec![])])
            .await;
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let non_executable = crate::coupling_template_catalog_v1()
        .templates
        .into_iter()
        .filter(|template| !template.runtime_executable)
        .collect::<Vec<_>>();
    assert_eq!(
        non_executable
            .iter()
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>(),
        vec!["std::io.channel_ingress", "std::io.channel_egress",]
    );

    for template in non_executable {
        let record = publish_agent_manifest(
            &root,
            &manifest_with_coupling(
                &format!(
                    "reference_{}",
                    template.id.replace("std::", "").replace(['.', ':'], "_")
                ),
                &template.id,
                &format!(
                    "op://reference_policy/run@sha256:{}",
                    operation.active_artifact_hash
                ),
                "",
                &template.trigger_kinds[0].to_string(),
                &template.source.stream,
                &template.source.kinds[0].to_string(),
                &template.sink.stream,
                &serde_json::to_string(
                    &template
                        .sink
                        .kinds
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
                "",
            ),
        );

        let err = bind_published_agent_record(
            &record,
            None,
            &surface,
            Some(&operation_root),
            None,
            None,
            &BTreeSet::new(),
            None,
            &AgentManifestModelProfileSelection::default(),
            &AgentManifestBindOverrides::default(),
        )
        .await
        .unwrap_err();
        let diagnostic = err.to_string();

        assert!(diagnostic.contains(&template.id), "{diagnostic}");
        assert!(
            diagnostic.contains("no registered executor"),
            "{diagnostic}"
        );
    }
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_supervisor_spawn_coupling_honors_child_agent_policy() {
    let root = temp_dir("manifest-bind-supervisor-spawn-policy");
    let operation_root = root.join("operations");
    let operation = publish_multi_operation_record(
        &operation_root,
        "stdlib_supervisor_spawn",
        &[("run", vec![THREADS_SPAWN_CAPABILITY])],
    )
    .await;
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);
    let manifest = manifest_with_coupling(
        "blocked_supervisor_spawn",
        STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
        &format!(
            "op://stdlib_supervisor_spawn/run@sha256:{}",
            operation.active_artifact_hash
        ),
        THREADS_SPAWN_CAPABILITY,
        "turn.submitted",
        "thread",
        "turn.submitted",
        "control",
        "[\"thread.spawn.requested\", \"turn.waiting\"]",
        r#"initial_submission = "delegate work""#,
    ) + "\n[policies]\nallow_child_agents = false\n";
    let record = publish_agent_manifest(&root, &manifest);

    let err = bind_published_agent_record(
        &record,
        None,
        &surface,
        Some(&operation_root),
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap_err();

    let diagnostic = err.to_string();
    assert!(diagnostic.contains("allow_child_agents = false"));
    assert!(diagnostic.contains("std::supervisor.spawn"));
    assert!(diagnostic.contains("threads.spawn"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn manifest_without_couplings_binds_unchanged() {
    let root = temp_dir("manifest-bind-no-couplings");
    let record = publish_agent_manifest(&root, &minimal_manifest("plain_agent"));
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let bound = bind_published_agent_record(
        &record,
        None,
        &surface,
        None,
        None,
        None,
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap();

    assert!(bound.couplings.is_empty());
    assert!(bound.bind_receipt.couplings.is_empty());
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn skill_resource_binds_package_digest_and_static_index() {
    let root = temp_dir("manifest-bind-skill-resource");
    let skill_root = root.join("skills");
    let package_dir = root.join("skill-src").join("karl-skills");
    write_skill_file(
        &package_dir,
        "alpha",
        r#"---
name: alpha
description: Alpha description.
---
# Alpha

Alpha body.
"#,
    );
    write_skill_file(
        &package_dir,
        "設計",
        r#"# 設計

Unicode description.

Unicode body.
"#,
    );
    let package = LocalSkillRegistry::new(&skill_root)
        .publish_directory(PublishSkillPackageRequest {
            package_dir,
            name: None,
        })
        .unwrap();
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[[resources]]
name = "karl_skills"
kind = "skill"
ref = "{}"
"#,
            minimal_manifest("skillful_agent"),
            package.ref_uri()
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let first = bind_published_agent_record(
        &record,
        None,
        &surface,
        None,
        None,
        Some(&skill_root),
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap();
    let second = bind_published_agent_record(
        &record,
        None,
        &surface,
        None,
        None,
        Some(&skill_root),
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap();

    assert_eq!(first.bind_receipt.skill_packages.len(), 1);
    let binding = &first.bind_receipt.skill_packages[0];
    assert_eq!(binding.resource_name, "karl_skills");
    assert_eq!(binding.package_name, "karl-skills");
    assert_eq!(binding.ref_uri, package.ref_uri());
    assert_eq!(binding.artifact_hash, package.active_artifact_hash);
    assert_eq!(
        binding.package_digest,
        format!("sha256:{}", package.active_artifact_hash)
    );
    assert_eq!(binding.skill_count, 2);

    assert_eq!(first.skill_context_segments, second.skill_context_segments);
    let segment = first.skill_context_segments.first().unwrap();
    assert_eq!(segment.id, "skill-index:karl_skills");
    assert_eq!(segment.assembler, KERNEL_ASSEMBLER_STATIC);
    assert_eq!(segment.input, "karl_skills");
    assert!(segment.pinned);
    assert_eq!(segment.budget_share, None);
    assert_eq!(segment.ref_uri, package.ref_uri());
    assert_eq!(
        segment.content,
        "alpha — Alpha description.\n設計 — Unicode description.\n"
    );
    assert_eq!(segment.content_sha256, binding.index_sha256);
    assert_eq!(
        first.bind_receipt.skill_packages,
        second.bind_receipt.skill_packages
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn operation_bind_requires_declared_grants() {
    let root = temp_dir("manifest-bind-grants");
    let registry = LocalOperationRegistry::new(&root);
    let wasm = wat::parse_str(operation_guest_with_required_capability()).unwrap();
    let artifact = root.join("search.wasm");
    fs::write(&artifact, wasm).unwrap();
    let record = registry
        .publish_artifact(crate::PublishOperationRequest {
            name: "search".to_string(),
            artifact_path: artifact.clone(),
            source: crate::PublishedOperationSource::Wasm { bin_path: artifact },
            interface: None,
            capability_grants: BTreeSet::from(["net:https://example.com".to_string()]),
            metadata: Default::default(),
        })
        .await
        .unwrap();

    let err = bind_operation_ref(
        "search",
        &format!("op://search@sha256:{}", record.active_artifact_hash),
        &[],
        None,
        Some(&root),
        &mut BTreeSet::new(),
        &mut OperationBindingMap::new(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("requires grants"));

    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();
    bind_operation_ref(
        "search",
        &format!("op://search@sha256:{}", record.active_artifact_hash),
        &["net:https://example.com".to_string()],
        None,
        Some(&root),
        &mut granted,
        &mut operation_bindings,
    )
    .await
    .unwrap();
    assert!(granted.contains("net:https://example.com"));
    assert_eq!(
        operation_bindings_from_map(operation_bindings),
        vec![AgentManifestOperationBinding {
            name: "search".to_string(),
            artifact_hash: record.active_artifact_hash,
            grants: vec!["net:https://example.com".to_string()],
            operations: Vec::new(),
            direct_tools: Vec::new(),
        }]
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn two_segment_operation_ref_validates_only_named_operation() {
    let root = temp_dir("manifest-bind-operation-segment");
    let record = publish_multi_operation_record(
        &root,
        "analytics",
        &[
            ("profile", vec!["net:https://profile.example"]),
            ("summarize", vec!["net:https://summary.example"]),
        ],
    )
    .await;

    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();
    bind_operation_ref(
        "profile",
        &format!(
            "op://analytics/profile@sha256:{}",
            record.active_artifact_hash
        ),
        &["net:https://profile.example".to_string()],
        None,
        Some(&root),
        &mut granted,
        &mut operation_bindings,
    )
    .await
    .unwrap();

    assert_eq!(
        granted,
        BTreeSet::from(["net:https://profile.example".to_string()])
    );
    assert_eq!(
        operation_bindings_from_map(operation_bindings),
        vec![AgentManifestOperationBinding {
            name: "analytics".to_string(),
            artifact_hash: record.active_artifact_hash,
            grants: vec!["net:https://profile.example".to_string()],
            operations: vec!["profile".to_string()],
            direct_tools: Vec::new(),
        }]
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn single_segment_operation_ref_still_validates_all_operations() {
    let root = temp_dir("manifest-bind-whole-record-grants");
    let record = publish_multi_operation_record(
        &root,
        "analytics",
        &[
            ("profile", vec!["net:https://profile.example"]),
            ("summarize", vec!["net:https://summary.example"]),
        ],
    )
    .await;
    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();

    let err = bind_operation_ref(
        "analytics",
        &format!("op://analytics@sha256:{}", record.active_artifact_hash),
        &["net:https://profile.example".to_string()],
        None,
        Some(&root),
        &mut granted,
        &mut operation_bindings,
    )
    .await
    .unwrap_err();

    assert!(
        err.to_string()
            .contains("summarize:net:https://summary.example")
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn two_segment_operation_ref_fails_closed_for_unknown_operation() {
    let root = temp_dir("manifest-bind-unknown-operation-segment");
    let record = publish_multi_operation_record(
        &root,
        "analytics",
        &[("profile", Vec::new()), ("summarize", Vec::new())],
    )
    .await;
    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();

    let err = bind_operation_ref(
        "missing",
        &format!(
            "op://analytics/export@sha256:{}",
            record.active_artifact_hash
        ),
        &[],
        None,
        Some(&root),
        &mut granted,
        &mut operation_bindings,
    )
    .await
    .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("op://<record>/<operation>@sha256:<hash>"));
    assert!(text.contains("available operations: profile, summarize"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn operation_bindings_merge_grants_for_shared_artifact() {
    let root = temp_dir("manifest-bind-merge-grants");
    let registry = LocalOperationRegistry::new(&root);
    let wasm = wat::parse_str(operation_guest_with_required_capability()).unwrap();
    let artifact = root.join("search.wasm");
    fs::write(&artifact, wasm).unwrap();
    let record = registry
        .publish_artifact(crate::PublishOperationRequest {
            name: "search".to_string(),
            artifact_path: artifact.clone(),
            source: crate::PublishedOperationSource::Wasm { bin_path: artifact },
            interface: None,
            capability_grants: BTreeSet::from(["net:https://example.com".to_string()]),
            metadata: Default::default(),
        })
        .await
        .unwrap();

    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();
    for row_grants in [
        vec!["net:https://example.com".to_string()],
        vec![
            "net:https://example.com".to_string(),
            "fs.read:/workspace".to_string(),
        ],
    ] {
        bind_operation_ref(
            "search",
            &format!("op://search@sha256:{}", record.active_artifact_hash),
            &row_grants,
            None,
            Some(&root),
            &mut granted,
            &mut operation_bindings,
        )
        .await
        .unwrap();
    }

    assert_eq!(
        granted,
        BTreeSet::from([
            "fs.read:/workspace".to_string(),
            "net:https://example.com".to_string(),
        ])
    );
    assert_eq!(
        operation_bindings_from_map(operation_bindings),
        vec![AgentManifestOperationBinding {
            name: "search".to_string(),
            artifact_hash: record.active_artifact_hash,
            grants: vec![
                "fs.read:/workspace".to_string(),
                "net:https://example.com".to_string(),
            ],
            operations: Vec::new(),
            direct_tools: Vec::new(),
        }]
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn operation_binding_merge_whole_record_absorbs_operation_subset() {
    let root = temp_dir("manifest-bind-merge-whole-record");
    let record = publish_multi_operation_record(
        &root,
        "analytics",
        &[
            ("profile", vec!["net:https://profile.example"]),
            ("summarize", vec!["net:https://summary.example"]),
        ],
    )
    .await;
    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();

    bind_operation_ref(
        "profile",
        &format!(
            "op://analytics/profile@sha256:{}",
            record.active_artifact_hash
        ),
        &["net:https://profile.example".to_string()],
        None,
        Some(&root),
        &mut granted,
        &mut operation_bindings,
    )
    .await
    .unwrap();
    bind_operation_ref(
        "analytics",
        &format!("op://analytics@sha256:{}", record.active_artifact_hash),
        &[
            "net:https://profile.example".to_string(),
            "net:https://summary.example".to_string(),
        ],
        None,
        Some(&root),
        &mut granted,
        &mut operation_bindings,
    )
    .await
    .unwrap();

    assert_eq!(
        operation_bindings_from_map(operation_bindings),
        vec![AgentManifestOperationBinding {
            name: "analytics".to_string(),
            artifact_hash: record.active_artifact_hash,
            grants: vec![
                "net:https://profile.example".to_string(),
                "net:https://summary.example".to_string(),
            ],
            operations: Vec::new(),
            direct_tools: Vec::new(),
        }]
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn operation_binding_accumulator_merges_whole_record_order_independently() {
    let mut subset_then_whole = OperationBindingAccumulator::default();
    subset_then_whole.merge(
        BTreeSet::from(["net:https://profile.example".to_string()]),
        Some("profile".to_string()),
        None,
    );
    subset_then_whole.merge(
        BTreeSet::from(["net:https://summary.example".to_string()]),
        None,
        None,
    );
    assert_eq!(subset_then_whole.operation_names(), Vec::<String>::new());
    assert_eq!(
        subset_then_whole.grants,
        BTreeSet::from([
            "net:https://profile.example".to_string(),
            "net:https://summary.example".to_string(),
        ])
    );

    let mut whole_then_subset = OperationBindingAccumulator::default();
    whole_then_subset.merge(
        BTreeSet::from(["net:https://summary.example".to_string()]),
        None,
        None,
    );
    whole_then_subset.merge(
        BTreeSet::from(["net:https://profile.example".to_string()]),
        Some("profile".to_string()),
        None,
    );
    assert_eq!(whole_then_subset.operation_names(), Vec::<String>::new());
    assert_eq!(whole_then_subset.grants, subset_then_whole.grants);

    let mut subsets = OperationBindingAccumulator::default();
    subsets.merge(
        BTreeSet::from(["net:https://summary.example".to_string()]),
        Some("summarize".to_string()),
        None,
    );
    subsets.merge(
        BTreeSet::from(["net:https://profile.example".to_string()]),
        Some("profile".to_string()),
        None,
    );
    assert_eq!(
        subsets.operation_names(),
        vec!["profile".to_string(), "summarize".to_string()]
    );
    assert_eq!(subsets.grants, subset_then_whole.grants);
}

#[test]
fn operation_binding_accepts_legacy_metadata_without_grants_or_operations() {
    let bindings = serde_json::from_str::<Vec<AgentManifestOperationBinding>>(
            r#"[{"name":"search","artifact_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}]"#,
        )
        .unwrap();

    assert_eq!(
        bindings,
        vec![AgentManifestOperationBinding {
            name: "search".to_string(),
            artifact_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_string(),
            grants: Vec::new(),
            operations: Vec::new(),
            direct_tools: Vec::new(),
        }]
    );
}

#[tokio::test]
async fn operation_bind_requires_published_operation() {
    let root = temp_dir("manifest-bind-missing-operation");
    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();

    let err = bind_operation_ref(
        "search",
        "op://search@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        &["net:https://example.com".to_string()],
        None,
        Some(&root),
        &mut granted,
        &mut operation_bindings,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("was not found"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn operation_bind_requires_content_addressed_ref() {
    let root = temp_dir("manifest-bind-unpinned-operation");
    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();

    let err = bind_operation_ref(
        "search",
        "op://search",
        &[],
        None,
        Some(&root),
        &mut granted,
        &mut operation_bindings,
    )
    .await
    .unwrap_err();

    assert!(err.to_string().contains("content-addressed"));
    let _ = fs::remove_dir_all(root);
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("cooldis-{label}-{}", Uuid::now_v7()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn publish_agent_manifest(root: &Path, manifest: &str) -> crate::PublishedAgentRecord {
    let manifest_path = root.join("agent.cooldis.agent.toml");
    fs::write(&manifest_path, manifest).unwrap();
    crate::LocalAgentRegistry::new(root.join("agents"))
        .publish_manifest_path(&manifest_path)
        .unwrap()
}

fn minimal_manifest(name: &str) -> String {
    format!(
        r#"
[agent]
name = "{name}"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false
"#
    )
}

fn write_skill_file(package_dir: &Path, name: &str, body: &str) {
    let dir = package_dir.join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("SKILL.md"), body).unwrap();
}

#[allow(clippy::too_many_arguments)]
fn manifest_with_coupling(
    name: &str,
    coupling_id: &str,
    function_ref: &str,
    grant: &str,
    trigger_kind: &str,
    source_stream: &str,
    source_kind: &str,
    sink_stream: &str,
    sink_kind: &str,
    config_body: &str,
) -> String {
    let grants = if grant.is_empty() {
        "[]".to_string()
    } else {
        format!("[\"{grant}\"]")
    };
    let config_section = if config_body.trim().is_empty() {
        String::new()
    } else {
        format!(
            r#"
[couplings.config]
{config_body}
"#
        )
    };
    format!(
        r#"{}

[[couplings]]
id = "{coupling_id}"
function_ref = "{function_ref}"
grants = {grants}

[couplings.trigger]
kind = "{trigger_kind}"

[couplings.trigger.match]
tool = "bash"

[[couplings.source.selectors]]
stream = "{source_stream}"
kind = "{source_kind}"
scope = "turn"
since = "turn.start"

[couplings.sink]
stream = "{sink_stream}"
kind = {sink_kind}

[couplings.budget]
max_ms = 250
max_discharge_events = 4
{config_section}
"#,
        minimal_manifest(name)
    )
}

fn protocol_import(
    server_ref: &str,
    include_tools: Option<Vec<String>>,
    pin: Option<String>,
) -> AgentManifestProtocolToolImport {
    let expose = if pin.is_some() {
        vec![AgentManifestToolSurface::DirectTool]
    } else {
        Vec::new()
    };
    AgentManifestProtocolToolImport {
        id: "mcp_echo".to_string(),
        protocol: AgentManifestToolProtocol::Mcp,
        server_ref: server_ref.to_string(),
        expose,
        pin,
        include_tools,
        grants: Vec::new(),
    }
}

fn witnessed_tool(tool_name: &str, message_type: &str) -> WitnessedToolContract {
    WitnessedToolContract::witness(&ToolDefinition::new(
        tool_name,
        format!("Description for {tool_name}."),
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": message_type}}
        }),
    ))
    .unwrap()
}

fn operation_guest_with_required_capability() -> String {
    multi_operation_guest_with_required_capabilities(&[("search", vec!["net:https://example.com"])])
}

async fn publish_multi_operation_record(
    root: &Path,
    record_name: &str,
    operations: &[(&str, Vec<&str>)],
) -> crate::PublishedOperationRecord {
    fs::create_dir_all(root).unwrap();
    let registry = LocalOperationRegistry::new(root);
    let wasm =
        wat::parse_str(multi_operation_guest_with_required_capabilities(operations)).unwrap();
    let artifact = root.join(format!("{record_name}.wasm"));
    fs::write(&artifact, wasm).unwrap();
    let capability_grants = operations
        .iter()
        .flat_map(|(_, capabilities)| {
            capabilities
                .iter()
                .map(|capability| (*capability).to_string())
        })
        .collect::<BTreeSet<_>>();
    registry
        .publish_artifact(crate::PublishOperationRequest {
            name: record_name.to_string(),
            artifact_path: artifact.clone(),
            source: crate::PublishedOperationSource::Wasm { bin_path: artifact },
            interface: None,
            capability_grants,
            metadata: Default::default(),
        })
        .await
        .unwrap()
}

async fn publish_json_operation_record(
    root: &Path,
    record_name: &str,
    operation_name: &str,
) -> crate::PublishedOperationRecord {
    fs::create_dir_all(root).unwrap();
    let registry = LocalOperationRegistry::new(root);
    let wasm = wat::parse_str(json_operation_guest(operation_name)).unwrap();
    let artifact = root.join(format!("{record_name}.wasm"));
    fs::write(&artifact, wasm).unwrap();
    registry
        .publish_artifact(crate::PublishOperationRequest {
            name: record_name.to_string(),
            artifact_path: artifact.clone(),
            source: crate::PublishedOperationSource::Wasm { bin_path: artifact },
            interface: None,
            capability_grants: Default::default(),
            metadata: Default::default(),
        })
        .await
        .unwrap()
}

fn json_operation_guest(operation_name: &str) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": operation_name,
            "input": "json",
            "output": "json",
            "events": "none",
            "mode": "sync",
            "required_capabilities": []
        }]
    })
    .to_string();
    format!(
        r#"
            (module
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write
                drop
                i32.const 0)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
    )
}

fn multi_operation_guest_with_required_capabilities(operations: &[(&str, Vec<&str>)]) -> String {
    let operations = operations
        .iter()
        .enumerate()
        .map(|(index, (name, required_capabilities))| {
            serde_json::json!({
                "id": index + 1,
                "name": name,
                "input": "bytes",
                "output": "bytes",
                "events": "none",
                "mode": "sync",
                "required_capabilities": required_capabilities
            })
        })
        .collect::<Vec<_>>();
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": operations
    })
    .to_string();
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write
                drop
                i32.const 0)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
    )
}

fn wat_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| match byte {
            b'\n' => "\\0a".to_string(),
            b'\r' => "\\0d".to_string(),
            b'\t' => "\\09".to_string(),
            b'"' => "\\22".to_string(),
            b'\\' => "\\5c".to_string(),
            0x20..=0x7e => (*byte as char).to_string(),
            _ => format!("\\{byte:02x}"),
        })
        .collect()
}

#[test]
fn bind_receipt_placement_is_optional_on_the_wire() {
    // Receipts witnessed before ADR 0006 have no placement field; that
    // exact wire shape must keep decoding, with absent meaning local.
    let legacy_wire = serde_json::json!({
        "ref_uri": "cooldis://agents/karl",
        "manifest_hash": "sha256-manifest",
        "model_profile_id": "default",
        "provider_id": "anthropic",
        "model_id": "claude-sonnet-5",
        "tool_ids": ["threads/spawn"],
        "operation_bindings": [],
        "granted": [THREADS_SPAWN_CAPABILITY],
        "effective_runtime": {
            "default_cwd": "workspace",
            "streaming": true,
            "turn_timeout_ms": 1000,
            "cancellation_grace_ms": null,
            "compaction": {"auto_at_text_bytes": 500},
            "overrides": {"allow": []}
        },
        "overridden_keys": []
    });
    let legacy_receipt: AgentManifestBindReceipt =
        serde_json::from_value(legacy_wire.clone()).unwrap();
    assert_eq!(legacy_receipt.placement, None);
    assert_eq!(legacy_receipt.ref_uri, "cooldis://agents/karl");
    assert_eq!(legacy_receipt.tool_ids, vec!["threads/spawn"]);
    assert!(
        serde_json::to_value(&legacy_receipt)
            .unwrap()
            .get("placement")
            .is_none(),
        "absent placement must serialize to the legacy wire shape"
    );

    let placed = AgentManifestBindReceipt {
        placement: Some(AgentManifestPlacementBinding {
            target: crate::PlacementTarget::Sandbox,
            executor_ref: Some("executors/pi-sandbox".to_string()),
            config: std::collections::BTreeMap::new(),
        }),
        ..legacy_receipt
    };
    let round_tripped: AgentManifestBindReceipt =
        serde_json::from_str(&serde_json::to_string(&placed).unwrap()).unwrap();
    assert_eq!(round_tripped, placed);
}

#[test]
fn bind_receipt_placement_tolerates_future_wire_fields() {
    let future_wire = serde_json::json!({
        "ref_uri": "cooldis://agents/karl",
        "manifest_hash": "sha256-manifest",
        "model_profile_id": "default",
        "provider_id": "anthropic",
        "model_id": "claude-sonnet-5",
        "tool_ids": [],
        "operation_bindings": [],
        "granted": [],
        "effective_runtime": {},
        "overridden_keys": [],
        "placement": {
            "target": "sandbox",
            "executor_ref": "executors/pi-sandbox",
            "config": {},
            "future_lease_epoch": 7
        },
        "future_receipt_field": true
    });

    let decoded: AgentManifestBindReceipt = serde_json::from_value(future_wire).unwrap();
    assert_eq!(
        decoded.placement,
        Some(AgentManifestPlacementBinding {
            target: crate::PlacementTarget::Sandbox,
            executor_ref: Some("executors/pi-sandbox".to_string()),
            config: std::collections::BTreeMap::new(),
        })
    );
}
