use super::*;
use crate::agent::manifest_schema::{
    AgentManifestCouplingSource, AgentManifestCouplingTrigger, KERNEL_ASSEMBLER_STATIC,
};
use crate::{
    AgentManifestBashTool, AgentManifestCompactionDefaults, AgentManifestDirectTool,
    AgentManifestRuntimeOverridePolicy, AgentManifestToolProtocol, LocalBlobRegistry,
    LocalSkillRegistry, PublishSkillPackageRequest, STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
    SkillImportPlan, THREADS_SPAWN_CAPABILITY, ToolDefinition, ToolUniverseDiscovery,
    WitnessedToolContract,
};
use async_trait::async_trait;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use uuid::Uuid;

struct StaticToolUniverseDiscoverer {
    discovery: ToolUniverseDiscovery,
}

#[async_trait]
impl ToolUniverseDiscoverer for StaticToolUniverseDiscoverer {
    async fn discover(&self, _server_ref: &str) -> VerletResult<ToolUniverseDiscovery> {
        Ok(self.discovery.clone())
    }
}

fn defaults_with_allow(
    allow: Vec<AgentManifestRuntimeOverrideKey>,
) -> AgentManifestRuntimeDefaults {
    AgentManifestRuntimeDefaults {
        default_cwd: "workspace".to_string(),
        streaming: true,
        max_tool_rounds: Some(AgentManifestMaxToolRounds::Limited(8)),
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
        AgentManifestRuntimeOverrideKey::MaxToolRounds,
        AgentManifestRuntimeOverrideKey::CompactionAutoAtTextBytes,
    ]);
    let overrides = AgentManifestBindOverrides {
        default_cwd: Some("repo".to_string()),
        streaming: Some(false),
        max_tool_rounds: Some(AgentManifestMaxToolRounds::Unlimited),
        compaction_auto_at_text_bytes: Some(2048),
        ..AgentManifestBindOverrides::default()
    };

    let (effective, overridden_keys) = apply_runtime_overrides(&defaults, &overrides).unwrap();

    assert_eq!(effective.default_cwd, "repo");
    assert!(!effective.streaming);
    assert_eq!(
        effective.max_tool_rounds,
        Some(AgentManifestMaxToolRounds::Unlimited)
    );
    assert_eq!(effective.compaction.auto_at_text_bytes, Some(2048));
    assert_eq!(
        overridden_keys,
        vec![
            "default_cwd".to_string(),
            "streaming".to_string(),
            "max_tool_rounds".to_string(),
            "compaction.auto_at_text_bytes".to_string(),
        ]
    );
}

#[test]
fn runtime_tool_round_override_is_allowlisted_and_validated() {
    let defaults = defaults_with_allow(vec![AgentManifestRuntimeOverrideKey::MaxToolRounds]);
    let finite = AgentManifestBindOverrides {
        max_tool_rounds: Some(AgentManifestMaxToolRounds::Limited(64)),
        ..AgentManifestBindOverrides::default()
    };

    let (effective, overridden_keys) = apply_runtime_overrides(&defaults, &finite).unwrap();
    assert_eq!(
        effective.max_tool_rounds,
        Some(AgentManifestMaxToolRounds::Limited(64))
    );
    assert_eq!(overridden_keys, vec!["max_tool_rounds".to_string()]);

    let invalid = AgentManifestBindOverrides {
        max_tool_rounds: Some(AgentManifestMaxToolRounds::Limited(0)),
        ..AgentManifestBindOverrides::default()
    };
    let err = apply_runtime_overrides(&defaults, &invalid).unwrap_err();
    assert!(err.to_string().contains("max_tool_rounds"));
    assert!(err.to_string().contains("must be > 0"));

    let camel_case: AgentManifestBindOverrides =
        serde_json::from_value(serde_json::json!({"maxToolRounds": "unlimited"})).unwrap();
    assert_eq!(
        camel_case.max_tool_rounds,
        Some(AgentManifestMaxToolRounds::Unlimited)
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
fn workspace_binding_resolution_is_declared_fail_closed_and_override_first() {
    let root = temp_dir("manifest-workspace-binding");
    let default_host = root.join("default");
    let override_host = root.join("override");
    fs::create_dir_all(&default_host).unwrap();
    fs::create_dir_all(&override_host).unwrap();
    let requirement = AgentManifestWorkspaceRequirement {
        guest_path: "/workspace".to_string(),
        min_mode: AgentManifestWorkspaceMode::ReadWrite,
    };
    let default_binding = AgentManifestWorkspaceBinding {
        host_path: default_host.clone(),
        mode: AgentManifestWorkspaceMode::ReadWrite,
    };
    let override_binding = AgentManifestWorkspaceBinding {
        host_path: override_host.clone(),
        mode: AgentManifestWorkspaceMode::ReadWrite,
    };

    let missing = resolve_manifest_workspace(Some(&requirement), None, None).unwrap_err();
    assert!(missing.to_string().contains("requires a workspace binding"));

    let undeclared = resolve_manifest_workspace(None, Some(&default_binding), None).unwrap_err();
    assert!(undeclared.to_string().contains("did not declare"));

    let resolved = resolve_manifest_workspace(
        Some(&requirement),
        Some(&default_binding),
        Some(&override_binding),
    )
    .unwrap()
    .expect("resolved workspace mount");
    assert_eq!(resolved.guest_path, PathBuf::from("/workspace"));
    assert_eq!(
        resolved.host_path,
        fs::canonicalize(&override_host).unwrap()
    );
    assert_eq!(resolved.mode, AgentManifestWorkspaceMode::ReadWrite);
    let default_origin =
        resolve_manifest_workspace_with_origin(Some(&requirement), Some(&default_binding), None)
            .unwrap()
            .unwrap();
    assert_eq!(
        default_origin.origin,
        AgentManifestBindingOrigin::DaemonDefault
    );
    let override_origin = resolve_manifest_workspace_with_origin(
        Some(&requirement),
        Some(&default_binding),
        Some(&override_binding),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        override_origin.origin,
        AgentManifestBindingOrigin::BindOverride
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn workspace_binding_enforces_the_manifest_mode_floor() {
    let root = temp_dir("manifest-workspace-mode-floor");
    fs::create_dir_all(&root).unwrap();
    let requirement = AgentManifestWorkspaceRequirement {
        guest_path: "/app".to_string(),
        min_mode: AgentManifestWorkspaceMode::ReadWrite,
    };
    let binding = AgentManifestWorkspaceBinding {
        host_path: root.clone(),
        mode: AgentManifestWorkspaceMode::ReadOnly,
    };

    let err = resolve_manifest_workspace(Some(&requirement), Some(&binding), None).unwrap_err();

    assert!(err.to_string().contains("minimum mode rw"));
    let _ = fs::remove_dir_all(root);
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
                Err(VerletError::RuntimeFactory("unknown provider".to_string()))
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
    let manifest_path = root.join("streaming.verlet.agent.toml");
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
    let manifest_path = root.join("blob.verlet.agent.toml");
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
    let receipt = serde_json::to_value(&bound.bind_receipt).unwrap();
    assert_eq!(receipt["model_profile_origin"], "manifest-default");
    assert_eq!(receipt["placement_origin"], "daemon-default");
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn missing_blob_resource_fails_bind_with_publish_hint() {
    let root = temp_dir("manifest-bind-missing-blob");
    let missing_hash = "a".repeat(64);
    let manifest_path = root.join("missing-blob.verlet.agent.toml");
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
    assert!(text.contains("verlet blob publish"));
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
            witnessed_tool("verlet_mcp_echo", "string"),
            witnessed_tool("other.echo", "string"),
        ],
        1,
    )
    .unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let tool = protocol_import(
        "mcp://arcade",
        Some(vec!["verlet_mcp_echo".to_string()]),
        None,
    );

    let binding = bind_protocol_tool_import(&tool, Some(&discoverer))
        .await
        .unwrap();

    assert_eq!(binding.discovery.tools.len(), 1);
    assert!(binding.discovery.contract("verlet_mcp_echo").is_some());
    assert!(binding.discovery.contract("other.echo").is_none());
    assert_eq!(
        binding.include_tools,
        Some(BTreeSet::from(["verlet_mcp_echo".to_string()]))
    );
}

#[tokio::test]
async fn effect_classes_land_in_operation_direct_and_pinned_universe_receipts() {
    let root = temp_dir("manifest-bind-effect-classes");
    let bash_record =
        publish_multi_operation_record(&root, "bash-ops", &[("inspect", vec![])]).await;
    let direct_record =
        publish_multi_operation_record(&root, "direct-ops", &[("lookup", vec![])]).await;
    let witnessed = witnessed_tool("remote.lookup", "string");
    let pin = format!("mcptool://arcade/remote.lookup@{}", witnessed.schema_hash);
    let discovery = ToolUniverseDiscovery::witness("mcp://arcade", vec![witnessed], 1).unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let tools = vec![
        AgentManifestTool::Bash(AgentManifestBashTool {
            id: "inspect".to_string(),
            command: "inspect".to_string(),
            operation_ref: format!(
                "op://bash-ops/inspect@sha256:{}",
                bash_record.active_artifact_hash
            ),
            effect_class: EffectClass::Pure,
            grants: Vec::new(),
        }),
        AgentManifestTool::Direct(AgentManifestDirectTool {
            id: "lookup".to_string(),
            tool_name: "lookup".to_string(),
            operation_ref: format!(
                "op://direct-ops/lookup@sha256:{}",
                direct_record.active_artifact_hash
            ),
            effect_class: EffectClass::Idempotent,
            grants: Vec::new(),
        }),
        AgentManifestTool::ProtocolImport(AgentManifestProtocolToolImport {
            effect_class: EffectClass::Pure,
            ..protocol_import("mcp://arcade", None, Some(pin))
        }),
    ];

    let bound = bind_tools(
        &tools,
        Some(&root),
        &BTreeSet::from(["mcp://arcade".to_string()]),
        Some(&discoverer),
        0,
    )
    .await
    .unwrap();

    let bash = bound
        .operation_bindings
        .iter()
        .find(|binding| binding.name == "bash-ops")
        .unwrap();
    assert_eq!(bash.effect_class, EffectClass::Pure);
    let direct = bound
        .operation_bindings
        .iter()
        .find(|binding| binding.name == "direct-ops")
        .unwrap();
    assert_eq!(direct.effect_class, EffectClass::Idempotent);
    assert_eq!(direct.direct_tools[0].effect_class, EffectClass::Idempotent);
    let universe = ToolUniverseBindReceipt::from_binding(&bound.tool_universes[0]);
    assert_eq!(universe.tools[0].effect_class, EffectClass::Pure);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn protocol_tool_import_pin_drift_fails_bind_with_both_hashes() {
    let witnessed = witnessed_tool("verlet_mcp_echo", "string");
    let expected_hash = format!("sha256:{}", "f".repeat(64));
    let discovery =
        ToolUniverseDiscovery::witness("mcp://arcade", vec![witnessed.clone()], 1).unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let tool = protocol_import(
        "mcp://arcade",
        None,
        Some(format!(
            "mcptool://arcade/verlet_mcp_echo@{}",
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
    let witnessed = witnessed_tool("verlet_mcp_echo", "string");
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
            "mcptool://arcade/verlet_mcp_echo@sha256:{pin_hash}"
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
    let witnessed = witnessed_tool("verlet_mcp_echo", "string");
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
            "mcptool://arcade/verlet_mcp_echo@sha256:{pin_hash}"
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
        vec![witnessed_tool("verlet_mcp_echo", "string")],
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
    let witnessed = witnessed_tool("verlet_mcp_echo", "string");
    let pin_hash = witnessed
        .schema_hash
        .trim_start_matches("sha256:")
        .to_string();
    let discovery = ToolUniverseDiscovery::witness("mcp://arcade", vec![witnessed], 1).unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let pin = format!("mcptool://arcade/verlet_mcp_echo@sha256:{pin_hash}");
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
        0,
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
    assert!(err.to_string().contains("verlet_mcp_echo"));
}

#[tokio::test]
async fn bind_receipt_keeps_each_pinned_universe_import_correspondence() {
    let root = temp_dir("manifest-bind-multiple-pinned-universes");
    let first_tool = witnessed_tool("first.echo", "string");
    let second_tool = witnessed_tool("second.echo", "string");
    let first_pin = format!(
        "mcptool://arcade/{}@{}",
        first_tool.tool_name, first_tool.schema_hash
    );
    let second_pin = format!(
        "mcptool://arcade/{}@{}",
        second_tool.tool_name, second_tool.schema_hash
    );
    let discovery =
        ToolUniverseDiscovery::witness("mcp://arcade", vec![first_tool, second_tool], 1).unwrap();
    let discoverer = StaticToolUniverseDiscoverer { discovery };
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"
[agent]
name = "multiple-pinned-universes"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "protocol_tool_import"
id = "first-import"
protocol = "mcp"
server_ref = "mcp://arcade"
expose = ["direct_tool"]
pin = "{first_pin}"

[[tools]]
type = "protocol_tool_import"
id = "second-import"
protocol = "mcp"
server_ref = "mcp://arcade"
expose = ["direct_tool"]
pin = "{second_pin}"

[runtime]
default_cwd = "."
streaming = false
"#
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let bound = bind_published_agent_record(
        &record,
        None,
        &surface,
        None,
        None,
        None,
        &BTreeSet::from(["mcp://arcade".to_string()]),
        Some(&discoverer),
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
    )
    .await
    .unwrap();

    assert_eq!(
        bound.bind_receipt.tool_ids,
        vec!["first-import".to_string(), "second-import".to_string()]
    );
    assert_eq!(bound.bind_receipt.tool_universes.len(), 2);
    assert_eq!(
        bound.bind_receipt.tool_universes[0].import_id,
        "first-import"
    );
    assert_eq!(bound.bind_receipt.tool_universes[0].pinned, vec![first_pin]);
    assert_eq!(
        bound.bind_receipt.tool_universes[1].import_id,
        "second-import"
    );
    assert_eq!(
        bound.bind_receipt.tool_universes[1].pinned,
        vec![second_pin]
    );

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn bind_receipt_does_not_record_operation_rows_by_manifest_tool_id() {
    let root = temp_dir("manifest-bind-operation-tool-correspondence");
    let operation_root = root.join("operations");
    let operation = publish_multi_operation_record(
        &operation_root,
        "operation-record",
        &[("operation-name", Vec::new())],
    )
    .await;
    let manifest_path = root.join("operation-tool.verlet.agent.toml");
    fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "operation-tool-correspondence"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "bash_tool"
id = "manifest-tool-id"
command = "run-operation"
operation_ref = "op://operation-record/operation-name@sha256:{}"

[runtime]
default_cwd = "."
streaming = false
"#,
            operation.active_artifact_hash
        ),
    )
    .unwrap();
    let record = crate::LocalAgentRegistry::new(root.join("agents"))
        .publish_manifest_path_with_operation_registry(&manifest_path, &operation_root)
        .unwrap();
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

    assert_eq!(
        bound.bind_receipt.tool_ids,
        vec!["manifest-tool-id".to_string()]
    );
    assert_eq!(bound.bind_receipt.operation_bindings.len(), 1);
    assert_eq!(
        bound.bind_receipt.operation_bindings[0].name,
        "operation-record"
    );
    assert_eq!(
        bound.bind_receipt.operation_bindings[0].operations,
        vec!["operation-name".to_string()]
    );
    assert!(
        bound.bind_receipt.operation_bindings[0]
            .direct_tools
            .is_empty()
    );

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn protocol_tool_import_unconfigured_server_ref_error_is_preserved() {
    let root = temp_dir("manifest-bind-unconfigured-mcp");
    let manifest_path = root.join("mcp.verlet.agent.toml");
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
            grant_expiries: Vec::new(),
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
async fn imported_skill_omission_is_model_visible_at_bind() {
    let root = temp_dir("manifest-bind-imported-skill");
    let skill_dir = root.join("skill-src/fixture-skill");
    fs::create_dir_all(skill_dir.join("scripts")).unwrap();
    fs::write(
        skill_dir.join("SKILL.md"),
        "# Fixture Skill\n\nFixture description.\n",
    )
    .unwrap();
    fs::write(skill_dir.join("scripts/check.py"), "print('check')\n").unwrap();
    let skill_root = root.join("skills");
    let plan = SkillImportPlan::from_directory(&skill_dir, None).unwrap();
    let imported = plan
        .publish(
            &LocalSkillRegistry::new(&skill_root),
            &LocalBlobRegistry::new(root.join("blobs")),
        )
        .unwrap();
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[[resources]]
name = "fixture_skill"
kind = "skill"
ref = "{}"
"#,
            minimal_manifest("imported_skill_agent"),
            imported.skill.ref_uri()
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let bound = bind_published_agent_record(
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

    let index = &bound.skill_context_segments[0].content;
    assert!(index.contains("scripts omitted"), "{index}");
    assert!(index.contains("scripts/check.py"), "{index}");
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn floating_skill_resource_pins_latest_at_each_bind_and_preserves_prior_binding() {
    let root = temp_dir("manifest-bind-floating-skill-resource");
    let skill_root = root.join("skills");
    let package_dir = root.join("skill-src").join("karl-skills");
    write_skill_file(
        &package_dir,
        "alpha",
        "# Alpha\n\nFirst description.\n\nFirst body.\n",
    );
    let registry = LocalSkillRegistry::new(&skill_root);
    let first_package = registry
        .publish_directory(PublishSkillPackageRequest {
            package_dir: package_dir.clone(),
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
ref = "skill://karl-skills"
"#,
            minimal_manifest("floating_skill_agent")
        ),
    );
    assert_eq!(record.resource_count, 1);
    assert!(
        record.resolved_refs.is_empty(),
        "floating skill refs must be absent from compile-time resolved_refs"
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);

    let first_bound = bind_published_agent_record(
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
    let first_binding = first_bound.bind_receipt.skill_packages[0].clone();
    assert_eq!(first_binding.ref_uri, first_package.ref_uri());
    assert_eq!(
        first_binding.package_digest,
        format!("sha256:{}", first_package.active_artifact_hash)
    );

    fs::write(
        package_dir.join("alpha/SKILL.md"),
        "# Alpha\n\nSecond description.\n\nSecond body.\n",
    )
    .unwrap();
    let second_package = registry
        .publish_directory(PublishSkillPackageRequest {
            package_dir,
            name: None,
        })
        .unwrap();
    assert_ne!(
        first_package.active_artifact_hash,
        second_package.active_artifact_hash
    );

    let second_bound = bind_published_agent_record(
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
    let second_binding = &second_bound.bind_receipt.skill_packages[0];
    assert_eq!(second_binding.ref_uri, second_package.ref_uri());
    assert_eq!(
        second_binding.package_digest,
        format!("sha256:{}", second_package.active_artifact_hash)
    );
    assert_eq!(
        first_bound.bind_receipt.skill_packages[0], first_binding,
        "an existing bound thread must retain its witnessed version"
    );
    assert_eq!(
        first_bound.skill_context_segments[0].content,
        "alpha — First description.\n"
    );
    assert_eq!(
        second_bound.skill_context_segments[0].content,
        "alpha — Second description.\n"
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn floating_skill_resource_fails_closed_for_unknown_package_and_duplicate_skill_names() {
    let root = temp_dir("manifest-bind-floating-skill-failures");
    let skill_root = root.join("skills");
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);
    let unknown = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[[resources]]
name = "missing_skills"
kind = "skill"
ref = "skill://missing-skills"
"#,
            minimal_manifest("missing_skill_agent")
        ),
    );
    let err = bind_published_agent_record(
        &unknown,
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
    .unwrap_err()
    .to_string();
    assert!(err.contains("missing-skills"), "{err}");
    assert!(
        err.contains("not found in the local skill registry"),
        "{err}"
    );
    assert!(err.contains("verlet skill publish"), "{err}");
    assert!(!err.contains("replace the ref with a hash"), "{err}");

    let missing_hash = "0".repeat(64);
    let pinned_unknown = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[[resources]]
name = "missing_pinned_skills"
kind = "skill"
ref = "skill://missing-skills@sha256:{missing_hash}"
"#,
            minimal_manifest("missing_pinned_skill_agent")
        ),
    );
    let err = bind_published_agent_record(
        &pinned_unknown,
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
    .unwrap_err()
    .to_string();
    assert!(err.contains("publish the skill package"), "{err}");
    assert!(err.contains("replace the ref with a hash"), "{err}");

    for package_name in ["first-skills", "second-skills"] {
        let package_dir = root.join("skill-src").join(package_name);
        write_skill_file(
            &package_dir,
            "shared",
            &format!("# Shared\n\nDescription from {package_name}.\n"),
        );
        LocalSkillRegistry::new(&skill_root)
            .publish_directory(PublishSkillPackageRequest {
                package_dir,
                name: None,
            })
            .unwrap();
    }
    let duplicate = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[[resources]]
name = "first"
kind = "skill"
ref = "skill://first-skills"

[[resources]]
name = "second"
kind = "skill"
ref = "skill://second-skills"
"#,
            minimal_manifest("duplicate_skill_agent")
        ),
    );
    let err = bind_published_agent_record(
        &duplicate,
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
    .unwrap_err()
    .to_string();
    assert!(err.contains("duplicate /skills/shared.md"), "{err}");
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn workspace_skill_discovery_witnesses_entries_and_a_deterministic_index() {
    let root = temp_dir("manifest-bind-workspace-skill-discovery");
    let workspace = root.join("workspace");
    write_skill_file(
        &workspace.join(".agents/skills"),
        "zeta-dir",
        r#"---
name: alpha
description: Alpha workspace skill.
---
# Alpha

Alpha body.
"#,
    );
    write_skill_file(
        &workspace.join(".agents/skills"),
        "beta",
        "# Beta\n\nBeta workspace skill.\n\nBeta body.\n",
    );
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true
"#,
            minimal_manifest("workspace_skill_agent")
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);
    let workspace_binding = AgentManifestWorkspaceBinding {
        host_path: workspace.clone(),
        mode: AgentManifestWorkspaceMode::ReadWrite,
    };

    let bound = bind_published_agent_record_with_placement(
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
        None,
        None,
        None,
        Some(&workspace_binding),
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        bound.bind_receipt.workspace_origin,
        Some(AgentManifestBindingOrigin::BindOverride)
    );
    let discovery = bound
        .bind_receipt
        .skill_discovery
        .as_ref()
        .expect("enabled discovery must always be witnessed");
    assert_eq!(discovery.path, ".agents/skills");
    assert_eq!(discovery.skills.len(), 2);
    assert_eq!(discovery.skills[0].name, "alpha");
    assert_eq!(discovery.skills[0].path, ".agents/skills/zeta-dir/SKILL.md");
    assert_eq!(discovery.skills[0].description, "Alpha workspace skill.");
    assert!(discovery.skills[0].content_sha256.starts_with("sha256:"));
    assert_eq!(discovery.skills[1].name, "beta");
    assert_eq!(discovery.skills[1].path, ".agents/skills/beta/SKILL.md");

    let segment = bound.skill_context_segments.last().unwrap();
    assert_eq!(segment.id, "skill-discovery-index");
    assert_eq!(segment.assembler, KERNEL_ASSEMBLER_STATIC);
    assert_eq!(segment.input, ".agents/skills");
    assert!(segment.pinned);
    assert_eq!(
        segment.content,
        "alpha — Alpha workspace skill. — .agents/skills/zeta-dir/SKILL.md\n\
         beta — Beta workspace skill. — .agents/skills/beta/SKILL.md\n"
    );
    assert!(segment.content_sha256.starts_with("sha256:"));
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn workspace_skill_discovery_witnesses_missing_and_empty_directories() {
    for (label, create_directory) in [("missing", false), ("empty", true)] {
        let root = temp_dir(&format!("manifest-bind-workspace-skill-{label}"));
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        if create_directory {
            fs::create_dir_all(workspace.join("custom-skills")).unwrap();
        }
        let record = publish_agent_manifest(
            &root,
            &format!(
                r#"{}

[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true
path = "custom-skills"
"#,
                minimal_manifest(&format!("workspace_skill_{label}"))
            ),
        );
        let surface = AgentManifestProviderSurface::single("local_offline", "echo")
            .with_supports_streaming(false);
        let workspace_binding = AgentManifestWorkspaceBinding {
            host_path: workspace,
            mode: AgentManifestWorkspaceMode::ReadWrite,
        };

        let bound = bind_published_agent_record_with_placement(
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
            None,
            None,
            None,
            Some(&workspace_binding),
            false,
        )
        .await
        .unwrap();

        let discovery = bound.bind_receipt.skill_discovery.unwrap();
        assert_eq!(discovery.path, "custom-skills");
        assert!(discovery.skills.is_empty());
        assert_eq!(bound.skill_context_segments.len(), 1);
        assert!(bound.skill_context_segments[0].content.is_empty());
        let _ = fs::remove_dir_all(root);
    }
}

#[tokio::test]
async fn workspace_skill_discovery_fails_closed_on_duplicate_names() {
    let root = temp_dir("manifest-bind-workspace-skill-duplicates");
    let workspace = root.join("workspace");
    write_skill_file(
        &workspace.join(".agents/skills"),
        "first",
        "---\nname: shared\ndescription: First.\n---\nFirst body.\n",
    );
    write_skill_file(
        &workspace.join(".agents/skills"),
        "second",
        "---\nname: shared\ndescription: Second.\n---\nSecond body.\n",
    );
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true
"#,
            minimal_manifest("duplicate_workspace_skills")
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);
    let workspace_binding = AgentManifestWorkspaceBinding {
        host_path: workspace.clone(),
        mode: AgentManifestWorkspaceMode::ReadWrite,
    };

    let err = bind_published_agent_record_with_placement(
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
        None,
        None,
        None,
        Some(&workspace_binding),
        false,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("duplicate skill name \"shared\""), "{err}");

    fs::remove_dir_all(workspace.join(".agents/skills/second")).unwrap();
    let skill_registry_root = root.join("registry-skills");
    let package_dir = root.join("skill-src/registry-package");
    write_skill_file(
        &package_dir,
        "shared",
        "# Shared\n\nRegistry-bound description.\n",
    );
    LocalSkillRegistry::new(&skill_registry_root)
        .publish_directory(PublishSkillPackageRequest {
            package_dir,
            name: None,
        })
        .unwrap();
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true

[[resources]]
name = "registry_skills"
kind = "skill"
ref = "skill://registry-package"
"#,
            minimal_manifest("workspace_registry_duplicate_skills")
        ),
    );
    let err = bind_published_agent_record_with_placement(
        &record,
        None,
        &surface,
        None,
        None,
        Some(&skill_registry_root),
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
        None,
        None,
        None,
        Some(&workspace_binding),
        false,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("duplicate skill name \"shared\""), "{err}");
    assert!(err.contains("registry-bound skill packages"), "{err}");
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn workspace_skill_discovery_rehydrate_rejects_a_registry_duplicate() {
    let root = temp_dir("manifest-bind-workspace-skill-rehydrate-duplicate");
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let skill_registry_root = root.join("registry-skills");
    let package_dir = root.join("skill-src/registry-package");
    write_skill_file(
        &package_dir,
        "shared",
        "# Shared\n\nRegistry-bound description.\n",
    );
    LocalSkillRegistry::new(&skill_registry_root)
        .publish_directory(PublishSkillPackageRequest {
            package_dir,
            name: None,
        })
        .unwrap();
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true

[[resources]]
name = "registry_skills"
kind = "skill"
ref = "skill://registry-package"
"#,
            minimal_manifest("workspace_skill_rehydrate_duplicate")
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);
    let workspace_binding = AgentManifestWorkspaceBinding {
        host_path: workspace,
        mode: AgentManifestWorkspaceMode::ReadWrite,
    };
    let initial = bind_published_agent_record_with_placement(
        &record,
        None,
        &surface,
        None,
        None,
        Some(&skill_registry_root),
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
        None,
        None,
        None,
        Some(&workspace_binding),
        false,
    )
    .await
    .unwrap();
    let forged_discovery = AgentManifestSkillDiscovery {
        path: ".agents/skills".to_string(),
        skills: vec![AgentManifestDiscoveredSkill {
            name: "shared".to_string(),
            path: ".agents/skills/shared/SKILL.md".to_string(),
            content_sha256: format!("sha256:{}", "a".repeat(64)),
            description: "Discovered description.".to_string(),
        }],
    };

    let err = bind_published_agent_record_with_placement_and_skill_witness(
        &record,
        None,
        &surface,
        None,
        None,
        Some(&skill_registry_root),
        &BTreeSet::new(),
        None,
        &AgentManifestModelProfileSelection::default(),
        &AgentManifestBindOverrides::default(),
        None,
        None,
        None,
        Some(&workspace_binding),
        false,
        Some(&initial.skill_packages),
        Some(&forged_discovery),
        true,
        crate::kernel::history::now_ms(),
    )
    .await
    .unwrap_err()
    .to_string();

    assert!(err.contains("duplicate skill name \"shared\""), "{err}");
    assert!(err.contains("registry-bound skill packages"), "{err}");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn workspace_skill_discovery_witness_rejects_paths_fresh_discovery_cannot_emit() {
    let witness = AgentManifestSkillDiscovery {
        path: ".agents/skills".to_string(),
        skills: vec![AgentManifestDiscoveredSkill {
            name: "alpha".to_string(),
            path: ".agents/skills/alpha/nested/SKILL.md".to_string(),
            content_sha256: format!("sha256:{}", "a".repeat(64)),
            description: "Alpha skill.".to_string(),
        }],
    };

    let err = skill_context_segments_for_witnesses(&[], None, Some(&witness))
        .unwrap_err()
        .to_string();

    assert!(err.contains("path"), "{err}");
    assert!(err.contains("direct child"), "{err}");

    let witness = AgentManifestSkillDiscovery {
        path: "skills\nforged-index".to_string(),
        skills: Vec::new(),
    };
    let err = skill_context_segments_for_witnesses(&[], None, Some(&witness))
        .unwrap_err()
        .to_string();
    assert!(err.contains("unsafe"), "{err}");
}

#[test]
fn workspace_skill_discovery_witness_rejects_noncanonical_index_fields() {
    for (field, replacement) in [
        ("hash", format!("sha256:{}", "A".repeat(64))),
        ("name", "alpha\nforged — entry".to_string()),
        ("description", "Alpha skill.\nforged — entry".to_string()),
    ] {
        let mut skill = AgentManifestDiscoveredSkill {
            name: "alpha".to_string(),
            path: ".agents/skills/alpha/SKILL.md".to_string(),
            content_sha256: format!("sha256:{}", "a".repeat(64)),
            description: "Alpha skill.".to_string(),
        };
        match field {
            "hash" => skill.content_sha256 = replacement,
            "name" => skill.name = replacement,
            "description" => skill.description = replacement,
            _ => unreachable!(),
        }
        let witness = AgentManifestSkillDiscovery {
            path: ".agents/skills".to_string(),
            skills: vec![skill],
        };

        let err = skill_context_segments_for_witnesses(&[], None, Some(&witness))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("canonical") || err.contains("unsafe"),
            "expected {field} to fail canonical witness validation: {err}"
        );
    }
}

#[test]
fn workspace_skill_discovery_witness_must_match_the_compiled_manifest() {
    let root = temp_dir("manifest-bind-workspace-skill-manifest-witness");
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true
path = "expected-skills"
"#,
            minimal_manifest("workspace_skill_manifest_witness")
        ),
    );
    let (manifest, _) = compile_published_agent_record(&record, None).unwrap();
    let wrong_path = AgentManifestSkillDiscovery {
        path: "other-skills".to_string(),
        skills: Vec::new(),
    };

    let err = validate_skill_discovery_witness_for_manifest(&manifest, Some(&wrong_path))
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not match manifest path"), "{err}");
    let err = validate_skill_discovery_witness_for_manifest(&manifest, None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("has no skill discovery witness"), "{err}");
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn workspace_root_discovery_emits_a_normalized_relative_skill_path() {
    let root = temp_dir("manifest-bind-workspace-root-skill-discovery");
    let workspace = root.join("workspace");
    write_skill_file(&workspace, "alpha", "# Alpha\n\nAlpha workspace skill.\n");
    let record = publish_agent_manifest(
        &root,
        &format!(
            r#"{}

[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true
path = "."
"#,
            minimal_manifest("workspace_root_skill_agent")
        ),
    );
    let surface = AgentManifestProviderSurface::single("local_offline", "echo")
        .with_supports_streaming(false);
    let workspace_binding = AgentManifestWorkspaceBinding {
        host_path: workspace,
        mode: AgentManifestWorkspaceMode::ReadWrite,
    };

    let bound = bind_published_agent_record_with_placement(
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
        None,
        None,
        None,
        Some(&workspace_binding),
        false,
    )
    .await
    .unwrap();

    let discovery = bound.skill_discovery.unwrap();
    assert_eq!(discovery.path, ".");
    assert_eq!(discovery.skills[0].path, "alpha/SKILL.md");
    assert_eq!(bound.skill_context_segments[0].ref_uri, "workspace:///");
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn workspace_skill_read_rejects_a_symlink_swapped_after_open() {
    let root = temp_dir("manifest-bind-workspace-skill-swap");
    let workspace = root.join("workspace");
    let skill_dir = workspace.join(".agents/skills/alpha");
    let inside = workspace.join("inside.md");
    let outside = root.join("outside.md");
    fs::create_dir_all(&skill_dir).unwrap();
    fs::write(&inside, "inside").unwrap();
    fs::write(&outside, "outside").unwrap();
    let skill_file = skill_dir.join("SKILL.md");
    symlink(&outside, &skill_file).unwrap();
    let opened = fs::File::open(&skill_file).unwrap();
    fs::remove_file(&skill_file).unwrap();
    symlink(&inside, &skill_file).unwrap();

    let canonical_workspace = fs::canonicalize(&workspace).unwrap();
    let err = read_opened_workspace_skill_file(&canonical_workspace, &skill_file, opened)
        .unwrap_err()
        .to_string();

    assert!(err.contains("changed while it was opened"), "{err}");
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn workspace_skill_directory_listing_stays_on_the_opened_root() {
    let root = temp_dir("manifest-bind-workspace-skill-root-swap");
    let discovery_root = root.join("workspace/.agents/skills");
    let moved_root = root.join("workspace/.agents/original-skills");
    let outside_root = root.join("outside-skills");
    fs::create_dir_all(discovery_root.join("inside")).unwrap();
    fs::create_dir_all(outside_root.join("outside")).unwrap();
    let opened = fs::File::open(&discovery_root).unwrap();
    fs::rename(&discovery_root, &moved_root).unwrap();
    symlink(&outside_root, &discovery_root).unwrap();

    let names = read_opened_directory_names(&opened).unwrap();

    assert_eq!(names, vec![std::ffi::OsString::from("inside")]);
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn workspace_skill_discovery_confines_each_symlink_layer() {
    let root = temp_dir("manifest-bind-workspace-skill-symlinks");
    let workspace = root.join("workspace");
    let discovery_root = workspace.join(".agents/skills");
    let outside_root = root.join("outside-skills");
    write_skill_file(
        &outside_root,
        "outside",
        "# Outside\n\nOutside workspace skill.\n",
    );
    fs::create_dir_all(discovery_root.parent().unwrap()).unwrap();
    symlink(&outside_root, &discovery_root).unwrap();
    let resolved_workspace = AgentManifestResolvedWorkspaceMount {
        guest_path: PathBuf::from("/workspace"),
        host_path: fs::canonicalize(&workspace).unwrap(),
        mode: AgentManifestWorkspaceMode::ReadWrite,
    };

    let err = discover_workspace_skills(&resolved_workspace, ".agents/skills", &BTreeSet::new())
        .unwrap_err()
        .to_string();
    assert!(err.contains("outside the witnessed workspace"), "{err}");

    fs::remove_file(&discovery_root).unwrap();
    fs::create_dir_all(&discovery_root).unwrap();
    symlink(
        outside_root.join("outside"),
        discovery_root.join("linked-dir"),
    )
    .unwrap();
    let discovery =
        discover_workspace_skills(&resolved_workspace, ".agents/skills", &BTreeSet::new()).unwrap();
    assert!(discovery.skills.is_empty());

    let skill_dir = discovery_root.join("alpha");
    fs::create_dir_all(&skill_dir).unwrap();
    symlink(
        outside_root.join("outside/SKILL.md"),
        skill_dir.join("SKILL.md"),
    )
    .unwrap();
    let err = discover_workspace_skills(&resolved_workspace, ".agents/skills", &BTreeSet::new())
        .unwrap_err()
        .to_string();
    assert!(err.contains("outside the witnessed workspace"), "{err}");

    fs::remove_file(skill_dir.join("SKILL.md")).unwrap();
    let inside_body = workspace.join("inside-skill.md");
    fs::write(&inside_body, "# Alpha\n\nInside workspace skill.\n").unwrap();
    symlink(&inside_body, skill_dir.join("SKILL.md")).unwrap();
    let discovery =
        discover_workspace_skills(&resolved_workspace, ".agents/skills", &BTreeSet::new()).unwrap();
    assert_eq!(discovery.skills.len(), 1);
    assert_eq!(discovery.skills[0].name, "alpha");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn skill_binding_witness_comparison_is_order_independent_but_exact() {
    let alpha = AgentManifestSkillPackageBinding {
        resource_name: "alpha_resource".to_string(),
        package_name: "alpha-package".to_string(),
        ref_uri: format!("skill://alpha-package@sha256:{}", "a".repeat(64)),
        artifact_hash: "a".repeat(64),
        package_digest: format!("sha256:{}", "a".repeat(64)),
        skill_count: 1,
        index_sha256: format!("sha256:{}", "b".repeat(64)),
    };
    let beta = AgentManifestSkillPackageBinding {
        resource_name: "beta_resource".to_string(),
        package_name: "beta-package".to_string(),
        ref_uri: format!("skill://beta-package@sha256:{}", "c".repeat(64)),
        artifact_hash: "c".repeat(64),
        package_digest: format!("sha256:{}", "c".repeat(64)),
        skill_count: 2,
        index_sha256: format!("sha256:{}", "d".repeat(64)),
    };

    assert!(skill_package_bindings_match(
        &[alpha.clone(), beta.clone()],
        &[beta.clone(), alpha.clone()],
    ));
    assert!(!skill_package_bindings_match(
        &[alpha, beta.clone()],
        &[beta],
    ));
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
            effect_class: EffectClass::AtMostOnce,
            grants: vec!["net:https://example.com".to_string()],
            grant_expiries: Vec::new(),
            operations: Vec::new(),
            direct_tools: Vec::new(),
        }]
    );
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn bind_receipt_carries_future_expiry_and_excludes_expired_tool_rows() {
    let root = temp_dir("manifest-bind-grant-expiry");
    let operation_root = root.join("operations");
    let registry = LocalOperationRegistry::new(&operation_root);
    fs::create_dir_all(&operation_root).unwrap();
    let wasm = wat::parse_str(operation_guest_with_required_capability()).unwrap();
    let artifact = operation_root.join("search.wasm");
    fs::write(&artifact, wasm).unwrap();
    let operation = registry
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
    let manifest = format!(
        r#"{}

[[tools]]
type = "direct_tool"
id = "search"
tool_name = "search"
operation_ref = "op://search/search@sha256:{}"
grants = [
  {{ capability = "net:https://example.com", expires_at = "2026-07-16T20:00:00Z" }},
  {{ capability = "fs.read:/workspace", expires_at = "2026-07-16T21:00:00Z" }},
]
"#,
        minimal_manifest("grant-expiry"),
        operation.active_artifact_hash
    );
    let manifest_path = root.join("grant-expiry.verlet.agent.toml");
    fs::write(&manifest_path, manifest).unwrap();
    let record = crate::LocalAgentRegistry::new(root.join("agents"))
        .publish_manifest_path_with_operation_registry(&manifest_path, &operation_root)
        .unwrap();
    let surface = AgentManifestProviderSurface::single("local_offline", "echo");

    let future = bind_published_agent_record_at(
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
        1_784_231_999_000,
    )
    .await
    .unwrap();
    assert_eq!(future.bind_receipt.tool_ids, vec!["search"]);
    assert_eq!(future.bind_receipt.operation_bindings.len(), 1);
    assert_eq!(future.bind_receipt.grant_bindings.len(), 2);
    assert_eq!(
        future.bind_receipt.grant_bindings[0].expires_at.as_deref(),
        Some("2026-07-16T20:00:00Z")
    );
    assert!(!future.bind_receipt.grant_bindings[0].lapsed_at_bind);
    assert!(!future.bind_receipt.grant_bindings[0].surface_excluded);

    let expired = bind_published_agent_record_at(
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
        1_784_232_001_000,
    )
    .await
    .unwrap();
    assert!(expired.bind_receipt.tool_ids.is_empty());
    assert!(expired.bind_receipt.operation_bindings.is_empty());
    assert!(expired.operation_names.is_empty());
    assert_eq!(
        expired
            .bind_receipt
            .grant_bindings
            .iter()
            .filter(|binding| binding.lapsed_at_bind)
            .count(),
        1
    );
    assert!(
        expired
            .bind_receipt
            .grant_bindings
            .iter()
            .all(|binding| binding.surface_excluded)
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
            effect_class: EffectClass::AtMostOnce,
            grants: vec!["net:https://profile.example".to_string()],
            grant_expiries: Vec::new(),
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
            effect_class: EffectClass::AtMostOnce,
            grants: vec![
                "fs.read:/workspace".to_string(),
                "net:https://example.com".to_string(),
            ],
            grant_expiries: Vec::new(),
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
            effect_class: EffectClass::AtMostOnce,
            grants: vec![
                "net:https://profile.example".to_string(),
                "net:https://summary.example".to_string(),
            ],
            grant_expiries: Vec::new(),
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
            effect_class: EffectClass::AtMostOnce,
            grants: Vec::new(),
            grant_expiries: Vec::new(),
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
    let path = std::env::temp_dir().join(format!("verlet-{label}-{}", Uuid::now_v7()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn publish_agent_manifest(root: &Path, manifest: &str) -> crate::PublishedAgentRecord {
    let manifest_path = root.join("agent.verlet.agent.toml");
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
        effect_class: EffectClass::AtMostOnce,
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
              (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write
                drop
                i32.const 0)
              (func (export "__verlet_call_operation__")
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
              (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write
                drop
                i32.const 0)
              (func (export "__verlet_call_operation__")
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
fn raw_legacy_bind_receipt_decodes_without_optional_witnesses() {
    // This is the raw wire shape written before placement, workspace, skill
    // packages, and workspace skill discovery existed.
    let legacy_wire = format!(
        r#"{{
            "ref_uri":"cooldis://agents/karl",
            "manifest_hash":"sha256-manifest",
            "model_profile_id":"default",
            "provider_id":"anthropic",
            "model_id":"claude-sonnet-5",
            "tool_ids":["threads/spawn"],
            "operation_bindings":[],
            "granted":["{THREADS_SPAWN_CAPABILITY}"],
            "effective_runtime":{{
                "default_cwd":"workspace",
                "streaming":true,
                "turn_timeout_ms":1000,
                "cancellation_grace_ms":null,
                "compaction":{{"auto_at_text_bytes":500}},
                "overrides":{{"allow":[]}}
            }},
            "overridden_keys":[]
        }}"#
    );
    let legacy_receipt: AgentManifestBindReceipt = serde_json::from_str(&legacy_wire).unwrap();
    assert_eq!(legacy_receipt.placement, None);
    assert_eq!(legacy_receipt.model_profile_origin, None);
    assert_eq!(legacy_receipt.placement_origin, None);
    assert_eq!(legacy_receipt.workspace, None);
    assert_eq!(legacy_receipt.workspace_origin, None);
    assert_eq!(legacy_receipt.skill_discovery, None);
    assert_eq!(legacy_receipt.ref_uri, "cooldis://agents/karl");
    assert_eq!(legacy_receipt.tool_ids, vec!["threads/spawn"]);
    let legacy_value = serde_json::to_value(&legacy_receipt).unwrap();
    assert!(legacy_value.get("model_profile_origin").is_none());
    assert!(legacy_value.get("placement_origin").is_none());
    assert!(legacy_value.get("workspace_origin").is_none());
    assert!(
        serde_json::to_value(&legacy_receipt)
            .unwrap()
            .get("placement")
            .is_none(),
        "absent placement must serialize to the legacy wire shape"
    );
    assert!(
        serde_json::to_value(&legacy_receipt)
            .unwrap()
            .get("workspace")
            .is_none(),
        "absent workspace must serialize to the legacy wire shape"
    );
    assert!(
        serde_json::to_value(&legacy_receipt)
            .unwrap()
            .get("skill_discovery")
            .is_none(),
        "absent skill discovery must serialize to the legacy wire shape"
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

#[test]
fn placement_resolution_defaults_local_and_rpc_override_wins() {
    let default = AgentManifestPlacementBinding {
        target: crate::PlacementTarget::Sandbox,
        executor_ref: Some("executor://sandbox/default".to_string()),
        config: BTreeMap::from([("pool".to_string(), serde_json::json!("ci"))]),
    };
    let rpc_override = AgentManifestPlacementBinding {
        target: crate::PlacementTarget::Local,
        executor_ref: None,
        config: BTreeMap::new(),
    };

    assert_eq!(
        resolve_manifest_placement(None, None, false).unwrap(),
        AgentManifestPlacementBinding::default()
    );
    assert_eq!(
        resolve_manifest_placement(Some(&default), Some(&rpc_override), false).unwrap(),
        rpc_override
    );
    assert_eq!(
        resolve_manifest_placement_with_origin(None, None, false)
            .unwrap()
            .origin,
        AgentManifestBindingOrigin::DaemonDefault
    );
    assert_eq!(
        resolve_manifest_placement_with_origin(Some(&default), Some(&rpc_override), false)
            .unwrap()
            .origin,
        AgentManifestBindingOrigin::BindOverride
    );
}

#[test]
fn placement_resolution_opens_remote_only_for_a_served_sync_backend() {
    let unconfigured_message = "runtime factory failed: placement target remote requires the remote EventStore backend capability, which is not available";
    for served in [false, true] {
        let requested = AgentManifestPlacementBinding {
            target: crate::PlacementTarget::Remote,
            executor_ref: Some("executor://future".to_string()),
            config: BTreeMap::new(),
        };
        match served {
            true => assert_eq!(
                resolve_manifest_placement(Some(&requested), None, served).unwrap(),
                requested
            ),
            false => assert_eq!(
                resolve_manifest_placement(Some(&requested), None, served)
                    .unwrap_err()
                    .to_string(),
                unconfigured_message
            ),
        }
    }

    let sandbox = AgentManifestPlacementBinding {
        target: crate::PlacementTarget::Sandbox,
        executor_ref: Some("executor://future".to_string()),
        config: BTreeMap::new(),
    };
    for served in [false, true] {
        assert_eq!(
            resolve_manifest_placement(Some(&sandbox), None, served)
                .unwrap_err()
                .to_string(),
            "runtime factory failed: placement target sandbox requires the remote EventStore backend capability, which is not available"
        );
    }
}
