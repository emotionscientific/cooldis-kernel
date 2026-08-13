fn parse(source: &str) -> crate::VerletResult<crate::manifest_schema::AgentManifestSchema> {
    let value = toml::from_str(source).unwrap();
    crate::manifest_schema::AgentManifestSchema::from_toml_value(&value)
}

fn valid_manifest() -> String {
    let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    format!(
        r#"
[agent]
name = "release-verifier"
namespace = "verlet/labs"
version = "1.0.0"
display_name = "Release Verifier"
description = "Checks a release."
kind = "verlet.agent-manifest"
schema_version = 1
labels = {{ role = "review" }}
publisher = {{ id = "verlet", display_name = "Verlet" }}

[[model_profiles]]
id = "default"
provider_ref = "provider://openai_compatible"
model_ref = "model://example-chat-model"
credentials = {{ ref = "credential://openai_compatible/default" }}
retry = {{ max_attempts = 2, backoff_ms = 25 }}
fallbacks = [{{ provider_ref = "provider://backup", model_ref = "model://backup" }}]

[model_profiles.params]
max_tokens = 4096
temperature = 0.2
reasoning_effort = "medium"

[[tools]]
type = "bash_tool"
id = "tailcat"
command = "tailcat"
operation_ref = "op://tailcat@sha256:{hash}"

[[tools]]
type = "direct_tool"
id = "risk_lookup"
tool_name = "risk_lookup"
operation_ref = "op://risk-lookup@sha256:{hash}"

[[tools]]
type = "protocol_tool_import"
id = "mcp_docs"
protocol = "mcp"
server_ref = "mcp://docs"
include_tools = ["docs.search"]

[[resources]]
name = "system_prompt"
kind = "blob"
ref = "resource://system-prompt@sha256:{hash}"
mount = "context"
mode = "read"

[context]
[[context.pipelines]]
id = "default"

[[context.pipelines.sources]]
id = "system"
assembler = "{KERNEL_ASSEMBLER_STATIC}"
input = "system_prompt"
pinned = true

[[context.pipelines.sources]]
id = "records"
assembler = "{KERNEL_ASSEMBLER_RECORD_SELECT}"
select = {{ kind = ["observation"], stream = "events" }}
budget_share = 0.25

[[context.pipelines.sources]]
id = "history"
assembler = "{KERNEL_ASSEMBLER_ANCHORED_WINDOW}"
select = {{ stream = "thread", since = "anchor|start" }}
budget_share = "rest"

[policies]
filesystem = "vfs"
allow_child_agents = true

[policies.budgets]
max_turns = 8
max_tool_calls_per_turn = 4

[runtime]
default_cwd = "workspace"
streaming = true
max_tool_rounds = 64
turn_timeout_ms = 1000
cancellation_grace_ms = 100

[runtime.compaction]
auto_at_text_bytes = 1024

[runtime.overrides]
allow = ["default_cwd", "streaming", "compaction.auto_at_text_bytes"]
"#,
        KERNEL_ASSEMBLER_STATIC = crate::manifest_schema::KERNEL_ASSEMBLER_STATIC,
        KERNEL_ASSEMBLER_RECORD_SELECT = crate::manifest_schema::KERNEL_ASSEMBLER_RECORD_SELECT,
        KERNEL_ASSEMBLER_ANCHORED_WINDOW = crate::manifest_schema::KERNEL_ASSEMBLER_ANCHORED_WINDOW,
    )
}

#[test]
fn manifest_operation_bindings_reject_legacy_grants() {
    let source = valid_manifest().replace(
        "operation_ref = \"op://tailcat@sha256:",
        "grants = [\"fs.read:/workspace\"]\noperation_ref = \"op://tailcat@sha256:",
    );

    let err = parse(&source).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("unknown field"), "{text}");
    assert!(text.contains("grants"), "{text}");
}

#[test]
fn manifest_policies_reject_legacy_network_declaration() {
    let source = valid_manifest().replace(
        "[policies]\n",
        "[policies]\nnetwork = \"declared-origins\"\n",
    );
    let err = parse(&source).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("unknown field"), "{text}");
    assert!(text.contains("network"), "{text}");
}

fn manifest_with_coupling() -> String {
    let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    valid_manifest()
        + &format!(
            r#"
[[couplings]]
id = "bash_regex_gate"
function_ref = "op://policy/regex-tool-gate@sha256:{hash}"

[couplings.trigger]
kind = "tool.call.requested"
quota.per_turn = 16

[couplings.trigger.match]
tool = "bash"

[[couplings.source.selectors]]
stream = "thread"
kind = "tool.call.requested"
since = "turn:start"

[couplings.sink]
stream = "control"
kind = ["approval.requested", "tool.call.decision"]

[couplings.budget]
max_ms = 5000
max_discharge_events = 2

[couplings.config]
argument_path = "/command"
"#
        )
}

#[test]
fn full_fixture_manifest_parses_and_validates() {
    let manifest = parse(&valid_manifest()).unwrap();

    assert_eq!(manifest.identity.name, "release-verifier");
    assert_eq!(manifest.model_profiles.len(), 1);
    assert_eq!(manifest.tools.len(), 3);
    assert_eq!(manifest.resources.len(), 1);
    assert!(manifest.workspace.is_none());
    assert!(manifest.couplings.is_empty());
    assert_eq!(manifest.effective_context_pipeline().sources.len(), 3);
    assert!(manifest.tools.iter().all(|tool| match tool {
        crate::manifest_schema::AgentManifestTool::Bash(tool) =>
            tool.effect_class == crate::manifest_schema::EffectClass::AtMostOnce,
        crate::manifest_schema::AgentManifestTool::Direct(tool) =>
            tool.effect_class == crate::manifest_schema::EffectClass::AtMostOnce,
        crate::manifest_schema::AgentManifestTool::ProtocolImport(tool) => {
            tool.effect_class == crate::manifest_schema::EffectClass::AtMostOnce
        }
    }));
    assert!(
        !toml::to_string(&manifest).unwrap().contains("effect_class"),
        "legacy manifests must not acquire defaulted effect_class fields when re-encoded"
    );
}

#[test]
fn operation_attachment_config_parses_with_the_wasm_attachment_shape() {
    let source = valid_manifest().replace(
        "operation_ref = \"op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"",
        r#"operation_ref = "op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[tools.attachment]
allowed_secrets = ["WORKSPACE_TOKEN"]

[tools.attachment.allowed_private_network]
"https://internal.example.test" = ["GET"]"#,
    );

    let manifest = parse(&source).unwrap();
    let encoded = serde_json::to_value(&manifest).unwrap();
    assert_eq!(
        encoded["tools"][0]["attachment"],
        serde_json::json!({
            "allowed_secrets": ["WORKSPACE_TOKEN"],
            "allowed_private_network": {
                "https://internal.example.test": ["GET"]
            }
        })
    );
}

#[test]
fn operation_attachment_config_rejects_unknown_fields() {
    let source = valid_manifest().replace(
        "operation_ref = \"op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"",
        r#"operation_ref = "op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[tools.attachment]
allowed_secrets = ["WORKSPACE_TOKEN"]
allow_all_secrets = true"#,
    );

    let err = parse(&source).unwrap_err().to_string();
    assert!(err.contains("unknown field"), "{err}");
    assert!(err.contains("allow_all_secrets"), "{err}");
}

#[test]
fn operation_attachment_config_rejects_duplicate_keys() {
    let source = valid_manifest().replace(
        "operation_ref = \"op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"",
        r#"operation_ref = "op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[tools.attachment]
allowed_secrets = ["WORKSPACE_TOKEN"]
allowed_secrets = ["SECOND_TOKEN"]"#,
    );

    let err = toml::from_str::<toml::Value>(&source)
        .unwrap_err()
        .to_string();
    assert!(err.contains("duplicate key"), "{err}");
    assert!(err.contains("allowed_secrets"), "{err}");
}

#[test]
fn operation_attachment_config_distinguishes_absent_and_explicit_empty() {
    let absent = parse(&valid_manifest()).unwrap();
    let source = valid_manifest().replace(
        "operation_ref = \"op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"",
        r#"operation_ref = "op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[tools.attachment]"#,
    );
    let explicit_empty = parse(&source).unwrap();

    let crate::manifest_schema::AgentManifestTool::Bash(absent_tool) = &absent.tools[0] else {
        panic!("expected bash tool");
    };
    let crate::manifest_schema::AgentManifestTool::Bash(explicit_tool) = &explicit_empty.tools[0]
    else {
        panic!("expected bash tool");
    };
    assert!(absent_tool.attachment.is_empty());
    assert!(explicit_tool.attachment.is_empty());
    assert!(
        !toml::to_string(&explicit_empty)
            .unwrap()
            .contains("attachment"),
        "an explicit empty attachment remains default-deny and canonicalizes away"
    );
}

#[test]
fn operation_attachment_config_round_trips_exactly() {
    let source = valid_manifest().replace(
        "operation_ref = \"op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"",
        r#"operation_ref = "op://tailcat@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[tools.attachment]
allowed_secrets = ["WORKSPACE_TOKEN"]

[tools.attachment.allowed_private_network]
"https://internal.example.test" = ["GET", "POST"]"#,
    );

    let manifest = parse(&source).unwrap();
    let encoded = serde_json::to_value(&manifest).unwrap();
    let reparsed: crate::manifest_schema::AgentManifestSchema =
        serde_json::from_value(encoded).unwrap();
    assert_eq!(reparsed, manifest);
}

#[test]
fn agent_manifest_kind_rejects_legacy_form() {
    let canonical = valid_manifest();
    assert_eq!(
        parse(&canonical).unwrap().identity.kind.as_deref(),
        Some("verlet.agent-manifest")
    );

    let legacy = canonical.replace(
        "kind = \"verlet.agent-manifest\"",
        &format!("kind = \"{}\"", concat!("cool", "dis.agent-manifest")),
    );
    let err = parse(&legacy).unwrap_err();
    assert!(err.to_string().contains("verlet.agent-manifest"));
    assert!(
        err.to_string()
            .contains(concat!("cool", "dis.agent-manifest"))
    );

    let unsupported = canonical.replace(
        "kind = \"verlet.agent-manifest\"",
        "kind = \"other.agent-manifest\"",
    );
    let err = parse(&unsupported).unwrap_err();
    assert!(err.to_string().contains("verlet.agent-manifest"));
}

#[test]
fn tool_rows_accept_effect_classes_and_reject_unknown_values_by_row() {
    let source = valid_manifest()
        .replace(
            "command = \"tailcat\"",
            "command = \"tailcat\"\neffect_class = \"pure\"",
        )
        .replace(
            "tool_name = \"risk_lookup\"",
            "tool_name = \"risk_lookup\"\neffect_class = \"idempotent\"",
        )
        .replace(
            "server_ref = \"mcp://docs\"",
            "server_ref = \"mcp://docs\"\neffect_class = \"at-most-once\"",
        );
    let manifest = parse(&source).unwrap();
    assert!(matches!(
        &manifest.tools[0],
        crate::manifest_schema::AgentManifestTool::Bash(tool) if tool.effect_class == crate::manifest_schema::EffectClass::Pure
    ));
    assert!(matches!(
        &manifest.tools[1],
        crate::manifest_schema::AgentManifestTool::Direct(tool) if tool.effect_class == crate::manifest_schema::EffectClass::Idempotent
    ));
    assert!(matches!(
        &manifest.tools[2],
        crate::manifest_schema::AgentManifestTool::ProtocolImport(tool) if tool.effect_class == crate::manifest_schema::EffectClass::AtMostOnce
    ));

    let err = parse(&valid_manifest().replace(
        "tool_name = \"risk_lookup\"",
        "tool_name = \"risk_lookup\"\neffect_class = \"retryable\"",
    ))
    .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("tool \"risk_lookup\""), "{text}");
    assert!(text.contains("effect_class"), "{text}");
    assert!(text.contains("retryable"), "{text}");
}

#[test]
fn workspace_requirement_parses_without_a_host_path() {
    let source = valid_manifest().replace(
        "[policies]",
        r#"[workspace]
guest_path = "/workspace"
min_mode = "rw"

[policies]"#,
    );

    let manifest = parse(&source).unwrap();
    let workspace = manifest.workspace.expect("workspace requirement");

    assert_eq!(workspace.guest_path, "/workspace");
    assert_eq!(
        workspace.min_mode,
        crate::manifest_schema::AgentManifestWorkspaceMode::ReadWrite
    );
    assert!(
        !serde_json::to_value(workspace)
            .unwrap()
            .to_string()
            .contains("host")
    );
}

#[test]
fn workspace_requirement_rejects_unsafe_or_reserved_guest_paths() {
    for (guest_path, expected) in [
        ("workspace", "absolute"),
        ("/", "must not be /"),
        ("/workspace/../outside", "normalized"),
        ("/skills", "reserved"),
        ("/skills/nested", "reserved"),
        ("/spill", "reserved"),
        ("/spill/nested", "reserved"),
    ] {
        let source = valid_manifest().replace(
            "[policies]",
            &format!("[workspace]\nguest_path = {guest_path:?}\nmin_mode = \"ro\"\n\n[policies]"),
        );

        let err = parse(&source).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected {expected:?} for {guest_path:?}, got {err}"
        );
    }
}

#[test]
fn skill_discovery_defaults_off_and_accepts_a_workspace_relative_path() {
    let default_manifest = parse(&valid_manifest()).unwrap();
    assert!(!default_manifest.skills.discover);
    assert_eq!(default_manifest.skills.path, ".agents/skills");
    assert!(
        serde_json::to_value(&default_manifest)
            .unwrap()
            .get("skills")
            .is_none(),
        "an omitted [skills] section must retain the legacy resolved-manifest shape"
    );

    let source = valid_manifest().replace(
        "[policies]",
        r#"[workspace]
guest_path = "/workspace"
min_mode = "rw"

[skills]
discover = true
path = "project-skills"

[policies]"#,
    );
    let manifest = parse(&source).unwrap();
    assert!(manifest.skills.discover);
    assert_eq!(manifest.skills.path, "project-skills");
}

#[test]
fn skill_discovery_rejects_unsafe_paths_and_requires_a_workspace() {
    let without_workspace =
        valid_manifest().replace("[policies]", "[skills]\ndiscover = true\n\n[policies]");
    let err = parse(&without_workspace).unwrap_err().to_string();
    assert!(err.contains("skills.discover = true"), "{err}");
    assert!(err.contains("workspace requirement"), "{err}");

    for path in [
        "/tmp/skills",
        "../skills",
        "project/../../skills",
        "skills\nignore-the-index",
        "",
    ] {
        let source = valid_manifest().replace(
            "[policies]",
            &format!(
                "[workspace]\nguest_path = \"/workspace\"\nmin_mode = \"rw\"\n\n[skills]\ndiscover = true\npath = {path:?}\n\n[policies]"
            ),
        );
        let err = parse(&source).unwrap_err().to_string();
        assert!(
            err.contains("skills.path"),
            "expected path error for {path:?}: {err}"
        );
        assert!(
            err.contains("workspace-relative") || err.contains("must not contain `..`"),
            "expected actionable path error for {path:?}: {err}"
        );
    }
}

#[test]
fn coupling_rows_parse_and_validate_shape() {
    let manifest = parse(&manifest_with_coupling()).unwrap();

    assert_eq!(manifest.couplings.len(), 1);
    let coupling = &manifest.couplings[0];
    assert_eq!(coupling.id, "bash_regex_gate");
    assert_eq!(coupling.trigger.kind, "tool.call.requested");
    assert_eq!(coupling.trigger.quota.per_turn, Some(16));
    assert_eq!(
        coupling.trigger.match_fields.get("tool").unwrap(),
        &serde_json::json!("bash")
    );
    assert_eq!(
        coupling.source.selectors[0].kind,
        vec!["tool.call.requested"]
    );
    assert_eq!(
        coupling.sink.kind,
        vec![
            "approval.requested".to_string(),
            "tool.call.decision".to_string()
        ]
    );
    assert_eq!(coupling.budget.max_discharge_events, Some(2));
    assert_eq!(coupling.config["argument_path"], "/command");
}

#[test]
fn coupling_ids_allow_canonical_stdlib_template_ids() {
    let manifest = parse(
        &manifest_with_coupling()
            .replace("id = \"bash_regex_gate\"", "id = \"std::context.spill\""),
    )
    .unwrap();

    assert_eq!(manifest.couplings[0].id, "std::context.spill");
}

#[test]
fn missing_context_synthesizes_default_pipeline() {
    let source = valid_manifest()
        .split("[context]")
        .next()
        .unwrap()
        .to_string()
        + r#"
[policies]

[runtime]
"#;
    let manifest = parse(&source).unwrap();
    let pipeline = manifest.effective_context_pipeline();

    assert_eq!(pipeline.id, "default");
    assert_eq!(pipeline.sources[0].id, "identity");
    assert_eq!(pipeline.sources[0].input, None);
    assert!(pipeline.sources[0].pinned);
    assert_eq!(pipeline.sources[1].id, "history");
    assert_eq!(
        pipeline.sources[1].budget_share,
        Some(crate::manifest_schema::AgentManifestBudgetShare::Rest(
            crate::manifest_schema::AgentManifestBudgetRest::Rest
        ))
    );
}

#[test]
fn reserved_and_unknown_sections_fail_closed() {
    let err = parse("[agent]\nname = \"a\"\n[hooks]\n").unwrap_err();
    assert!(err.to_string().contains("reserved"));
    assert!(err.to_string().contains("deferred"));

    let err = parse("[agent]\nname = \"a\"\n[mystery]\n").unwrap_err();
    assert!(err.to_string().contains("unknown top-level"));
}

#[test]
fn coupling_rows_reject_role_duplicate_ids_and_source_sink_identity() {
    let err = parse(
        &manifest_with_coupling()
            .replace("function_ref =", "role = \"controller\"\nfunction_ref ="),
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field"));
    assert!(err.to_string().contains("role"));

    let err = parse(&(manifest_with_coupling() + "\n[[couplings]]\nid = \"bash_regex_gate\"\nfunction_ref = \"op://policy/other@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"\n[couplings.trigger]\nkind = \"turn.completed\"\n[[couplings.source.selectors]]\nstream = \"thread\"\nkind = \"turn.completed\"\n[couplings.sink]\nstream = \"control\"\nkind = \"loop.completed\"\n"))
        .unwrap_err();
    assert!(err.to_string().contains("duplicate coupling id"));

    let err = parse(&manifest_with_coupling().replace(
        "stream = \"control\"\nkind = [\"approval.requested\", \"tool.call.decision\"]",
        "stream = \"thread\"\nkind = [\"approval.requested\", \"tool.call.decision\"]",
    ))
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("sink must not equal selected source")
    );
}

#[test]
fn nested_unknown_keys_fail_with_section_name() {
    let mut source = valid_manifest();
    source = source.replace(
        "description = \"Checks a release.\"",
        "description = \"Checks a release.\"\nextra = true",
    );

    let err = parse(&source).unwrap_err();
    assert!(err.to_string().contains("[agent]"));
    assert!(err.to_string().contains("unknown field"));
}

#[test]
fn reserved_and_deferred_resource_kinds_are_named() {
    let err =
        parse(&valid_manifest().replace("kind = \"blob\"", "kind = \"dataset\"")).unwrap_err();
    assert!(err.to_string().contains("dataset"));
    assert!(err.to_string().contains("deferred"));

    let skill_ref = "skill://karl-skills@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let manifest = parse(
        &valid_manifest()
            .replace("kind = \"blob\"", "kind = \"skill\"")
            .replace(
                "resource://system-prompt@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                skill_ref,
            ),
    )
    .unwrap();
    assert_eq!(manifest.resources[0].reference, skill_ref);

    let floating_ref = "skill://karl-skills";
    let manifest = parse(
        &valid_manifest()
            .replace("kind = \"blob\"", "kind = \"skill\"")
            .replace(
                "resource://system-prompt@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                floating_ref,
            ),
    )
    .unwrap();
    assert_eq!(manifest.resources[0].reference, floating_ref);

    let malformed_hash = parse(
        &valid_manifest()
            .replace("kind = \"blob\"", "kind = \"skill\"")
            .replace(
                "resource://system-prompt@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "skill://karl-skills@sha256:short",
            ),
    )
    .unwrap_err()
    .to_string();
    assert!(
        malformed_hash.contains("artifact hash") && malformed_hash.contains("sha256 hex digest"),
        "{malformed_hash}"
    );

    let err = parse(&valid_manifest().replace("kind = \"blob\"", "kind = \"skill\"")).unwrap_err();
    assert!(err.to_string().contains("skill://"));
}

#[test]
fn resolved_ref_validation_uses_the_declared_skill_ref_authority() {
    let malformed = crate::manifest_schema::AgentManifestResolvedRef {
        declared: "skill://karl-skills@sha256:garbage".to_string(),
        resolved: None,
        content_hash: None,
        status: crate::manifest_schema::AgentManifestRefStatus::UnresolvedOffline,
    };

    let err = malformed.validate().unwrap_err().to_string();
    assert!(err.contains("skill package artifact hash"), "{err}");
    assert!(err.contains("sha256 hex digest"), "{err}");

    let floating = crate::manifest_schema::AgentManifestResolvedRef {
        declared: "skill://karl-skills".to_string(),
        resolved: None,
        content_hash: None,
        status: crate::manifest_schema::AgentManifestRefStatus::UnresolvedOffline,
    };
    let err = floating.validate().unwrap_err().to_string();
    assert!(err.contains("bind time"), "{err}");
    assert!(err.contains("resolved_refs"), "{err}");

    let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let resolved_to_floating = crate::manifest_schema::AgentManifestResolvedRef {
        declared: format!("resource://system-prompt@sha256:{hash}"),
        resolved: Some("skill://karl-skills".to_string()),
        content_hash: Some(format!("sha256:{hash}")),
        status: crate::manifest_schema::AgentManifestRefStatus::Resolved,
    };
    let err = resolved_to_floating.validate().unwrap_err().to_string();
    assert!(err.contains("bind time"), "{err}");
    assert!(err.contains("resolved_refs"), "{err}");
}

#[test]
fn context_budget_and_assembler_rules_are_enforced() {
    let cases = [
        (
            "id = \"default\"",
            "id = \"other\"",
            "pipeline id must be \"default\"",
        ),
        (
            &format!(
                "assembler = \"{KERNEL_ASSEMBLER_STATIC}\"\ninput = \"system_prompt\"",
                KERNEL_ASSEMBLER_STATIC = crate::manifest_schema::KERNEL_ASSEMBLER_STATIC
            ),
            "assembler = \"kernel://assembler/other\"\ninput = \"system_prompt\"",
            "not a V1 kernel assembler",
        ),
        (
            "input = \"system_prompt\"\npinned = true",
            "pinned = true",
            "requires input",
        ),
        (
            "pinned = true",
            "pinned = true\nbudget_share = 0.1",
            "must not declare budget_share",
        ),
        ("budget_share = 0.25", "", "must declare budget_share"),
        (
            "budget_share = 0.25",
            "budget_share = 0.0",
            "fraction must be in (0, 1]",
        ),
    ];

    for (from, to, expected) in cases {
        let err = parse(&valid_manifest().replace(from, to)).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected {expected:?}, got {err}"
        );
    }

    let err = parse(&valid_manifest().replace("budget_share = 0.25", "budget_share = \"rest\""))
        .unwrap_err();
    assert!(err.to_string().contains("at most one rest"));

    let err = parse(&valid_manifest().replace("budget_share = \"rest\"", "budget_share = 0.8"))
        .unwrap_err();
    assert!(err.to_string().contains("expected <= 1.0"));
}

#[test]
fn source_refs_are_shape_validated_and_legacy_tool_refs_are_rejected() {
    let err = parse(&valid_manifest().replace(
        "provider_ref = \"provider://openai_compatible\"",
        "provider_ref = \"openai_compatible\"",
    ))
    .unwrap_err();
    assert!(err.to_string().contains("provider://"));

    let err = parse(&valid_manifest().replace(
        "operation_ref = \"op://tailcat@",
        "operation_ref = \"tool://tailcat@",
    ))
    .unwrap_err();
    assert!(err.to_string().contains("op://"));
}

#[test]
fn protocol_tool_import_server_ref_rejects_content_addressed_source_record() {
    let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let err = parse(&valid_manifest().replace(
        "server_ref = \"mcp://docs\"",
        &format!("server_ref = \"mcp://docs@sha256:{hash}\""),
    ))
    .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("source refs name placement"));
    assert!(text.contains("mcp://<source-name>"));
    assert!(text.contains("mcptool://"));
    assert!(text.contains("not the source record"));
}

#[test]
fn runtime_numeric_fields_must_be_positive() {
    let err = parse(&valid_manifest().replace("turn_timeout_ms = 1000", "turn_timeout_ms = 0"))
        .unwrap_err();
    assert!(err.to_string().contains("turn_timeout_ms must be > 0"));

    let err = parse(&valid_manifest().replace("max_tool_rounds = 64", "max_tool_rounds = 0"))
        .unwrap_err();
    assert!(err.to_string().contains("max_tool_rounds must be > 0"));
}

#[test]
fn runtime_tool_round_budget_supports_finite_unlimited_and_absent() {
    let finite = parse(&valid_manifest()).unwrap();
    assert_eq!(
        finite.runtime.max_tool_rounds,
        Some(crate::manifest_schema::AgentManifestMaxToolRounds::Limited(
            64
        ))
    );

    let unlimited =
        parse(&valid_manifest().replace("max_tool_rounds = 64", "max_tool_rounds = \"unlimited\""))
            .unwrap();
    assert_eq!(
        unlimited.runtime.max_tool_rounds,
        Some(crate::manifest_schema::AgentManifestMaxToolRounds::Unlimited)
    );

    let absent = parse(&valid_manifest().replace("max_tool_rounds = 64\n", "")).unwrap();
    assert_eq!(absent.runtime.max_tool_rounds, None);

    let err =
        parse(&valid_manifest().replace("max_tool_rounds = 64", "max_tool_rounds = \"forever\""))
            .unwrap_err();
    assert!(err.to_string().contains("max_tool_rounds"));
}

#[test]
fn runtime_override_key_and_workspace_mode_names_are_pinned() {
    let keys: Vec<&'static str> = [
        crate::manifest_schema::AgentManifestRuntimeOverrideKey::DefaultCwd,
        crate::manifest_schema::AgentManifestRuntimeOverrideKey::Streaming,
        crate::manifest_schema::AgentManifestRuntimeOverrideKey::TurnTimeoutMs,
        crate::manifest_schema::AgentManifestRuntimeOverrideKey::CancellationGraceMs,
        crate::manifest_schema::AgentManifestRuntimeOverrideKey::MaxToolRounds,
        crate::manifest_schema::AgentManifestRuntimeOverrideKey::CompactionAutoAtTextBytes,
    ]
    .into_iter()
    .map(std::convert::Into::into)
    .collect();
    assert_eq!(
        keys,
        vec![
            "default_cwd",
            "streaming",
            "turn_timeout_ms",
            "cancellation_grace_ms",
            "max_tool_rounds",
            "compaction.auto_at_text_bytes",
        ]
    );

    let read_only: &'static str =
        crate::manifest_schema::AgentManifestWorkspaceMode::ReadOnly.into();
    let read_write: &'static str =
        crate::manifest_schema::AgentManifestWorkspaceMode::ReadWrite.into();
    assert_eq!(read_only, "ro");
    assert_eq!(read_write, "rw");
}
