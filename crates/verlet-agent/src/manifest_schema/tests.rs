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
kind = "cooldis.agent-manifest"
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
grants = ["fs.read:/workspace"]

[[tools]]
type = "direct_tool"
id = "risk_lookup"
tool_name = "risk_lookup"
operation_ref = "op://risk-lookup@sha256:{hash}"
grants = ["net.http:GET:https://example.test"]

[[tools]]
type = "protocol_tool_import"
id = "mcp_docs"
protocol = "mcp"
server_ref = "mcp://docs"
include_tools = ["docs.search"]
grants = ["net.localhost"]

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
network = "declared-origins"
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

fn manifest_with_coupling() -> String {
    let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    valid_manifest()
        + &format!(
            r#"
[[couplings]]
id = "bash_regex_gate"
function_ref = "op://policy/regex-tool-gate@sha256:{hash}"
grants = ["stream.read:thread", "stream.write:control"]

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
fn grants_accept_legacy_strings_and_expiring_objects_everywhere() {
    let expires_at = "2026-07-16T20:00:00Z";
    let source = manifest_with_coupling()
        .replace(
            "grants = [\"fs.read:/workspace\"]",
            &format!(
                "grants = [\"fs.read:/workspace\", {{ capability = \"fs.write:/workspace\", expires_at = \"{expires_at}\" }}]"
            ),
        )
        .replace(
            "grants = [\"net.http:GET:https://example.test\"]",
            &format!(
                "grants = [{{ capability = \"net.http:GET:https://example.test\", expires_at = \"{expires_at}\" }}]"
            ),
        )
        .replace(
            "grants = [\"net.localhost\"]",
            &format!(
                "grants = [{{ capability = \"net.localhost\", expires_at = \"{expires_at}\" }}]"
            ),
        )
        .replace(
            "grants = [\"stream.read:thread\", \"stream.write:control\"]",
            &format!(
                "grants = [\"stream.read:thread\", {{ capability = \"stream.write:control\", expires_at = \"{expires_at}\" }}]"
            ),
        );

    let manifest = parse(&source).unwrap();
    let encoded = serde_json::to_value(&manifest).unwrap();

    assert_eq!(encoded["tools"][0]["grants"][0], "fs.read:/workspace");
    assert_eq!(
        encoded["tools"][0]["grants"][1],
        serde_json::json!({
            "capability": "fs.write:/workspace",
            "expires_at": expires_at,
        })
    );
    assert_eq!(encoded["tools"][1]["grants"][0]["expires_at"], expires_at);
    assert_eq!(encoded["tools"][2]["grants"][0]["expires_at"], expires_at);
    assert_eq!(encoded["couplings"][0]["grants"][0], "stream.read:thread");
    assert_eq!(
        encoded["couplings"][0]["grants"][1]["expires_at"],
        expires_at
    );

    let decoded: crate::manifest_schema::AgentManifestSchema =
        serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);

    #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    struct GrantEnvelope {
        grants: Vec<crate::manifest_schema::AgentManifestGrant>,
    }

    let grant_wire = GrantEnvelope {
        grants: vec![crate::manifest_schema::AgentManifestGrant::Expiring(
            crate::manifest_schema::AgentManifestGrantExpiry {
                capability: "fs.write:/workspace".to_string(),
                expires_at: expires_at.to_string(),
            },
        )],
    };
    let first_encoding = toml::to_string(&grant_wire).unwrap();
    let decoded: GrantEnvelope = toml::from_str(&first_encoding).unwrap();
    assert_eq!(decoded, grant_wire);
    assert_eq!(toml::to_string(&decoded).unwrap(), first_encoding);
}

#[test]
fn expiring_grants_reject_unknown_fields_invalid_instants_and_non_utc_offsets() {
    for (grant, expected) in [
        (
            r#"{ capability = "fs.read:/workspace", expires_at = "2026-07-16T20:00:00Z", expires_in = "1h" }"#,
            &["tool \"tailcat\"", "unknown field", "expires_in"][..],
        ),
        (
            r#"{ capability = "fs.read:/workspace", expires_at = "tomorrow" }"#,
            &["tool \"tailcat\"", "RFC3339"][..],
        ),
        (
            r#"{ capability = "fs.read:/workspace", expires_at = "2026-07-16T13:00:00-07:00" }"#,
            &["tool \"tailcat\"", "UTC"][..],
        ),
        (
            r#"{ capability = "fs.read:/workspace", expires_at = 123 }"#,
            &["tool \"tailcat\"", "expires_at", "string"][..],
        ),
        (
            r#"{ capability = "fs.read:/workspace", expires_at = 2026-07-16T20:00:00Z }"#,
            &[
                "tool \"tailcat\"",
                "expires_at",
                "quoted RFC3339 UTC string",
            ][..],
        ),
    ] {
        let source = valid_manifest().replace(
            "grants = [\"fs.read:/workspace\"]",
            &format!("grants = [{grant}]"),
        );
        let err = parse(&source).unwrap_err().to_string();
        for expected in expected {
            assert!(
                err.contains(expected),
                "expected {expected:?} for {grant}, got {err}"
            );
        }
    }

    let coupling = manifest_with_coupling().replace(
        "grants = [\"stream.read:thread\", \"stream.write:control\"]",
        r#"grants = [{ capability = "stream.read:thread", expires_at = 2026-07-16T20:00:00Z }]"#,
    );
    let err = parse(&coupling).unwrap_err().to_string();
    assert!(err.contains("coupling \"bash_regex_gate\""), "{err}");
    assert!(err.contains("quoted RFC3339 UTC string"), "{err}");
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

    let err = parse(&(manifest_with_coupling() + "\n[[couplings]]\nid = \"bash_regex_gate\"\nfunction_ref = \"op://policy/other@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"\ngrants = []\n[couplings.trigger]\nkind = \"turn.completed\"\n[[couplings.source.selectors]]\nstream = \"thread\"\nkind = \"turn.completed\"\n[couplings.sink]\nstream = \"control\"\nkind = \"loop.completed\"\n"))
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
