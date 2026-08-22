fn temp_root(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "verlet-daemon-config-{name}-{}",
        uuid::Uuid::now_v7()
    ))
}

fn merge_daemon_identity_layers(
    layers: &[&str],
) -> crate::kernel::runtime_host::VerletResult<crate::daemon::daemon_config::VerletDaemonConfig> {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    for text in layers {
        let presence = crate::daemon::daemon_config::daemon_config_presence(text)?;
        let layer = crate::daemon::daemon_config::decode_daemon_config(text)?;
        crate::daemon::daemon_config::merge_daemon_config_layer(&mut config, layer, presence);
    }
    config.validate()?;
    Ok(config)
}

#[test]
fn daemon_identity_presence_tracks_supported_nesting_forms() {
    for text in [
        "[daemon.identity]\n",
        "[daemon]\nidentity = {}\n",
        "[identity]\n",
        "identity = {}\n",
    ] {
        assert!(
            crate::daemon::daemon_config::daemon_config_presence(text)
                .unwrap()
                .identity,
            "identity section should be present in {text:?}"
        );
    }

    assert!(
        !crate::daemon::daemon_config::daemon_config_presence("[daemon.runtime]\ncwd = \"work\"\n")
            .unwrap()
            .identity
    );
}

#[test]
fn daemon_identity_layers_are_section_atomic_across_mode_flips() {
    let local_to_managed = merge_daemon_identity_layers(&[
        r#"
[daemon.identity]
mode = "local"
tenant_id = "tenant-local"
console_principal = "operator-local"
"#,
        r#"
[daemon.identity]
mode = "managed"
"#,
    ])
    .unwrap_err();
    assert!(local_to_managed.to_string().contains(
        "managed mode requires [daemon.identity] tenant_id; see docs/adr/0008-identity-plane-v0.md D5"
    ));

    let managed_to_empty_local = merge_daemon_identity_layers(&[
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "tenant-managed"
console_principal = "operator-managed"
"#,
        "[daemon]\nidentity = {}\n",
    ])
    .unwrap();
    assert_eq!(
        managed_to_empty_local.identity,
        crate::daemon::daemon_config::synthesized_local_daemon_identity_config()
    );

    let partial_managed_overlay = merge_daemon_identity_layers(&[
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "tenant-base"
console_principal = "operator-base"
"#,
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "tenant-overlay"
"#,
    ])
    .unwrap_err();
    assert!(partial_managed_overlay.to_string().contains(
        "managed mode requires [daemon.identity] console_principal; see docs/adr/0008-identity-plane-v0.md D5"
    ));
}

#[test]
fn loads_toml_daemon_identity_config() {
    let root = temp_root("identity");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "tenant-managed"
console_principal = "operator-managed"
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.identity.mode,
        crate::daemon::identity::IdentityMode::Managed
    );
    assert_eq!(
        loaded.config.identity.tenant_id.as_deref(),
        Some("tenant-managed")
    );
    assert_eq!(
        loaded.config.identity.console_principal,
        Some(crate::daemon::identity::PrincipalId::new(
            "operator-managed"
        ))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn managed_daemon_identity_without_tenant_hard_fails() {
    let root = temp_root("identity-managed-missing-tenant");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[daemon.identity]
mode = "managed"
console_principal = "operator-managed"
"#,
    )
    .unwrap();

    let error = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("managed mode requires [daemon.identity] tenant_id; see docs/adr/0008-identity-plane-v0.md D5")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn managed_daemon_identity_with_empty_tenant_hard_fails() {
    let root = temp_root("identity-managed-empty-tenant");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[daemon.identity]
mode = "managed"
tenant_id = ""
console_principal = "operator-managed"
"#,
    )
    .unwrap();

    let error = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("managed mode requires [daemon.identity] tenant_id; see docs/adr/0008-identity-plane-v0.md D5")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn managed_daemon_identity_with_blank_tenant_or_missing_console_principal_hard_fails() {
    let root = temp_root("identity-managed-blank-fields");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");

    std::fs::write(
        &path,
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "   "
console_principal = "operator-managed"
"#,
    )
    .unwrap();
    let error = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap_err();
    assert!(error.to_string().contains(
        "managed mode requires [daemon.identity] tenant_id; see docs/adr/0008-identity-plane-v0.md D5"
    ));

    std::fs::write(
        &path,
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "tenant-managed"
"#,
    )
    .unwrap();
    let error = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap_err();
    assert!(error.to_string().contains(
        "managed mode requires [daemon.identity] console_principal; see docs/adr/0008-identity-plane-v0.md D5"
    ));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn managed_identity_overlay_without_tenant_cannot_inherit_a_lower_layer_tenant() {
    let root = temp_root("identity-managed-overlay-missing-tenant");
    std::fs::create_dir_all(&root).unwrap();
    let base = root.join("base.toml");
    let overlay = root.join("overlay.toml");
    std::fs::write(
        &base,
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "tenant-base"
console_principal = "operator-base"
"#,
    )
    .unwrap();
    std::fs::write(
        &overlay,
        r#"
[daemon.identity]
mode = "managed"
console_principal = "operator-overlay"
"#,
    )
    .unwrap();

    let error = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[base, overlay],
        root.clone(),
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("managed mode requires [daemon.identity] tenant_id; see docs/adr/0008-identity-plane-v0.md D5")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn local_daemon_identity_without_section_synthesizes_current_defaults() {
    let root = temp_root("identity-local-defaults");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(&path, "[daemon.runtime]\ncwd = \"work\"\n").unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.identity,
        crate::daemon::daemon_config::synthesized_local_daemon_identity_config()
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn layered_daemon_identity_is_presence_tracked_and_merged_as_a_section() {
    let root = temp_root("identity-layered");
    std::fs::create_dir_all(&root).unwrap();
    let base = root.join("base.toml");
    let unrelated_overlay = root.join("unrelated-overlay.toml");
    let identity_overlay = root.join("identity-overlay.toml");
    std::fs::write(
        &base,
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "tenant-base"
console_principal = "operator-base"
"#,
    )
    .unwrap();
    std::fs::write(
        &unrelated_overlay,
        r#"
[daemon.app_server]
listen = "ws://127.0.0.1:0/rpc"
"#,
    )
    .unwrap();
    std::fs::write(
        &identity_overlay,
        r#"
[daemon.identity]
mode = "managed"
tenant_id = "tenant-overlay"
console_principal = "operator-overlay"
"#,
    )
    .unwrap();

    let without_identity_overlay = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[base.clone(), unrelated_overlay.clone()],
        root.clone(),
    )
    .unwrap();
    assert_eq!(
        without_identity_overlay
            .config
            .identity
            .tenant_id
            .as_deref(),
        Some("tenant-base")
    );
    assert_eq!(
        without_identity_overlay.config.identity.console_principal,
        Some(crate::daemon::identity::PrincipalId::new("operator-base"))
    );

    let with_identity_overlay = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[base, unrelated_overlay, identity_overlay],
        root.clone(),
    )
    .unwrap();
    assert_eq!(
        with_identity_overlay.config.identity.mode,
        crate::daemon::identity::IdentityMode::Managed
    );
    assert_eq!(
        with_identity_overlay.config.identity.tenant_id.as_deref(),
        Some("tenant-overlay")
    );
    assert_eq!(
        with_identity_overlay.config.identity.console_principal,
        Some(crate::daemon::identity::PrincipalId::new(
            "operator-overlay"
        ))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn loads_toml_daemon_config_and_resolves_relative_paths() {
    let root = temp_root("toml");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[daemon.runtime]
cwd = "work"
runtime_home = ".verlet/runtime"
state_home = ".verlet/state"

[daemon.runtime.placement]
target = "remote"
executor_ref = "executor://cluster/default"
config = { region = "us-west" }

[daemon.runtime.workspace]
host_path = "host-workspace"
mode = "rw"

[daemon.app_server]
listen = "unix:///tmp/verlet-test.sock"

[daemon.provider]
provider = "bifrost_openai"
base_url = "https://bifrost.example.test"
api_key_env = "BIFROST_KEY"
model = "openai/gpt-5.5"
env_file = ".env"

[daemon.io.ingress.persistence]
mode = "best_effort_direct"

[[daemon.io.routes]]
id = "chat-tui"
kind = "websocket.tui"
policy = "steer_when_active"
threading = "selected_thread"
agent_ref = "agent://karl-dev@latest"
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();

    assert_eq!(loaded.path.as_deref(), Some(path.as_path()));
    assert_eq!(loaded.config.runtime.cwd, Some(root.join("work")));
    assert_eq!(
        loaded.config.runtime.runtime_home,
        Some(root.join(".verlet/runtime"))
    );
    let placement = loaded.config.runtime.placement.as_ref().unwrap();
    assert_eq!(
        placement.target,
        crate::kernel::control_decision::PlacementTarget::Remote
    );
    assert_eq!(
        placement.executor_ref.as_deref(),
        Some("executor://cluster/default")
    );
    assert_eq!(placement.config["region"], serde_json::json!("us-west"));
    assert_eq!(
        loaded.config.runtime.workspace,
        Some(crate::agent::manifest_bind::AgentManifestWorkspaceBinding {
            host_path: root.join("host-workspace"),
            mode: verlet_agent::manifest_schema::AgentManifestWorkspaceMode::ReadWrite,
        })
    );
    assert_eq!(loaded.config.provider.env_file, Some(root.join(".env")));
    assert_eq!(
        loaded.config.io.ingress.persistence.mode,
        verlet_io_core::IngressPersistenceMode::BestEffortDirect
    );
    assert_eq!(loaded.config.io.routes[0].id, "chat-tui");
    assert_eq!(
        loaded.config.io.routes[0].agent_ref.as_deref(),
        Some("agent://karl-dev@latest")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn loads_route_egress_projection_and_typing_simulation() {
    let root = temp_root("egress-projection");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[[daemon.io.routes]]
id = "telegram-main"
kind = "websocket.tui"
egress_projection = [
  { pattern = '\[sticker:(?P<file_id>[^\]]+)\]', action = "sticker" },
  { pattern = '\[no_response\]', action = "silence" },
]
typing_simulation = { chars_per_second = 25 }
egress_retry = { max_attempts = 7, base_backoff_ms = 250 }
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();
    let route = &loaded.config.io.routes[0];

    assert_eq!(route.egress_projection.len(), 2);
    assert_eq!(route.egress_projection[0].action, "sticker");
    assert_eq!(
        route
            .typing_simulation
            .as_ref()
            .map(|config| config.chars_per_second),
        Some(25)
    );
    assert_eq!(route.egress_retry.max_attempts, 7);
    assert_eq!(route.egress_retry.base_backoff_ms, 250);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn invalid_egress_projection_regex_reports_rule_index() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "telegram-main".to_string(),
            kind: "websocket.tui".to_string(),
            enabled: true,
            policy: None,
            content_policies: None,
            threading: None,
            agent_ref: None,
            coalesce_bursts: None,
            ingress: None,
            egress_projection: vec![
                crate::daemon::daemon_config::VerletEgressProjectionRuleConfig {
                    pattern: "[bad".to_string(),
                    action: "sticker".to_string(),
                },
            ],
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let errors = config.validation_errors();

    assert!(
        errors.iter().any(|error| {
            error.contains("io.routes.telegram-main.egress_projection[0].pattern")
        })
    );
}

#[test]
fn validates_route_agent_ref_syntax() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "telegram-main".to_string(),
            kind: "websocket.tui".to_string(),
            enabled: true,
            policy: None,
            content_policies: None,
            threading: None,
            agent_ref: Some("karl-dev".to_string()),
            coalesce_bursts: None,
            ingress: None,
            egress_projection: Vec::new(),
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let errors = config.validation_errors();

    assert!(
        errors
            .iter()
            .any(|error| error.contains("io.routes.telegram-main.agent_ref"))
    );
}

#[test]
fn validates_coalesce_bursts_route_config() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "coalesce-main".to_string(),
            kind: "websocket.tui".to_string(),
            enabled: true,
            policy: Some("steer_when_active".to_string()),
            content_policies: None,
            threading: None,
            agent_ref: None,
            coalesce_bursts: Some(crate::daemon::daemon_config::VerletCoalesceBurstsConfig {
                window_ms: 0,
                max_batch: 0,
            }),
            ingress: None,
            egress_projection: Vec::new(),
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let errors = config.validation_errors();

    assert!(errors.iter().any(|error| error.contains("window_ms")));
    assert!(errors.iter().any(|error| error.contains("max_batch")));
}

#[test]
fn loads_toml_daemon_config_and_resolves_registry_paths() {
    let root = temp_root("registries");
    let absolute_agents = root.join("absolute-agents");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[daemon.registries]
operations = ".verlet/operations"
agents = "{}"
"#,
            absolute_agents.display()
        ),
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.registries.operations,
        Some(root.join(".verlet/operations"))
    );
    assert_eq!(
        loaded.config.registries.agents,
        Some(absolute_agents.clone())
    );
    loaded.config.validate().unwrap();

    let encoded = toml::to_string(&loaded.config).unwrap();
    let decoded = crate::daemon::daemon_config::decode_daemon_config(&encoded).unwrap();
    assert_eq!(decoded.registries, loaded.config.registries);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn discovers_project_root_from_nearest_config_then_dot_verlet() {
    let root = temp_root("project-discovery");
    let workspace = root.join("workspace");
    let nested = workspace.join("src/nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(workspace.join(".verlet")).unwrap();

    let discovered = crate::daemon::daemon_config::discover_verlet_project(&nested).unwrap();
    assert_eq!(discovered.root, workspace);
    assert_eq!(discovered.config_path, None);

    let configured = root.join("configured");
    let configured_nested = configured.join("a/b");
    std::fs::create_dir_all(&configured_nested).unwrap();
    std::fs::write(configured.join("verlet.toml"), "").unwrap();

    let discovered =
        crate::daemon::daemon_config::discover_verlet_project(&configured_nested).unwrap();
    assert_eq!(discovered.root, configured);
    assert_eq!(
        discovered.config_path,
        Some(discovered.root.join("verlet.toml"))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn project_discovery_ignores_legacy_config() {
    let root = temp_root("project-config-compat");
    let nested = root.join("src/nested");
    let canonical = root.join("verlet.toml");
    let legacy = root.join(concat!("cool", "dis.toml"));
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(&legacy, "").unwrap();

    let legacy_only = crate::daemon::daemon_config::discover_verlet_project(&nested).unwrap();
    assert_eq!(legacy_only.root, nested);
    assert_eq!(legacy_only.config_path, None);

    std::fs::write(&canonical, "").unwrap();
    let both = crate::daemon::daemon_config::discover_verlet_project(&nested).unwrap();
    assert_eq!(both.config_path.as_deref(), Some(canonical.as_path()));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn project_discovery_ignores_nearer_legacy_config() {
    let root = temp_root("project-config-nearest-legacy");
    let project = root.join("work/karl");
    let nested = project.join("src/nested");
    let ancestor_config = root.join("verlet.toml");
    let project_config = project.join(concat!("cool", "dis.toml"));
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(&ancestor_config, "").unwrap();
    std::fs::write(&project_config, "").unwrap();

    let discovered = crate::daemon::daemon_config::discover_verlet_project(&nested).unwrap();

    assert_eq!(discovered.root, root);
    assert_eq!(
        discovered.config_path.as_deref(),
        Some(ancestor_config.as_path())
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn project_discovery_ignores_nearer_legacy_state_dir() {
    let root = temp_root("project-state-nearest-legacy");
    let project = root.join("work/karl");
    let nested = project.join("src/nested");
    let project_state = project.join(concat!(".", "cool", "dis"));
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(root.join(".verlet")).unwrap();
    std::fs::create_dir_all(&project_state).unwrap();

    let discovered = crate::daemon::daemon_config::discover_verlet_project(&nested).unwrap();

    assert_eq!(discovered.root, root);
    assert_eq!(discovered.config_path, None);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn layered_toml_config_preserves_earlier_values_until_overridden() {
    let root = temp_root("layered");
    let user_root = root.join("user");
    let project_root = root.join("project");
    let explicit_root = root.join("explicit");
    std::fs::create_dir_all(&user_root).unwrap();
    std::fs::create_dir_all(&project_root).unwrap();
    std::fs::create_dir_all(&explicit_root).unwrap();
    let user_config = user_root.join("config.toml");
    let project_config = project_root.join("verlet.toml");
    let explicit_config = explicit_root.join("override.toml");
    std::fs::write(
        &user_config,
        r#"
[daemon.runtime]
state_home = "user-state"

[daemon.runtime.placement]
target = "sandbox"
executor_ref = "executor://user-sandbox"

[daemon.provider]
provider = "openai_compatible"
model = "user-model"
"#,
    )
    .unwrap();
    std::fs::write(
        &project_config,
        r#"
[daemon.runtime]
runtime_home = ".verlet/runtime"

[daemon.registries]
agents = ".verlet/agents"

[daemon.provider]
model = "project-model"
"#,
    )
    .unwrap();
    std::fs::write(
        &explicit_config,
        r#"
[daemon.runtime]
cwd = "explicit-work"

[daemon.runtime.placement]
target = "local"

[daemon.provider]
stream = true
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[
            user_config.clone(),
            project_config.clone(),
            explicit_config.clone(),
        ],
        root.clone(),
    )
    .unwrap();

    assert_eq!(
        loaded.config.runtime.cwd,
        Some(explicit_root.join("explicit-work"))
    );
    assert_eq!(
        loaded.config.runtime.runtime_home,
        Some(project_root.join(".verlet/runtime"))
    );
    assert_eq!(
        loaded.config.runtime.state_home,
        Some(user_root.join("user-state"))
    );
    assert_eq!(
        loaded.config.runtime.placement,
        Some(crate::agent::manifest_bind::AgentManifestPlacementBinding::default())
    );
    assert_eq!(
        loaded.config.registries.agents,
        Some(project_root.join(".verlet/agents"))
    );
    assert_eq!(
        loaded.config.provider.provider.as_deref(),
        Some("openai_compatible")
    );
    assert_eq!(
        loaded.config.provider.model.as_deref(),
        Some("project-model")
    );
    assert_eq!(loaded.config.provider.stream, Some(true));
    assert_eq!(loaded.path.as_deref(), Some(explicit_config.as_path()));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn layered_runtime_lease_epoch_is_presence_tracked_and_last_value_wins() {
    let root = temp_root("layered-lease-epoch");
    std::fs::create_dir_all(&root).unwrap();
    let lower = root.join("lower.toml");
    let omitted = root.join("omitted.toml");
    let higher = root.join("higher.toml");
    std::fs::write(&lower, "[daemon.runtime]\nlease_epoch = 7\n").unwrap();
    std::fs::write(&omitted, "[daemon.runtime]\ncwd = \"work\"\n").unwrap();
    std::fs::write(&higher, "[daemon.runtime]\nlease_epoch = 9\n").unwrap();

    let defaulted = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        std::slice::from_ref(&omitted),
        root.clone(),
    )
    .unwrap();
    assert_eq!(defaulted.config.runtime.lease_epoch, 0);

    let preserved = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[lower.clone(), omitted.clone()],
        root.clone(),
    )
    .unwrap();
    assert_eq!(preserved.config.runtime.lease_epoch, 7);

    let overridden = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[lower, omitted, higher],
        root.clone(),
    )
    .unwrap();
    assert_eq!(overridden.config.runtime.lease_epoch, 9);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn layered_placement_target_only_survives_higher_layer_omission() {
    let root = temp_root("layered-placement-omission");
    let lower_root = root.join("lower");
    let higher_root = root.join("higher");
    std::fs::create_dir_all(&lower_root).unwrap();
    std::fs::create_dir_all(&higher_root).unwrap();
    let lower_config = lower_root.join("config.toml");
    let higher_config = higher_root.join("config.toml");
    std::fs::write(
        &lower_config,
        r#"
[daemon.runtime.placement]
target = "sandbox"
"#,
    )
    .unwrap();
    std::fs::write(
        &higher_config,
        r#"
[daemon.runtime]
cwd = "workspace"
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[lower_config, higher_config],
        root.clone(),
    )
    .unwrap();

    assert_eq!(
        loaded.config.runtime.placement,
        Some(crate::agent::manifest_bind::AgentManifestPlacementBinding {
            target: crate::kernel::control_decision::PlacementTarget::Sandbox,
            executor_ref: None,
            config: std::collections::BTreeMap::new(),
        })
    );
    assert_eq!(
        loaded.config.runtime.cwd,
        Some(higher_root.join("workspace"))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn layered_workspace_binding_is_replaced_atomically_by_the_higher_layer() {
    let root = temp_root("layered-workspace");
    let lower_root = root.join("lower");
    let higher_root = root.join("higher");
    std::fs::create_dir_all(&lower_root).unwrap();
    std::fs::create_dir_all(&higher_root).unwrap();
    let lower_config = lower_root.join("config.toml");
    let higher_config = higher_root.join("config.toml");
    std::fs::write(
        &lower_config,
        r#"
[daemon.runtime.workspace]
host_path = "readonly"
mode = "ro"
"#,
    )
    .unwrap();
    std::fs::write(
        &higher_config,
        r#"
[daemon.runtime.workspace]
host_path = "writable"
mode = "rw"
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[lower_config, higher_config],
        root.clone(),
    )
    .unwrap();

    assert_eq!(
        loaded.config.runtime.workspace,
        Some(crate::agent::manifest_bind::AgentManifestWorkspaceBinding {
            host_path: higher_root.join("writable"),
            mode: verlet_agent::manifest_schema::AgentManifestWorkspaceMode::ReadWrite,
        })
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn loads_toml_daemon_operations_config() {
    let root = temp_root("operations");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[daemon.operations]
global_operation_names = ["http_fetch", "json_query"]
load_all_active_when_unbound = true
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.operations.global_operation_names,
        vec!["http_fetch", "json_query"]
    );
    assert!(loaded.config.operations.load_all_active_when_unbound);

    let encoded = toml::to_string(&loaded.config).unwrap();
    let decoded = crate::daemon::daemon_config::decode_daemon_config(&encoded).unwrap();
    assert_eq!(decoded.operations, loaded.config.operations);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn layered_toml_daemon_sync_config_merges_by_field_presence() {
    let root = temp_root("sync-layered");
    std::fs::create_dir_all(&root).unwrap();
    let base = root.join("base.toml");
    let overlay = root.join("overlay.toml");
    std::fs::write(
        &base,
        r#"
[daemon.sync]
listen = "ws://127.0.0.1:0"
lease_ttl_secs = 45
"#,
    )
    .unwrap();
    std::fs::write(
        &overlay,
        r#"
[daemon.sync]
lease_ttl_secs = 90
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &[base, overlay],
        root.clone(),
    )
    .unwrap();
    assert_eq!(
        loaded.config.sync.listen.as_deref(),
        Some("ws://127.0.0.1:0")
    );
    assert_eq!(loaded.config.sync.lease_ttl_secs, 90);
    loaded.config.validate().unwrap();

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn toml_config_accepts_raw_daemon_shape() {
    let root = temp_root("raw");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[app_server]
listen = "unix:///tmp/verlet-raw.sock"

[io.ingress.persistence]
mode = "durable_queue"
queue_name = "raw-ingress"
visibility_timeout_secs = 45

[io.ingress.queue]
sqlite_path = "queue.sqlite"
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.app_server.listen,
        "unix:///tmp/verlet-raw.sock"
    );
    assert_eq!(
        loaded.config.io.ingress.persistence.queue_name.as_deref(),
        Some("raw-ingress")
    );
    assert_eq!(
        loaded.config.io.ingress.queue.sqlite_path,
        Some(root.join("queue.sqlite"))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn default_daemon_socket_prefers_user_runtime_directory() {
    let path = crate::daemon::daemon_config::default_daemon_socket_path_from_env(|key| match key {
        "XDG_RUNTIME_DIR" => Some(std::ffi::OsString::from("/run/user/501")),
        "HOME" => Some(std::ffi::OsString::from("/Users/me")),
        _ => None,
    });

    assert_eq!(
        path,
        std::path::PathBuf::from("/run/user/501/verlet/verlet.sock")
    );
    assert_ne!(
        crate::daemon::daemon_config::unix_listen_url(path),
        "unix:///tmp/verlet.sock"
    );
}

#[test]
fn default_daemon_socket_uses_user_state_when_runtime_dir_is_absent() {
    let path = crate::daemon::daemon_config::default_daemon_socket_path_from_env(|key| match key {
        "HOME" => Some(std::ffi::OsString::from("/Users/me")),
        _ => None,
    });

    if cfg!(target_os = "macos") {
        assert_eq!(
            path,
            std::path::PathBuf::from(
                "/Users/me/Library/Application Support/verlet/run/verlet.sock"
            )
        );
    } else {
        assert_eq!(
            path,
            std::path::PathBuf::from("/Users/me/.local/state/verlet/run/verlet.sock")
        );
    }
}

#[test]
fn default_daemon_socket_ignores_existing_legacy_runtime_directory() {
    let root = temp_root("legacy-daemon-socket");
    let legacy_dir = root.join(concat!("cool", "dis"));
    std::fs::create_dir_all(&legacy_dir).unwrap();

    let path = crate::daemon::daemon_config::default_daemon_socket_path_from_env(|key| match key {
        "XDG_RUNTIME_DIR" => Some(root.as_os_str().to_os_string()),
        _ => None,
    });

    assert_eq!(path, root.join("verlet/verlet.sock"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn resolves_relative_unix_socket_listen_against_config_dir() {
    let root = temp_root("relative-socket");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[daemon.app_server]
listen = "unix://run/verlet.sock"
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.app_server.listen,
        format!("unix://{}", root.join("run/verlet.sock").display())
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn resolves_relative_sync_unix_socket_listen_against_config_dir() {
    let root = temp_root("relative-sync-socket");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("verlet.toml");
    std::fs::write(
        &path,
        r#"
[daemon.sync]
listen = "unix://run/sync.sock"
"#,
    )
    .unwrap();

    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.sync.listen,
        Some(format!("unix://{}", root.join("run/sync.sock").display()))
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn validates_bad_queue_and_route_config() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config.app_server.listen = "tcp://127.0.0.1:9999".to_string();
    config.io.ingress.queue.dsn = Some("postgres://db".to_string());
    config.io.ingress.queue.sqlite_path = Some(std::path::PathBuf::from("queue.sqlite"));
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "".to_string(),
            kind: "".to_string(),
            enabled: true,
            policy: None,
            content_policies: None,
            threading: None,
            agent_ref: None,
            coalesce_bursts: None,
            ingress: None,
            egress_projection: Vec::new(),
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let errors = config.validation_errors();

    assert!(
        errors
            .iter()
            .any(|error| error.contains("app_server.listen"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("cannot set both dsn and sqlite_path"))
    );
    assert!(
        errors
            .iter()
            .any(|error| error.contains("id cannot be empty"))
    );
}

#[test]
fn renders_launchd_and_systemd_services() {
    let spec = crate::daemon::daemon_config::VerletDaemonServiceSpec::new(
        std::path::PathBuf::from("/usr/local/bin/verlet"),
        std::path::PathBuf::from("/Users/me/verlet.toml"),
    )
    .with_label("com.example.verlet")
    .with_working_directory("/Users/me/project");

    let launchd = crate::daemon::daemon_config::render_verlet_daemon_service(
        crate::daemon::daemon_config::VerletDaemonServiceTarget::Launchd,
        &spec,
    );
    assert!(launchd.contains("<string>com.example.verlet</string>"));
    assert!(launchd.contains("<string>serve</string>"));
    assert!(launchd.contains("<string>--config</string>"));

    let systemd = crate::daemon::daemon_config::render_verlet_daemon_service(
        crate::daemon::daemon_config::VerletDaemonServiceTarget::Systemd,
        &spec,
    );
    assert!(
        systemd.contains("ExecStart=/usr/local/bin/verlet serve --config /Users/me/verlet.toml")
    );
    assert!(systemd.contains("WorkingDirectory=/Users/me/project"));
}

#[test]
fn daemon_idle_timeout_uses_human_duration_syntax() {
    let config =
        crate::daemon::daemon_config::decode_daemon_config("[daemon]\nidle_timeout = \"2s\"\n")
            .unwrap();

    assert_eq!(
        config.idle_timeout().unwrap(),
        Some(std::time::Duration::from_secs(2))
    );

    let invalid =
        crate::daemon::daemon_config::decode_daemon_config("[daemon]\nidle_timeout = \"later\"\n")
            .unwrap();
    assert!(
        invalid
            .validation_errors()
            .iter()
            .any(|error| error.starts_with("idle_timeout:"))
    );
}

#[test]
fn service_install_paths_are_user_scoped() {
    let home = std::path::PathBuf::from("/Users/me");

    let launchd = crate::daemon::daemon_config::verlet_daemon_service_install_path_for_home(
        crate::daemon::daemon_config::VerletDaemonServiceTarget::Launchd,
        "com.example.verlet",
        &home,
    )
    .unwrap();
    assert_eq!(
        launchd,
        std::path::PathBuf::from("/Users/me/Library/LaunchAgents/com.example.verlet.plist")
    );

    let systemd = crate::daemon::daemon_config::verlet_daemon_service_install_path_for_home(
        crate::daemon::daemon_config::VerletDaemonServiceTarget::Systemd,
        "verlet",
        &home,
    )
    .unwrap();
    assert!(systemd.ends_with(".config/systemd/user/verlet.service"));
}

#[test]
fn service_labels_reject_paths() {
    let err = crate::daemon::daemon_config::verlet_daemon_service_file_name(
        crate::daemon::daemon_config::VerletDaemonServiceTarget::Launchd,
        "../bad",
    )
    .unwrap_err();
    assert!(err.to_string().contains("service label"));
}

#[test]
fn validates_telegram_route_shape() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "telegram-main".to_string(),
            kind: "telegram.bot".to_string(),
            enabled: true,
            policy: None,
            content_policies: None,
            threading: None,
            agent_ref: None,
            coalesce_bursts: None,
            ingress: None,
            egress_projection: Vec::new(),
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: Some(crate::daemon::daemon_config::VerletTelegramRouteConfig {
                listen: Some("127.0.0.1:9000".to_string()),
                path: "telegram".to_string(),
                secret_token: Some("secret".to_string()),
                secret_token_env: None,
                bot_token: None,
                bot_token_env: None,
                api_base: None,
            }),
            metadata: std::collections::BTreeMap::new(),
        });

    let errors = config.validation_errors();
    assert!(errors.iter().any(|error| error.contains("path")));
}

#[test]
fn enabled_telegram_route_requires_webhook_secret() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "telegram-main".to_string(),
            kind: "telegram.bot".to_string(),
            enabled: true,
            policy: None,
            content_policies: None,
            threading: None,
            agent_ref: None,
            coalesce_bursts: None,
            ingress: None,
            egress_projection: Vec::new(),
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: Some(crate::daemon::daemon_config::VerletTelegramRouteConfig {
                listen: Some("127.0.0.1:9000".to_string()),
                path: "/telegram".to_string(),
                secret_token: None,
                secret_token_env: None,
                bot_token: None,
                bot_token_env: None,
                api_base: None,
            }),
            metadata: std::collections::BTreeMap::new(),
        });

    let err = config.validate().unwrap_err().to_string();
    assert!(err.contains("io.routes.telegram-main.telegram"));
    assert!(err.contains("secret_token or secret_token_env is required"));

    config.io.routes[0].enabled = false;
    assert!(config.validate().is_ok());
}

#[test]
fn invalid_content_policy_names_field() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "telegram-main".to_string(),
            kind: "telegram.bot".to_string(),
            enabled: false,
            policy: None,
            content_policies: Some(std::collections::BTreeMap::from([(
                "external.event".to_string(),
                "wake_everything".to_string(),
            )])),
            agent_ref: None,
            threading: None,
            coalesce_bursts: None,
            ingress: None,
            egress_projection: Vec::new(),
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let errors = config.validation_errors();

    assert!(errors.iter().any(|error| {
        error.contains("io.routes.telegram-main.content_policies.external.event")
            && error.contains("wake_everything")
    }));
}

#[test]
fn valid_content_policies_are_route_kind_lenient() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "tui-main".to_string(),
            kind: "websocket.tui".to_string(),
            enabled: true,
            policy: Some("queue_per_conversation".to_string()),
            content_policies: Some(std::collections::BTreeMap::from([(
                "external.event".to_string(),
                "observe_only".to_string(),
            )])),
            agent_ref: None,
            threading: None,
            coalesce_bursts: None,
            ingress: None,
            egress_projection: Vec::new(),
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let errors = config.validation_errors();

    assert!(
        errors
            .iter()
            .all(|error| !error.contains("content_policies")),
        "valid content_policies should not be route-kind-gated: {errors:?}"
    );
}

#[test]
fn content_policy_coalesce_requires_coalesce_config() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    config
        .io
        .routes
        .push(crate::daemon::daemon_config::VerletIoRouteConfig {
            id: "event-main".to_string(),
            kind: "websocket.tui".to_string(),
            enabled: true,
            policy: None,
            content_policies: Some(std::collections::BTreeMap::from([(
                "external.event".to_string(),
                "coalesce_bursts".to_string(),
            )])),
            agent_ref: None,
            threading: None,
            coalesce_bursts: None,
            ingress: None,
            egress_projection: Vec::new(),
            typing_simulation: None,
            egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
            telegram: None,
            metadata: std::collections::BTreeMap::new(),
        });

    let errors = config.validation_errors();

    assert!(errors.iter().any(|error| {
        error.contains("io.routes.event-main.content_policies.external.event")
            && error.contains("requires coalesce_bursts config")
    }));
}

#[test]
fn validates_single_clock_tick_route() {
    let mut config = crate::daemon::daemon_config::VerletDaemonConfig::default();
    for id in ["clock-main", "clock-backup"] {
        config
            .io
            .routes
            .push(crate::daemon::daemon_config::VerletIoRouteConfig {
                id: id.to_string(),
                kind: "clock.tick".to_string(),
                enabled: true,
                policy: None,
                content_policies: None,
                threading: None,
                agent_ref: None,
                coalesce_bursts: None,
                ingress: None,
                egress_projection: Vec::new(),
                typing_simulation: None,
                egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
                telegram: None,
                metadata: std::collections::BTreeMap::new(),
            });
    }

    let errors = config.validation_errors();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("at most one clock.tick route"))
    );
}
