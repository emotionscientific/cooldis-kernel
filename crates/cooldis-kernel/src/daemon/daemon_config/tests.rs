use super::*;
use crate::daemon::identity::{IdentityMode, PrincipalId};

fn temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "cooldis-daemon-config-{name}-{}",
        uuid::Uuid::now_v7()
    ))
}

fn merge_daemon_identity_layers(layers: &[&str]) -> CooldisResult<CooldisDaemonConfig> {
    let mut config = CooldisDaemonConfig::default();
    for text in layers {
        let presence = daemon_config_presence(text)?;
        let layer = decode_daemon_config(text)?;
        merge_daemon_config_layer(&mut config, layer, presence);
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
            daemon_config_presence(text).unwrap().identity,
            "identity section should be present in {text:?}"
        );
    }

    assert!(
        !daemon_config_presence("[daemon.runtime]\ncwd = \"work\"\n")
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
        synthesized_local_daemon_identity_config()
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
    .unwrap();
    assert_eq!(
        partial_managed_overlay.identity.tenant_id.as_deref(),
        Some("tenant-overlay")
    );
    assert_eq!(partial_managed_overlay.identity.console_principal, None);
}

#[test]
fn loads_toml_daemon_identity_config() {
    let root = temp_root("identity");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("cooldis.toml");
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

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    assert_eq!(loaded.config.identity.mode, IdentityMode::Managed);
    assert_eq!(
        loaded.config.identity.tenant_id.as_deref(),
        Some("tenant-managed")
    );
    assert_eq!(
        loaded.config.identity.console_principal,
        Some(PrincipalId::new("operator-managed"))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn managed_daemon_identity_without_tenant_hard_fails() {
    let root = temp_root("identity-managed-missing-tenant");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        r#"
[daemon.identity]
mode = "managed"
console_principal = "operator-managed"
"#,
    )
    .unwrap();

    let error = load_cooldis_daemon_config(Some(&path)).unwrap_err();

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
    let path = root.join("cooldis.toml");
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

    let error = load_cooldis_daemon_config(Some(&path)).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("managed mode requires [daemon.identity] tenant_id; see docs/adr/0008-identity-plane-v0.md D5")
    );

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

    let error = load_cooldis_daemon_config_layers(&[base, overlay], root.clone()).unwrap_err();

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
    let path = root.join("cooldis.toml");
    std::fs::write(&path, "[daemon.runtime]\ncwd = \"work\"\n").unwrap();

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.identity,
        synthesized_local_daemon_identity_config()
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

    let without_identity_overlay =
        load_cooldis_daemon_config_layers(&[base.clone(), unrelated_overlay.clone()], root.clone())
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
        Some(PrincipalId::new("operator-base"))
    );

    let with_identity_overlay = load_cooldis_daemon_config_layers(
        &[base, unrelated_overlay, identity_overlay],
        root.clone(),
    )
    .unwrap();
    assert_eq!(
        with_identity_overlay.config.identity.mode,
        IdentityMode::Managed
    );
    assert_eq!(
        with_identity_overlay.config.identity.tenant_id.as_deref(),
        Some("tenant-overlay")
    );
    assert_eq!(
        with_identity_overlay.config.identity.console_principal,
        Some(PrincipalId::new("operator-overlay"))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn loads_toml_daemon_config_and_resolves_relative_paths() {
    let root = temp_root("toml");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        r#"
[daemon.runtime]
cwd = "work"
runtime_home = ".cooldis/runtime"
state_home = ".cooldis/state"

[daemon.runtime.placement]
target = "remote"
executor_ref = "executor://cluster/default"
config = { region = "us-west" }

[daemon.runtime.workspace]
host_path = "host-workspace"
mode = "rw"

[daemon.app_server]
listen = "unix:///tmp/cooldis-test.sock"

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

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    assert_eq!(loaded.path.as_deref(), Some(path.as_path()));
    assert_eq!(loaded.config.runtime.cwd, Some(root.join("work")));
    assert_eq!(
        loaded.config.runtime.runtime_home,
        Some(root.join(".cooldis/runtime"))
    );
    let placement = loaded.config.runtime.placement.as_ref().unwrap();
    assert_eq!(placement.target, crate::PlacementTarget::Remote);
    assert_eq!(
        placement.executor_ref.as_deref(),
        Some("executor://cluster/default")
    );
    assert_eq!(placement.config["region"], serde_json::json!("us-west"));
    assert_eq!(
        loaded.config.runtime.workspace,
        Some(AgentManifestWorkspaceBinding {
            host_path: root.join("host-workspace"),
            mode: AgentManifestWorkspaceMode::ReadWrite,
        })
    );
    assert_eq!(loaded.config.provider.env_file, Some(root.join(".env")));
    assert_eq!(
        loaded.config.io.ingress.persistence.mode,
        IngressPersistenceMode::BestEffortDirect
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
    let path = root.join("cooldis.toml");
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

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();
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
    let mut config = CooldisDaemonConfig::default();
    config.io.routes.push(CooldisIoRouteConfig {
        id: "telegram-main".to_string(),
        kind: "websocket.tui".to_string(),
        enabled: true,
        policy: None,
        content_policies: None,
        threading: None,
        agent_ref: None,
        coalesce_bursts: None,
        ingress: None,
        egress_projection: vec![CooldisEgressProjectionRuleConfig {
            pattern: "[bad".to_string(),
            action: "sticker".to_string(),
        }],
        typing_simulation: None,
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: None,
        metadata: BTreeMap::new(),
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
    let mut config = CooldisDaemonConfig::default();
    config.io.routes.push(CooldisIoRouteConfig {
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
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: None,
        metadata: BTreeMap::new(),
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
    let mut config = CooldisDaemonConfig::default();
    config.io.routes.push(CooldisIoRouteConfig {
        id: "coalesce-main".to_string(),
        kind: "websocket.tui".to_string(),
        enabled: true,
        policy: Some("steer_when_active".to_string()),
        content_policies: None,
        threading: None,
        agent_ref: None,
        coalesce_bursts: Some(CooldisCoalesceBurstsConfig {
            window_ms: 0,
            max_batch: 0,
        }),
        ingress: None,
        egress_projection: Vec::new(),
        typing_simulation: None,
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: None,
        metadata: BTreeMap::new(),
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
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[daemon.registries]
operations = ".cooldis/operations"
agents = "{}"
"#,
            absolute_agents.display()
        ),
    )
    .unwrap();

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.registries.operations,
        Some(root.join(".cooldis/operations"))
    );
    assert_eq!(
        loaded.config.registries.agents,
        Some(absolute_agents.clone())
    );
    loaded.config.validate().unwrap();

    let encoded = toml::to_string(&loaded.config).unwrap();
    let decoded = decode_daemon_config(&encoded).unwrap();
    assert_eq!(decoded.registries, loaded.config.registries);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn discovers_project_root_from_nearest_config_then_dot_cooldis() {
    let root = temp_root("project-discovery");
    let workspace = root.join("workspace");
    let nested = workspace.join("src/nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(workspace.join(".cooldis")).unwrap();

    let discovered = discover_cooldis_project(&nested).unwrap();
    assert_eq!(discovered.root, workspace);
    assert_eq!(discovered.config_path, None);

    let configured = root.join("configured");
    let configured_nested = configured.join("a/b");
    std::fs::create_dir_all(&configured_nested).unwrap();
    std::fs::write(configured.join("cooldis.toml"), "").unwrap();

    let discovered = discover_cooldis_project(&configured_nested).unwrap();
    assert_eq!(discovered.root, configured);
    assert_eq!(
        discovered.config_path,
        Some(discovered.root.join("cooldis.toml"))
    );

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
    let project_config = project_root.join("cooldis.toml");
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
runtime_home = ".cooldis/runtime"

[daemon.registries]
agents = ".cooldis/agents"

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

    let loaded = load_cooldis_daemon_config_layers(
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
        Some(project_root.join(".cooldis/runtime"))
    );
    assert_eq!(
        loaded.config.runtime.state_home,
        Some(user_root.join("user-state"))
    );
    assert_eq!(
        loaded.config.runtime.placement,
        Some(AgentManifestPlacementBinding::default())
    );
    assert_eq!(
        loaded.config.registries.agents,
        Some(project_root.join(".cooldis/agents"))
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

    let loaded =
        load_cooldis_daemon_config_layers(&[lower_config, higher_config], root.clone()).unwrap();

    assert_eq!(
        loaded.config.runtime.placement,
        Some(AgentManifestPlacementBinding {
            target: crate::PlacementTarget::Sandbox,
            executor_ref: None,
            config: BTreeMap::new(),
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

    let loaded =
        load_cooldis_daemon_config_layers(&[lower_config, higher_config], root.clone()).unwrap();

    assert_eq!(
        loaded.config.runtime.workspace,
        Some(AgentManifestWorkspaceBinding {
            host_path: higher_root.join("writable"),
            mode: AgentManifestWorkspaceMode::ReadWrite,
        })
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn loads_toml_daemon_operations_config() {
    let root = temp_root("operations");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        r#"
[daemon.operations]
global_operation_names = ["http_fetch", "json_query"]
load_all_active_when_unbound = true
"#,
    )
    .unwrap();

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.operations.global_operation_names,
        vec!["http_fetch", "json_query"]
    );
    assert!(loaded.config.operations.load_all_active_when_unbound);

    let encoded = toml::to_string(&loaded.config).unwrap();
    let decoded = decode_daemon_config(&encoded).unwrap();
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

    let loaded = load_cooldis_daemon_config_layers(&[base, overlay], root.clone()).unwrap();
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
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        r#"
[app_server]
listen = "unix:///tmp/cooldis-raw.sock"

[io.ingress.persistence]
mode = "durable_queue"
queue_name = "raw-ingress"
visibility_timeout_secs = 45

[io.ingress.queue]
sqlite_path = "queue.sqlite"
"#,
    )
    .unwrap();

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.app_server.listen,
        "unix:///tmp/cooldis-raw.sock"
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
    let path = default_daemon_socket_path_from_env(|key| match key {
        "XDG_RUNTIME_DIR" => Some(OsString::from("/run/user/501")),
        "HOME" => Some(OsString::from("/Users/me")),
        _ => None,
    });

    assert_eq!(path, PathBuf::from("/run/user/501/cooldis/cooldis.sock"));
    assert_ne!(unix_listen_url(path), "unix:///tmp/cooldis.sock");
}

#[test]
fn default_daemon_socket_uses_user_state_when_runtime_dir_is_absent() {
    let path = default_daemon_socket_path_from_env(|key| match key {
        "HOME" => Some(OsString::from("/Users/me")),
        _ => None,
    });

    if cfg!(target_os = "macos") {
        assert_eq!(
            path,
            PathBuf::from("/Users/me/Library/Application Support/cooldis/run/cooldis.sock")
        );
    } else {
        assert_eq!(
            path,
            PathBuf::from("/Users/me/.local/state/cooldis/run/cooldis.sock")
        );
    }
}

#[test]
fn resolves_relative_unix_socket_listen_against_config_dir() {
    let root = temp_root("relative-socket");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        r#"
[daemon.app_server]
listen = "unix://run/cooldis.sock"
"#,
    )
    .unwrap();

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.app_server.listen,
        format!("unix://{}", root.join("run/cooldis.sock").display())
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn resolves_relative_sync_unix_socket_listen_against_config_dir() {
    let root = temp_root("relative-sync-socket");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        r#"
[daemon.sync]
listen = "unix://run/sync.sock"
"#,
    )
    .unwrap();

    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    assert_eq!(
        loaded.config.sync.listen,
        Some(format!("unix://{}", root.join("run/sync.sock").display()))
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn validates_bad_queue_and_route_config() {
    let mut config = CooldisDaemonConfig::default();
    config.app_server.listen = "tcp://127.0.0.1:9999".to_string();
    config.io.ingress.queue.dsn = Some("postgres://db".to_string());
    config.io.ingress.queue.sqlite_path = Some(PathBuf::from("queue.sqlite"));
    config.io.routes.push(CooldisIoRouteConfig {
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
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: None,
        metadata: BTreeMap::new(),
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
    let spec = CooldisDaemonServiceSpec::new(
        PathBuf::from("/usr/local/bin/cooldis"),
        PathBuf::from("/Users/me/cooldis.toml"),
    )
    .with_label("com.example.cooldis")
    .with_working_directory("/Users/me/project");

    let launchd = render_cooldis_daemon_service(CooldisDaemonServiceTarget::Launchd, &spec);
    assert!(launchd.contains("<string>com.example.cooldis</string>"));
    assert!(launchd.contains("<string>daemon</string>"));
    assert!(launchd.contains("<string>--config</string>"));

    let systemd = render_cooldis_daemon_service(CooldisDaemonServiceTarget::Systemd, &spec);
    assert!(
        systemd.contains(
            "ExecStart=/usr/local/bin/cooldis daemon run --config /Users/me/cooldis.toml"
        )
    );
    assert!(systemd.contains("WorkingDirectory=/Users/me/project"));
}

#[test]
fn service_install_paths_are_user_scoped() {
    let home = PathBuf::from("/Users/me");

    let launchd = cooldis_daemon_service_install_path_for_home(
        CooldisDaemonServiceTarget::Launchd,
        "com.example.cooldis",
        &home,
    )
    .unwrap();
    assert_eq!(
        launchd,
        PathBuf::from("/Users/me/Library/LaunchAgents/com.example.cooldis.plist")
    );

    let systemd = cooldis_daemon_service_install_path_for_home(
        CooldisDaemonServiceTarget::Systemd,
        "cooldis",
        &home,
    )
    .unwrap();
    assert!(systemd.ends_with(".config/systemd/user/cooldis.service"));
}

#[test]
fn service_labels_reject_paths() {
    let err = cooldis_daemon_service_file_name(CooldisDaemonServiceTarget::Launchd, "../bad")
        .unwrap_err();
    assert!(err.to_string().contains("service label"));
}

#[test]
fn validates_telegram_route_shape() {
    let mut config = CooldisDaemonConfig::default();
    config.io.routes.push(CooldisIoRouteConfig {
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
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: Some(CooldisTelegramRouteConfig {
            listen: Some("127.0.0.1:9000".to_string()),
            path: "telegram".to_string(),
            secret_token: Some("secret".to_string()),
            secret_token_env: None,
            bot_token: None,
            bot_token_env: None,
            api_base: None,
        }),
        metadata: BTreeMap::new(),
    });

    let errors = config.validation_errors();
    assert!(errors.iter().any(|error| error.contains("path")));
}

#[test]
fn enabled_telegram_route_requires_webhook_secret() {
    let mut config = CooldisDaemonConfig::default();
    config.io.routes.push(CooldisIoRouteConfig {
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
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: Some(CooldisTelegramRouteConfig {
            listen: Some("127.0.0.1:9000".to_string()),
            path: "/telegram".to_string(),
            secret_token: None,
            secret_token_env: None,
            bot_token: None,
            bot_token_env: None,
            api_base: None,
        }),
        metadata: BTreeMap::new(),
    });

    let err = config.validate().unwrap_err().to_string();
    assert!(err.contains("io.routes.telegram-main.telegram"));
    assert!(err.contains("secret_token or secret_token_env is required"));

    config.io.routes[0].enabled = false;
    assert!(config.validate().is_ok());
}

#[test]
fn invalid_content_policy_names_field() {
    let mut config = CooldisDaemonConfig::default();
    config.io.routes.push(CooldisIoRouteConfig {
        id: "telegram-main".to_string(),
        kind: "telegram.bot".to_string(),
        enabled: false,
        policy: None,
        content_policies: Some(BTreeMap::from([(
            "external.event".to_string(),
            "wake_everything".to_string(),
        )])),
        agent_ref: None,
        threading: None,
        coalesce_bursts: None,
        ingress: None,
        egress_projection: Vec::new(),
        typing_simulation: None,
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: None,
        metadata: BTreeMap::new(),
    });

    let errors = config.validation_errors();

    assert!(errors.iter().any(|error| {
        error.contains("io.routes.telegram-main.content_policies.external.event")
            && error.contains("wake_everything")
    }));
}

#[test]
fn valid_content_policies_are_route_kind_lenient() {
    let mut config = CooldisDaemonConfig::default();
    config.io.routes.push(CooldisIoRouteConfig {
        id: "tui-main".to_string(),
        kind: "websocket.tui".to_string(),
        enabled: true,
        policy: Some("queue_per_conversation".to_string()),
        content_policies: Some(BTreeMap::from([(
            "external.event".to_string(),
            "observe_only".to_string(),
        )])),
        agent_ref: None,
        threading: None,
        coalesce_bursts: None,
        ingress: None,
        egress_projection: Vec::new(),
        typing_simulation: None,
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: None,
        metadata: BTreeMap::new(),
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
    let mut config = CooldisDaemonConfig::default();
    config.io.routes.push(CooldisIoRouteConfig {
        id: "event-main".to_string(),
        kind: "websocket.tui".to_string(),
        enabled: true,
        policy: None,
        content_policies: Some(BTreeMap::from([(
            "external.event".to_string(),
            "coalesce_bursts".to_string(),
        )])),
        agent_ref: None,
        threading: None,
        coalesce_bursts: None,
        ingress: None,
        egress_projection: Vec::new(),
        typing_simulation: None,
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: None,
        metadata: BTreeMap::new(),
    });

    let errors = config.validation_errors();

    assert!(errors.iter().any(|error| {
        error.contains("io.routes.event-main.content_policies.external.event")
            && error.contains("requires coalesce_bursts config")
    }));
}

#[test]
fn validates_single_clock_tick_route() {
    let mut config = CooldisDaemonConfig::default();
    for id in ["clock-main", "clock-backup"] {
        config.io.routes.push(CooldisIoRouteConfig {
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
            egress_retry: CooldisEgressRetryConfig::default(),
            telegram: None,
            metadata: BTreeMap::new(),
        });
    }

    let errors = config.validation_errors();
    assert!(
        errors
            .iter()
            .any(|error| error.contains("at most one clock.tick route"))
    );
}
