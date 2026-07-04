use super::*;

fn temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "cooldis-daemon-config-{name}-{}",
        uuid::Uuid::now_v7()
    ))
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
    assert_eq!(loaded.config.provider.env_file, Some(root.join(".env")));
    assert_eq!(
        loaded.config.io.ingress.persistence.mode,
        IngressPersistenceMode::BestEffortDirect
    );
    assert_eq!(loaded.config.io.routes[0].id, "chat-tui");

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
  { pattern = '\[reaction:(?P<emoji>[^\]]+)\]', action = "reaction" },
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

    assert_eq!(route.egress_projection.len(), 3);
    assert_eq!(route.egress_projection[0].action, "reaction");
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
        threading: None,
        ingress: None,
        egress_projection: vec![CooldisEgressProjectionRuleConfig {
            pattern: "[bad".to_string(),
            action: "reaction".to_string(),
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
        threading: None,
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
        threading: None,
        ingress: None,
        egress_projection: Vec::new(),
        typing_simulation: None,
        egress_retry: CooldisEgressRetryConfig::default(),
        telegram: Some(CooldisTelegramRouteConfig {
            listen: Some("127.0.0.1:9000".to_string()),
            path: "telegram".to_string(),
            secret_token: None,
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
