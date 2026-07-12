use super::*;
use crate::CooldisDaemonConfig;

#[test]
fn turso_cross_process_lock_match_accepts_only_refused_open_shapes() {
    assert!(turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: Locking error: Failed locking file '/tmp/session_history.sqlite3'. File is locked by another process"
    ));
    assert!(turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: Locking error: Failed locking file. File is locked by another process"
    ));

    assert!(!turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: Locking error: Failed to release file lock: permission denied"
    ));
    assert!(!turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: I/O error: Failed locking file '/tmp/session_history.sqlite3'. File is locked by another process"
    ));
    assert!(!turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: Internal error: sqlite engine error: Locking error: Failed locking file. File is locked by another process"
    ));
    assert!(!turso_cross_process_lock_error(
        "history storage failed: database is locked"
    ));
}

#[test]
fn root_help_is_a_concise_starting_surface() {
    assert!(ROOT_HELP.contains(
        "Start here:\n  cooldis console\n  cooldis chat [PROMPT]\n  cooldis init <name>"
    ));
    assert!(ROOT_HELP.contains(
        "Explore:\n  cooldis commands\n  cooldis help <command>\n  cooldis <command> --help\n  man cooldis"
    ));
    assert!(!ROOT_HELP.contains("Example usage:"));
    assert!(!ROOT_HELP.contains("Advanced:"));
    assert!(!ROOT_HELP.contains("cooldis coupling run --replay"));
    assert!(!ROOT_HELP.contains("cooldis daemon run"));
}

#[test]
fn parse_publish_args_collects_cli_only_fields_without_runtime() {
    let args = vec![
        "--module-path",
        "tool",
        "--name",
        "tailcat",
        "--registry-root",
        ".cooldis/operations",
        "--grant",
        "net.http:POST:https://api.example.com",
        "--metadata",
        "provider=\"fixture\"",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let parsed = parse_publish_args(args).unwrap();

    assert_eq!(parsed.module_path, Some(PathBuf::from("tool")));
    assert_eq!(parsed.name.as_deref(), Some("tailcat"));
    assert!(
        parsed
            .capability_grants
            .contains("net.http:POST:https://api.example.com")
    );
    assert_eq!(
        parsed.metadata["provider"],
        Value::String("fixture".to_string())
    );
}

#[test]
fn parse_run_args_distinguishes_registry_run_from_source_run() {
    let registry_args = vec!["tailcat", "tail", "--input", "/workspace/tail.txt"]
        .into_iter()
        .map(OsString::from)
        .collect();
    let registry = parse_run_args(registry_args).unwrap();
    assert_eq!(registry.registered_name.as_deref(), Some("tailcat"));
    assert_eq!(registry.operation, "tail");

    let source_args = vec![
        "--module-path",
        "tool",
        "tail",
        "--input",
        "/workspace/tail.txt",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();
    let source = parse_run_args(source_args).unwrap();
    assert_eq!(source.registered_name, None);
    assert_eq!(source.module_path, Some(PathBuf::from("tool")));
    assert_eq!(source.operation, "tail");
}

#[test]
fn parse_tool_source_add_accepts_remote_mcp_contract_fields() {
    let args = vec![
        "arcade",
        "--kind",
        "mcp-http",
        "--url",
        "https://example.com/mcp",
        "--bearer-secret",
        "arcade.api_key",
        "--header",
        "x-provider=arcade",
        "--include-tool",
        "gmail_search",
        "--include-tool",
        "gmail_send",
        "--timeout-ms",
        "5000",
        "--max-output-bytes",
        "32768",
        "--state-home",
        "/tmp/cooldis-state",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let parsed = parse_tool_source_add_args(args).unwrap();

    assert_eq!(parsed.name.as_deref(), Some("arcade"));
    assert_eq!(parsed.kind, Some(McpRemoteTransport::StreamableHttp));
    assert_eq!(parsed.url.as_deref(), Some("https://example.com/mcp"));
    assert_eq!(parsed.bearer_secret.as_deref(), Some("arcade.api_key"));
    assert_eq!(
        parsed.include_tools,
        BTreeSet::from(["gmail_search".to_string(), "gmail_send".to_string()])
    );
    assert_eq!(parsed.timeout_ms, Some(5000));
    assert_eq!(parsed.max_output_bytes, Some(32768));
    assert_eq!(parsed.state_home, Some(PathBuf::from("/tmp/cooldis-state")));
}

#[test]
fn parse_tool_source_show_accepts_json_and_state_home() {
    let args = vec!["arcade", "--json", "--state-home", "/tmp/cooldis-state"]
        .into_iter()
        .map(OsString::from)
        .collect();

    let parsed = parse_tool_source_show_args(args).unwrap();

    assert_eq!(parsed.name.as_deref(), Some("arcade"));
    assert!(parsed.json);
    assert_eq!(parsed.state_home, Some(PathBuf::from("/tmp/cooldis-state")));
}

#[test]
fn parse_chat_args_collects_prompt_and_homes() {
    let args = vec![
        "--cwd",
        "/tmp/work",
        "--config",
        "/tmp/cooldis-chat.json",
        "--runtime-home",
        "/tmp/runtime",
        "--state-home",
        "/tmp/state",
        "--provider",
        "bifrost_openai",
        "--model",
        "openai/gpt-5.5",
        "hello",
        "agent",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();
    let parsed = parse_chat_args(args).unwrap();
    assert_eq!(parsed.cwd, PathBuf::from("/tmp/work"));
    assert_eq!(
        parsed.config_path,
        Some(PathBuf::from("/tmp/cooldis-chat.json"))
    );
    assert_eq!(parsed.runtime_home, Some(PathBuf::from("/tmp/runtime")));
    assert_eq!(parsed.state_home, Some(PathBuf::from("/tmp/state")));
    assert_eq!(parsed.provider.as_deref(), Some("bifrost_openai"));
    assert_eq!(parsed.model.as_deref(), Some("openai/gpt-5.5"));
    assert_eq!(parsed.attach, None);
    assert_eq!(parsed.prompt.as_deref(), Some("hello agent"));
}

#[test]
fn parse_chat_args_collects_attach_endpoint() {
    let args = vec!["--attach", "unix:///tmp/cooldis.sock", "hello"]
        .into_iter()
        .map(OsString::from)
        .collect();

    let parsed = parse_chat_args(args).unwrap();

    assert_eq!(parsed.attach.as_deref(), Some("unix:///tmp/cooldis.sock"));
    assert_eq!(parsed.prompt.as_deref(), Some("hello"));
}

#[test]
fn parse_console_args_defaults_to_loopback_and_open() {
    let parsed = parse_console_args(Vec::new()).unwrap();

    assert_eq!(
        parsed.listen.ip(),
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
    );
    assert_eq!(parsed.listen.port(), 0);
    assert!(parsed.open);
    assert_eq!(parsed.config_path, None);
    assert!(!parsed.cwd_explicit);
}

#[test]
fn parse_console_args_collects_browser_and_runtime_options() {
    let args = vec![
        "--no-open",
        "--cwd",
        "/tmp/work",
        "--config",
        "/tmp/cooldis.toml",
        "--port",
        "4321",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let parsed = parse_console_args(args).unwrap();

    assert_eq!(parsed.listen, "127.0.0.1:4321".parse().unwrap());
    assert!(!parsed.open);
    assert_eq!(parsed.cwd, PathBuf::from("/tmp/work"));
    assert!(parsed.cwd_explicit);
    assert_eq!(parsed.config_path, Some(PathBuf::from("/tmp/cooldis.toml")));
}

#[test]
fn console_app_server_config_from_toml_preserves_config_cwd_unless_overridden() {
    let root = std::env::temp_dir().join(format!("cooldis-console-config-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let config_path = root.join("cooldis.toml");
    std::fs::write(
        &config_path,
        r#"
[daemon.runtime]
cwd = "configured-work"

[daemon.app_server]
listen = "unix:///tmp/ignored-console-config.sock"
"#,
    )
    .unwrap();
    let listen = AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap());

    let parsed = parse_console_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    let config = console_app_server_config(&parsed, listen.clone()).unwrap();
    assert_eq!(config.listen, listen);
    assert_eq!(config.cwd, root.join("configured-work"));

    let parsed = parse_console_args(
        vec![
            "--config",
            config_path.to_str().unwrap(),
            "--cwd",
            "/tmp/override-work",
        ]
        .into_iter()
        .map(OsString::from)
        .collect(),
    )
    .unwrap();
    let config = console_app_server_config(&parsed, listen).unwrap();
    assert_eq!(config.cwd, PathBuf::from("/tmp/override-work"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn console_app_server_config_defaults_to_project_local_roots_and_user_state() {
    let root = std::env::temp_dir().join(format!("cooldis-console-project-{}", Uuid::now_v7()));
    let nested = root.join("work/nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir_all(root.join("work/.cooldis")).unwrap();
    let parsed = parse_console_args(
        vec!["--cwd", nested.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    let listen = AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap());
    let config = console_app_server_config(&parsed, listen).unwrap();

    let project = root.join("work");
    assert_eq!(config.runtime_home, project.join(".cooldis/runtime"));
    assert_eq!(config.state_home, project.join(".cooldis/state"));
    assert_eq!(config.agent_registry_root, project.join(".cooldis/agents"));
    assert_eq!(
        config.capsule_bindings.registry_root,
        Some(project.join(".cooldis/operations"))
    );
    assert_eq!(
        config.user_metadata_store_path(),
        default_user_cooldis_home()
            .unwrap()
            .join("state/metadata.sqlite3")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn console_project_storage_root_does_not_reuse_user_home() {
    let root = std::env::temp_dir().join(format!("cooldis-console-home-{}", Uuid::now_v7()));
    let project_root = root.join("home");
    let user_home = project_root.join(".cooldis");

    assert_eq!(
        console_project_storage_root(&project_root, &user_home),
        user_home.join("projects/home")
    );
    assert_eq!(
        console_project_storage_root(&root.join("work"), &user_home),
        root.join("work/.cooldis")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn prepare_console_project_storage_creates_operation_registry_root() {
    let root = std::env::temp_dir().join(format!("cooldis-console-roots-{}", Uuid::now_v7()));
    let workspace = root.join("workspace");
    let mut config = CooldisAppServerConfig::local(
        AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap()),
        &workspace,
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    config.capsule_bindings.registry_root = Some(root.join("operations"));

    prepare_console_project_storage(&config).unwrap();

    assert!(config.runtime_home.is_dir());
    assert!(config.state_home.is_dir());
    assert!(config.user_state_home.is_dir());
    assert!(config.agent_registry_root.is_dir());
    assert!(
        config
            .capsule_bindings
            .registry_root
            .as_ref()
            .is_some_and(|path| path.is_dir())
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn parse_daemon_service_print_uses_explicit_target_and_config() {
    let args = vec![
        "--target",
        "systemd",
        "--config",
        "/tmp/cooldis.toml",
        "--bin",
        "/usr/local/bin/cooldis",
        "--label",
        "com.example.cooldis",
        "--working-directory",
        "/tmp/work",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let parsed = parse_daemon_service_print_args(args).unwrap();

    assert_eq!(parsed.target, CooldisDaemonServiceTarget::Systemd);
    assert_eq!(parsed.config_path, PathBuf::from("/tmp/cooldis.toml"));
    assert_eq!(parsed.executable, PathBuf::from("/usr/local/bin/cooldis"));
    assert_eq!(parsed.label, "com.example.cooldis");
    assert_eq!(parsed.working_directory, Some(PathBuf::from("/tmp/work")));
}

#[test]
fn parse_daemon_service_uninstall_accepts_target_and_label() {
    let args = vec!["--target", "launchd", "--label", "com.example.cooldis"]
        .into_iter()
        .map(OsString::from)
        .collect();

    let parsed = parse_daemon_service_uninstall_args(args).unwrap();

    assert_eq!(parsed.target, CooldisDaemonServiceTarget::Launchd);
    assert_eq!(parsed.label, "com.example.cooldis");
}

#[test]
fn parse_daemon_run_accepts_config_only() {
    let args = vec!["--config", "/tmp/cooldis.toml"]
        .into_iter()
        .map(OsString::from)
        .collect();

    let parsed = parse_daemon_run_args(args).unwrap();

    assert_eq!(parsed.config_path, Some(PathBuf::from("/tmp/cooldis.toml")));
}

#[test]
fn daemon_app_server_config_from_loaded_keeps_registry_defaults_when_unset() {
    let root = std::env::temp_dir().join(format!("cooldis-daemon-defaults-{}", Uuid::now_v7()));
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.app_server.listen = "unix:///tmp/cooldis-daemon-defaults.sock".to_string();
    daemon_config.runtime.cwd = Some(root.join("work"));

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    // lexicon-allow: capsule - existing app-server config field
    assert_eq!(
        app_config.capsule_bindings.registry_root,
        Some(PathBuf::from(".cooldis/operations"))
    );
    assert_eq!(
        app_config.agent_registry_root,
        PathBuf::from(".cooldis/agents")
    );
}

#[test]
fn daemon_app_server_config_from_loaded_applies_registry_roots() {
    let root = std::env::temp_dir().join(format!("cooldis-daemon-registries-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        r#"
[daemon.runtime]
cwd = "work"

[daemon.app_server]
listen = "unix://run/cooldis.sock"

[daemon.registries]
operations = ".cooldis/operations"
agents = ".cooldis/agents"
"#,
    )
    .unwrap();
    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    let app_config = daemon_app_server_config_from_loaded(&loaded).unwrap();

    assert_eq!(app_config.cwd, root.join("work"));
    assert_eq!(
        // lexicon-allow: capsule - existing app-server config field
        app_config.capsule_bindings.registry_root,
        Some(root.join(".cooldis/operations"))
    );
    assert_eq!(app_config.agent_registry_root, root.join(".cooldis/agents"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn daemon_app_server_config_from_loaded_applies_operations_policy() {
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.app_server.listen = "unix:///tmp/cooldis-daemon-operations.sock".to_string();
    daemon_config.operations.global_operation_names =
        vec!["http_fetch".to_string(), "json_query".to_string()];
    daemon_config.operations.load_all_active_when_unbound = true;

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    assert_eq!(
        // lexicon-allow: capsule - existing app-server config field
        app_config.capsule_bindings.registry_root,
        Some(PathBuf::from(".cooldis/operations"))
    );
    assert_eq!(
        // lexicon-allow: capsule - existing app-server config field
        app_config.capsule_bindings.global_operation_names,
        vec!["http_fetch", "json_query"]
    );
    // lexicon-allow: capsule - existing app-server config field
    assert!(app_config.capsule_bindings.load_all_active_when_unbound);
}

#[test]
fn daemon_app_server_config_from_loaded_absolutizes_relative_registry_roots() {
    let current_dir = std::env::current_dir().unwrap();
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.app_server.listen = "unix:///tmp/cooldis-daemon-relative.sock".to_string();
    daemon_config.runtime.cwd = Some(PathBuf::from("config/work"));
    daemon_config.registries.operations = Some(PathBuf::from("config/.cooldis/operations"));
    daemon_config.registries.agents = Some(PathBuf::from("config/.cooldis/agents"));

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    assert_eq!(app_config.cwd, PathBuf::from("config/work"));
    assert_eq!(
        app_config.capsule_bindings.registry_root, // lexicon-allow: capsule - existing app-server config field
        Some(current_dir.join("config/.cooldis/operations"))
    );
    assert_eq!(
        app_config.agent_registry_root,
        current_dir.join("config/.cooldis/agents")
    );
}

#[tokio::test]
async fn daemon_default_operation_registry_binds_agent_manifest_without_registries_config() {
    let root = daemon_test_root("default-operation-bind");
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = workspace.join(".cooldis/operations");
    let agent_registry_root = workspace.join(".cooldis/agents");
    let record =
        publish_daemon_test_operation(&operation_registry_root, "http_fetch", "http_fetch").await;
    publish_daemon_test_agent(
        &root,
        &agent_registry_root,
        &operation_registry_root,
        "researcher",
        "http_fetch",
        "http_fetch",
        &format!(
            "op://http_fetch/http_fetch@sha256:{}",
            record.active_artifact_hash
        ),
    );
    let daemon_config = daemon_test_config(&root, &workspace);

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();
    let app = CooldisAppServer::new_local(app_config).await.unwrap();

    let operations = app
        .local_json_rpc_request("operation/list", json!({}))
        .await
        .unwrap();
    assert!(
        operations["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|operation| operation["name"].as_str() == Some("http_fetch"))
    );
    let thread = app
        .local_json_rpc_request(
            "thread/start",
            json!({ "agentRef": "agent://researcher@latest" }),
        )
        .await
        .unwrap();
    assert!(thread["thread"]["id"].as_str().is_some());

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn daemon_default_operation_registry_absent_rejects_agent_manifest_publish() {
    let root = daemon_test_root("default-operation-absent");
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = workspace.join(".cooldis/operations");
    let agent_registry_root = workspace.join(".cooldis/agents");
    let err = publish_daemon_test_agent_result(
            &root,
            &agent_registry_root,
            &operation_registry_root,
            "researcher",
            "http_fetch",
            "http_fetch",
            "op://http_fetch/http_fetch@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap_err();
    assert!(err.to_string().contains("none was found"));
    assert!(!operation_registry_root.exists());

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn daemon_operations_load_all_uses_default_registry_for_default_manifest() {
    let root = daemon_test_root("default-operation-load-all");
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = workspace.join(".cooldis/operations");
    for operation_name in ["http_fetch", "file_read", "json_query"] {
        publish_daemon_test_operation(&operation_registry_root, operation_name, operation_name)
            .await;
    }
    let mut daemon_config = daemon_test_config(&root, &workspace);
    daemon_config.operations.load_all_active_when_unbound = true;

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();
    let app = CooldisAppServer::new_local(app_config).await.unwrap();

    let default_agent = LocalAgentRegistry::new(workspace.join(".cooldis/agents"))
        .load_ref("agent://cooldis/default@latest")
        .unwrap();
    let tools = default_agent.resolved_manifest["tools"].as_array().unwrap();
    for command in ["http_fetch", "file_read", "json_query"] {
        let row = tools
            .iter()
            .find(|tool| tool["command"].as_str() == Some(command))
            .unwrap_or_else(|| panic!("missing default manifest bash command {command}"));
        assert_eq!(row["type"].as_str(), Some("bash_tool"));
    }
    let thread = app
        .local_json_rpc_request("thread/start", json!({}))
        .await
        .unwrap();
    assert!(thread["thread"]["id"].as_str().is_some());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn parse_debug_rpc_call_accepts_method_params_and_url() {
    let args = vec![
        "thread/read",
        r#"{"threadId":"abc","includeTurns":false}"#,
        "--url",
        "ws://127.0.0.1:49200/rpc",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let parsed = parse_debug_rpc_call_args(args).unwrap();

    assert_eq!(parsed.method, "thread/read");
    assert_eq!(parsed.params["threadId"].as_str(), Some("abc"));
    assert_eq!(parsed.params["includeTurns"].as_bool(), Some(false));
    assert_eq!(
        parsed.endpoint.url.as_deref(),
        Some("ws://127.0.0.1:49200/rpc")
    );
}

#[test]
fn parse_debug_rpc_rejects_conflicting_endpoint_flags() {
    let args = vec![
        "thread/list",
        "--url",
        "ws://127.0.0.1:49200/rpc",
        "--config",
        "/tmp/cooldis.toml",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let err = parse_debug_rpc_call_args(args).unwrap_err().to_string();

    assert!(err.contains("--url or --config"));
}

#[test]
fn parse_debug_rpc_call_rejects_invalid_params_json() {
    let args = vec!["thread/list", "{not-json"]
        .into_iter()
        .map(OsString::from)
        .collect();

    let err = parse_debug_rpc_call_args(args).unwrap_err().to_string();

    assert!(err.contains("invalid PARAMS_JSON"));
}

#[test]
fn parse_debug_rpc_turn_requires_one_thread_selector_and_text() {
    let both = vec!["--thread", "abc", "--new", "hello"]
        .into_iter()
        .map(OsString::from)
        .collect();
    let missing = vec!["--new"].into_iter().map(OsString::from).collect();

    assert!(
        parse_debug_rpc_turn_args(both)
            .unwrap_err()
            .to_string()
            .contains("exactly one of --thread or --new")
    );
    assert!(
        parse_debug_rpc_turn_args(missing)
            .unwrap_err()
            .to_string()
            .contains("requires <text>")
    );
}

#[test]
fn parse_debug_rpc_turn_collects_json_thread_and_text() {
    let args = vec![
        "--thread",
        "thread-1",
        "--json",
        "hello",
        "from",
        "rpc",
        "--config",
        "/tmp/cooldis.toml",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let parsed = parse_debug_rpc_turn_args(args).unwrap();

    match parsed.target {
        DebugRpcThreadTarget::Existing(thread_id) => assert_eq!(thread_id, "thread-1"),
        DebugRpcThreadTarget::New => panic!("expected existing thread target"),
    }
    assert!(parsed.json);
    assert_eq!(parsed.text, "hello from rpc");
    assert_eq!(
        parsed.endpoint.config,
        Some(PathBuf::from("/tmp/cooldis.toml"))
    );
}

#[test]
fn notification_error_detection_handles_failed_completed_turn() {
    let notification = JsonRpcNotification {
        method: "turn/completed".to_string(),
        params: Some(json!({
            "threadId": "thread-1",
            "turn": {
                "id": "turn-1",
                "status": "failed",
                "error": { "message": "provider failed" }
            }
        })),
    };

    assert!(notification_is_turn_error(
        &notification,
        "thread-1",
        "turn-1"
    ));
    assert_eq!(
        notification_turn_error_message(&notification),
        "provider failed"
    );
}

#[test]
fn load_chat_provider_config_reads_bifrost_json() {
    let dir = std::env::temp_dir().join(format!("cooldis-chat-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "bifrost_openai",
                "base_url": "https://bifrost.example.test",
                "api_key": "test-key",
                "model": "openai/gpt-5.5",
                "max_tokens": 2048,
                "stream": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::BifrostOpenAI {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(base_url, "https://bifrost.example.test");
            assert_eq!(api_key, "test-key");
            assert_eq!(model, "openai/gpt-5.5");
            assert_eq!(max_tokens, 2048);
            assert!(!stream);
        }
        ChatProviderConfig::Local => panic!("expected bifrost config"),
        ChatProviderConfig::OpenAIChatCompletions { .. } => {
            panic!("expected bifrost responses config")
        }
        ChatProviderConfig::AnthropicMessages { .. } => {
            panic!("expected bifrost responses config")
        }
        ChatProviderConfig::AnthropicBedrock { .. } => {
            panic!("expected bifrost responses config")
        }
        ChatProviderConfig::CatalogOpenAIChatCompletions { .. } => {
            panic!("expected bifrost responses config")
        }
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_reads_anthropic_json() {
    let dir = std::env::temp_dir().join(format!("cooldis-anthropic-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "anthropic",
                "base_url": "https://api.anthropic.com",
                "api_key": "test-anthropic-key",
                "model": "claude-sonnet-4-5-20250929",
                "max_tokens": 1024,
                "stream": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::AnthropicMessages {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(base_url, "https://api.anthropic.com");
            assert_eq!(api_key, "test-anthropic-key");
            assert_eq!(model, "claude-sonnet-4-5-20250929");
            assert_eq!(max_tokens, 1024);
            assert!(!stream);
        }
        _ => panic!("expected Anthropic Messages config"),
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_reads_anthropic_bedrock_env_file() {
    let dir = std::env::temp_dir().join(format!("cooldis-bedrock-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    let env_path = dir.join("bedrock.env");
    fs::write(
        &env_path,
        "\
AWS_ACCESS_KEY_ID=AKIA_TEST
AWS_SECRET_ACCESS_KEY=test-secret
AWS_SESSION_TOKEN=test-session
AWS_BEDROCK_REGION=us-west-2
COOLDIS_ANTHROPIC_BEDROCK_MODEL=us.anthropic.claude-sonnet-4-5-20250929-v1:0
",
    )
    .unwrap();
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "anthropic_bedrock",
                "base_url": "https://bedrock-runtime.us-west-2.amazonaws.com/",
                "env_file": "bedrock.env",
                "max_tokens": 2048,
                "stream": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::AnthropicBedrock {
            region,
            base_url,
            access_key_id,
            secret_access_key,
            session_token,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(region, "us-west-2");
            assert_eq!(
                base_url.as_deref(),
                Some("https://bedrock-runtime.us-west-2.amazonaws.com")
            );
            assert_eq!(access_key_id, "AKIA_TEST");
            assert_eq!(secret_access_key, "test-secret");
            assert_eq!(session_token.as_deref(), Some("test-session"));
            assert_eq!(model, "us.anthropic.claude-sonnet-4-5-20250929-v1:0");
            assert_eq!(max_tokens, 2048);
            assert!(!stream);
        }
        _ => panic!("expected Anthropic Bedrock config"),
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_reads_openai_compatible_json() {
    let dir = std::env::temp_dir().join(format!(
        "cooldis-openai_compatible-config-{}",
        Uuid::now_v7()
    ));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "openai_compatible",
                "api_key": "test-openai_compatible-key",
                "stream": false
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::OpenAIChatCompletions {
            provider,
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
            headers,
        } => {
            assert_eq!(provider, "openai_compatible");
            assert_eq!(base_url, "https://api.example.invalid/v1");
            assert_eq!(api_key, "test-openai_compatible-key");
            assert_eq!(model, APP_SERVER_OPENAI_COMPATIBLE_MODEL);
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
            assert_eq!(
                headers,
                vec![("X-Example-Provider".to_string(), "required".to_string())]
            );
        }
        ChatProviderConfig::Local
        | ChatProviderConfig::BifrostOpenAI { .. }
        | ChatProviderConfig::AnthropicMessages { .. }
        | ChatProviderConfig::AnthropicBedrock { .. } => {
            panic!("expected openai_compatible chat completions config")
        }
        ChatProviderConfig::CatalogOpenAIChatCompletions { .. } => {
            panic!("expected direct openai_compatible chat completions config")
        }
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_provider_config_uses_catalog_for_plain_openai_compatible_without_key() {
    let dir = std::env::temp_dir().join(format!(
        "cooldis-openai_compatible-catalog-{}",
        Uuid::now_v7()
    ));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    let env_path = dir.join("empty.env");
    fs::write(&env_path, "").unwrap();
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                "provider": "openai_compatible",
                "model": "example-chat-model-large",
                "stream": false,
                "env_file": "empty.env"
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    match load_chat_provider_config(&args).unwrap() {
        ChatProviderConfig::CatalogOpenAIChatCompletions {
            provider_id,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(provider_id, "openai_compatible");
            assert_eq!(model.as_deref(), Some("example-chat-model-large"));
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
        }
        _ => panic!("expected catalog-backed openai_compatible config"),
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_daemon_provider_config_uses_catalog_for_plain_openai_compatible_without_key() {
    let dir = std::env::temp_dir().join(format!(
        "cooldis-openai_compatible-daemon-catalog-{}",
        Uuid::now_v7()
    ));
    fs::create_dir_all(&dir).unwrap();
    let env_path = dir.join("empty.env");
    fs::write(&env_path, "").unwrap();
    let config = CooldisProviderConfig {
        provider: Some("openai_compatible".to_string()),
        model: Some("example-chat-model-large".to_string()),
        stream: Some(false),
        env_file: Some(env_path),
        ..CooldisProviderConfig::default()
    };

    match load_daemon_provider_config(&config).unwrap() {
        ChatProviderConfig::CatalogOpenAIChatCompletions {
            provider_id,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(provider_id, "openai_compatible");
            assert_eq!(model.as_deref(), Some("example-chat-model-large"));
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
        }
        _ => panic!("expected catalog-backed openai_compatible daemon config"),
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_daemon_provider_config_reads_anthropic_bedrock_env_file() {
    let dir =
        std::env::temp_dir().join(format!("cooldis-bedrock-daemon-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let env_path = dir.join("bedrock.env");
    fs::write(
        &env_path,
        "\
AWS_ACCESS_KEY_ID=AKIA_DAEMON_TEST
AWS_SECRET_ACCESS_KEY=daemon-secret
AWS_BEDROCK_REGION=us-east-1
AWS_BEDROCK_MODEL=anthropic.claude-sonnet-4-5-20250929-v1:0
",
    )
    .unwrap();
    let config = CooldisProviderConfig {
        provider: Some("anthropic_bedrock".to_string()),
        env_file: Some(env_path),
        stream: Some(false),
        ..CooldisProviderConfig::default()
    };

    match load_daemon_provider_config(&config).unwrap() {
        ChatProviderConfig::AnthropicBedrock {
            region,
            base_url,
            access_key_id,
            secret_access_key,
            session_token,
            model,
            max_tokens,
            stream,
        } => {
            assert_eq!(region, "us-east-1");
            assert_eq!(base_url, None);
            assert_eq!(access_key_id, "AKIA_DAEMON_TEST");
            assert_eq!(secret_access_key, "daemon-secret");
            assert_eq!(session_token, None);
            assert_eq!(model, "anthropic.claude-sonnet-4-5-20250929-v1:0");
            assert_eq!(max_tokens, 4096);
            assert!(!stream);
        }
        _ => panic!("expected Anthropic Bedrock daemon config"),
    }
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn load_chat_operation_bindings_config_resolves_registry_root() {
    let dir = std::env::temp_dir().join(format!("cooldis-operation-config-{}", Uuid::now_v7()));
    fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("cooldis.json");
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "chat": {
                // lexicon-allow: capsule - existing chat config field name
                "capsule_bindings": {
                    "registry_root": "operations",
                    "global_operation_names": ["search"],
                    "load_all_active_when_unbound": true
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let args = parse_chat_args(
        vec!["--config", config_path.to_str().unwrap()]
            .into_iter()
            .map(OsString::from)
            .collect(),
    )
    .unwrap();
    // lexicon-allow: capsule - existing chat config function name
    let bindings = load_chat_capsule_bindings_config(&args).unwrap();
    assert_eq!(bindings.registry_root, Some(dir.join("operations")));
    assert_eq!(bindings.global_operation_names, vec!["search"]);
    assert!(bindings.load_all_active_when_unbound);
    let _ = fs::remove_dir_all(dir);
}

fn daemon_test_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("cooldis-daemon-{name}-{}", Uuid::now_v7()))
}

fn daemon_test_config(root: &Path, workspace: &Path) -> CooldisDaemonConfig {
    let mut config = CooldisDaemonConfig::default();
    config.app_server.listen = format!("unix://{}", root.join("daemon.sock").display());
    config.runtime.cwd = Some(workspace.to_path_buf());
    config.runtime.runtime_home = Some(root.join("runtime"));
    config.runtime.state_home = Some(root.join("state"));
    config
}

async fn publish_daemon_test_operation(
    registry_root: &Path,
    record_name: &str,
    operation_name: &str,
) -> PublishedOperationRecord {
    fs::create_dir_all(registry_root).unwrap();
    let wasm = wat::parse_str(daemon_test_operation_guest(operation_name))
        .expect("daemon test operation fixture should compile");
    let artifact_path = registry_root.join(format!("{record_name}.wasm"));
    fs::write(&artifact_path, wasm).unwrap();
    LocalOperationRegistry::new(registry_root)
        .publish_artifact(PublishOperationRequest {
            name: record_name.to_string(),
            artifact_path: artifact_path.clone(),
            source: PublishedOperationSource::Wasm {
                bin_path: artifact_path,
            },
            interface: None,
            capability_grants: Default::default(),
            metadata: Default::default(),
        })
        .await
        .unwrap()
}

fn publish_daemon_test_agent(
    root: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    name: &str,
    command: &str,
    tool_id: &str,
    operation_ref: &str,
) -> PublishedAgentRecord {
    publish_daemon_test_agent_result(
        root,
        agent_registry_root,
        operation_registry_root,
        name,
        command,
        tool_id,
        operation_ref,
    )
    .unwrap()
}

fn publish_daemon_test_agent_result(
    root: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    name: &str,
    command: &str,
    tool_id: &str,
    operation_ref: &str,
) -> CooldisResult<PublishedAgentRecord> {
    let manifest_path = root.join(format!("{name}.cooldis.agent.toml"));
    fs::write(
        &manifest_path,
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

[[tools]]
type = "bash_tool"
id = "{tool_id}"
command = "{command}"
operation_ref = "{operation_ref}"

[runtime]
default_cwd = "."
streaming = false
"#
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, operation_registry_root)
}

fn daemon_test_operation_guest(operation_name: &str) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": operation_name,
            "input": "bytes",
            "output": "bytes",
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
              (data (i32.const 8192) "ok")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const 2
                  return
                end
                i32.const 0
                i32.const 2
                i32.store
                local.get $output
                i32.const 8192
                i32.const 0
                call $sink_write
                drop
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

fn loaded_daemon_config(config: CooldisDaemonConfig) -> LoadedCooldisDaemonConfig {
    LoadedCooldisDaemonConfig {
        config,
        path: None,
        base_dir: std::env::current_dir().unwrap(),
    }
}
