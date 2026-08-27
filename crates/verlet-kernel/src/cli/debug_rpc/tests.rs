#[test]
fn parse_debug_rpc_call_accepts_method_params_and_url() {
    let args = vec![
        "thread/read",
        r#"{"threadId":"abc","includeTurns":false}"#,
        "--url",
        "ws://127.0.0.1:49200/rpc",
    ]
    .into_iter()
    .map(std::ffi::OsString::from)
    .collect();

    let parsed = crate::cli::debug_rpc::parse_debug_rpc_call_args(args).unwrap();

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
        "/tmp/verlet.toml",
    ]
    .into_iter()
    .map(std::ffi::OsString::from)
    .collect();

    let err = crate::cli::debug_rpc::parse_debug_rpc_call_args(args)
        .unwrap_err()
        .to_string();

    assert!(err.contains("--url or --config"));
}

#[cfg(unix)]
#[test]
fn resolve_debug_rpc_endpoint_discovers_project_record_from_nested_cwd() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let nested = project.join("nested/worktree");
    let state_root = project.join(".verlet/state");
    let socket = root.path().join("owner.sock");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(project.join("verlet.toml"), "").unwrap();
    crate::adapters::app_server::instance::write_instance_endpoint(
        &state_root,
        &crate::adapters::app_server::instance::InstanceEndpoint {
            pid: std::process::id(),
            unix_socket: socket.clone(),
            ws_url: None,
            started_at: "2026-08-27T00:00:00Z".to_string(),
            instance_id: "debug-discovery-test".to_string(),
        },
    )
    .unwrap();

    let resolved = crate::cli::debug_rpc::resolve_debug_rpc_endpoint_from(
        &crate::cli::debug_rpc::DebugRpcEndpointArgs {
            url: None,
            config: None,
        },
        &nested,
    )
    .unwrap();

    assert_eq!(
        resolved.transport,
        crate::cli::debug_rpc::DebugRpcTransport::Unix(socket)
    );
    assert_eq!(resolved.record_path, Some(state_root.join("endpoint.json")));
}

#[test]
fn resolve_debug_rpc_endpoint_explicit_flags_precede_discovery() {
    let root = tempfile::tempdir().unwrap();
    let config_path = root.path().join("explicit.toml");
    std::fs::write(
        &config_path,
        "[daemon.app_server]\nlisten = \"ws://127.0.0.1:49321/rpc\"\n",
    )
    .unwrap();
    let missing_cwd = root.path().join("missing-cwd");

    let explicit_url = crate::cli::debug_rpc::resolve_debug_rpc_endpoint_from(
        &crate::cli::debug_rpc::DebugRpcEndpointArgs {
            url: Some("unix:///tmp/explicit-debug.sock".to_string()),
            config: None,
        },
        &missing_cwd,
    )
    .unwrap();
    assert_eq!(
        explicit_url.transport,
        crate::cli::debug_rpc::DebugRpcTransport::Unix(std::path::PathBuf::from(
            "/tmp/explicit-debug.sock"
        ))
    );
    assert_eq!(explicit_url.record_path, None);

    let explicit_config = crate::cli::debug_rpc::resolve_debug_rpc_endpoint_from(
        &crate::cli::debug_rpc::DebugRpcEndpointArgs {
            url: None,
            config: Some(config_path),
        },
        &missing_cwd,
    )
    .unwrap();
    assert_eq!(
        explicit_config.transport,
        crate::cli::debug_rpc::DebugRpcTransport::WebSocket("ws://127.0.0.1:49321/rpc".to_string())
    );
    assert_eq!(explicit_config.record_path, None);
}

#[test]
fn resolve_debug_rpc_endpoint_falls_back_when_record_is_absent() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    std::fs::create_dir_all(&project).unwrap();

    let resolved = crate::cli::debug_rpc::resolve_debug_rpc_endpoint_from(
        &crate::cli::debug_rpc::DebugRpcEndpointArgs {
            url: None,
            config: None,
        },
        &project,
    )
    .unwrap();

    assert_eq!(
        resolved.transport,
        crate::cli::debug_rpc::DebugRpcTransport::WebSocket(
            crate::cli::debug_rpc::DEBUG_RPC_DEFAULT_URL.to_string()
        )
    );
    assert_eq!(resolved.record_path, None);
}

#[cfg(unix)]
#[test]
fn resolve_debug_rpc_endpoint_rejects_dead_record_with_cold_journal_guidance() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let state_root = project.join(".verlet/state");
    std::fs::create_dir_all(&project).unwrap();
    crate::adapters::app_server::instance::write_instance_endpoint(
        &state_root,
        &crate::adapters::app_server::instance::InstanceEndpoint {
            pid: u32::MAX,
            unix_socket: root.path().join("dead-owner.sock"),
            ws_url: None,
            started_at: "2026-08-27T00:00:00Z".to_string(),
            instance_id: "dead-debug-owner".to_string(),
        },
    )
    .unwrap();

    let error = crate::cli::debug_rpc::resolve_debug_rpc_endpoint_from(
        &crate::cli::debug_rpc::DebugRpcEndpointArgs {
            url: None,
            config: None,
        },
        &project,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains(&state_root.join("endpoint.json").display().to_string()));
    assert!(error.contains("recorded pid 4294967295 is not running"));
    assert!(error.contains("debug journal --journal"));
}

#[test]
fn parse_debug_rpc_call_rejects_invalid_params_json() {
    let args = vec!["thread/list", "{not-json"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();

    let err = crate::cli::debug_rpc::parse_debug_rpc_call_args(args)
        .unwrap_err()
        .to_string();

    assert!(err.contains("invalid PARAMS_JSON"));
}

#[test]
fn parse_debug_rpc_turn_requires_one_thread_selector_and_text() {
    let both = vec!["--thread", "abc", "--new", "hello"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    let missing = vec!["--new"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();

    assert!(
        crate::cli::debug_rpc::parse_debug_rpc_turn_args(both)
            .unwrap_err()
            .to_string()
            .contains("exactly one of --thread or --new")
    );
    assert!(
        crate::cli::debug_rpc::parse_debug_rpc_turn_args(missing)
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
        "/tmp/verlet.toml",
    ]
    .into_iter()
    .map(std::ffi::OsString::from)
    .collect();

    let parsed = crate::cli::debug_rpc::parse_debug_rpc_turn_args(args).unwrap();

    match parsed.target {
        crate::cli::debug_rpc::DebugRpcThreadTarget::Existing(thread_id) => {
            assert_eq!(thread_id, "thread-1")
        }
        crate::cli::debug_rpc::DebugRpcThreadTarget::New => {
            panic!("expected existing thread target")
        }
    }
    assert!(parsed.json);
    assert_eq!(parsed.text, "hello from rpc");
    assert_eq!(
        parsed.endpoint.config,
        Some(std::path::PathBuf::from("/tmp/verlet.toml"))
    );
}

#[test]
fn notification_error_detection_handles_failed_completed_turn() {
    let notification = crate::adapters::app_server::connection::JsonRpcNotification {
        method: "turn/completed".to_string(),
        params: Some(serde_json::json!({
            "threadId": "thread-1",
            "turn": {
                "id": "turn-1",
                "status": "failed",
                "error": { "message": "provider failed" }
            }
        })),
    };

    assert!(crate::cli::debug_rpc::notification_is_turn_error(
        &notification,
        "thread-1",
        "turn-1"
    ));
    assert_eq!(
        crate::cli::debug_rpc::notification_turn_error_message(&notification),
        "provider failed"
    );
}

#[test]
fn rpc_tail_treats_remote_connection_close_as_success() {
    assert!(crate::cli::debug_rpc::rpc_connection_was_closed(
        &crate::kernel::runtime_host::VerletError::RpcClient(
            "Verlet RPC connection closed".to_string()
        )
    ));
    assert!(crate::cli::debug_rpc::rpc_connection_was_closed(
        &crate::kernel::runtime_host::VerletError::RpcClient(
            "Verlet RPC connection was closed by the endpoint: going away".to_string()
        )
    ));
    assert!(!crate::cli::debug_rpc::rpc_connection_was_closed(
        &crate::kernel::runtime_host::VerletError::RpcClient(
            "Verlet RPC connection read failed: reset".to_string()
        )
    ));
    assert!(!crate::cli::debug_rpc::rpc_connection_was_closed(
        &crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "Verlet RPC connection closed".to_string()
        )
    ));
}
