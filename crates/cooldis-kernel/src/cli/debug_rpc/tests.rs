use super::*;
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
fn rpc_tail_treats_remote_connection_close_as_success() {
    assert!(rpc_connection_was_closed(&CooldisError::RpcClient(
        "Cooldis RPC connection closed".to_string()
    )));
    assert!(rpc_connection_was_closed(&CooldisError::RpcClient(
        "Cooldis RPC connection was closed by the endpoint: going away".to_string()
    )));
    assert!(!rpc_connection_was_closed(&CooldisError::RpcClient(
        "Cooldis RPC connection read failed: reset".to_string()
    )));
    assert!(!rpc_connection_was_closed(&CooldisError::RuntimeFactory(
        "Cooldis RPC connection closed".to_string()
    )));
}
