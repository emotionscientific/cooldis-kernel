use super::*;
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
