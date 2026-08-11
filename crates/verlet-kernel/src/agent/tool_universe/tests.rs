use crate::agent::agent_tool_router::AgentKernelToolProvider as _;
use verlet_history::EventStore as _;

struct RecordingCaller {
    calls: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    output: crate::agent::tool_universe::ToolUniverseCallOutput,
}

#[async_trait::async_trait]
impl crate::agent::tool_universe::ToolUniverseCaller for RecordingCaller {
    async fn call_tool(
        &self,
        _tool_name: &str,
        arguments: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::agent::tool_universe::ToolUniverseCallOutput,
    > {
        self.calls.lock().unwrap().push(arguments);
        Ok(self.output.clone())
    }
}

struct StaticDiscoverer {
    discovery: crate::agent::tool_universe::ToolUniverseDiscovery,
}

#[async_trait::async_trait]
impl crate::agent::tool_universe::ToolUniverseDiscoverer for StaticDiscoverer {
    async fn discover(
        &self,
        _server_ref: &str,
    ) -> crate::kernel::runtime_host::VerletResult<crate::agent::tool_universe::ToolUniverseDiscovery>
    {
        Ok(self.discovery.clone())
    }
}

#[test]
fn pin_refs_parse_and_fail_closed() {
    let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let pin = verlet_agent::tool_ref::PinnedToolRef::parse(&format!(
        "mcptool://arcade/GoogleSearch.search@sha256:{hash}"
    ))
    .unwrap();
    assert_eq!(pin.server, "arcade");
    assert_eq!(pin.tool_name, "GoogleSearch.search");
    assert_eq!(pin.schema_hash, format!("sha256:{hash}"));
    assert_eq!(pin.server_ref(), "mcp://arcade");

    for bad in [
        "mcp://arcade/tool@sha256:00",
        "mcptool://arcade/GoogleSearch.search",
        "mcptool://arcade@sha256:0123",
        &format!(
            "mcptool://arcade/GoogleSearch.search@sha256:{}",
            "A".repeat(64)
        ),
        &format!("mcptool://arcade/@sha256:{hash}"),
        &format!("mcptool:///tool@sha256:{hash}"),
    ] {
        assert!(
            verlet_agent::tool_ref::PinnedToolRef::parse(bad).is_err(),
            "{bad} should fail"
        );
    }
}

#[test]
fn witnessed_contracts_are_schema_hash_addressed() {
    let definition = verlet_provider::ToolDefinition::new(
        "GoogleSearch.search",
        "Search the web.",
        serde_json::json!({
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }),
    );
    let contract =
        crate::agent::tool_universe::WitnessedToolContract::witness(&definition).unwrap();
    assert_eq!(
        contract.schema_hash,
        crate::agent::tool_universe::schema_hash_of(&definition.input_schema).unwrap()
    );
    assert!(contract.schema_hash.starts_with("sha256:"));

    let hex = contract
        .schema_hash
        .trim_start_matches("sha256:")
        .to_string();
    let pin = verlet_agent::tool_ref::PinnedToolRef::parse(&format!(
        "mcptool://arcade/GoogleSearch.search@sha256:{hex}"
    ))
    .unwrap();
    assert!(contract.matches_pin(&pin));

    let drifted = verlet_agent::tool_ref::PinnedToolRef {
        schema_hash: format!("sha256:{}", "f".repeat(64)),
        ..pin
    };
    assert!(!contract.matches_pin(&drifted));
}

#[test]
fn argument_fingerprint_is_stable_across_object_key_order() {
    let first = serde_json::from_str::<serde_json::Value>(
        r#"{"outer":{"b":2.50,"a":"\u96ea"},"z":-0.0,"text":"caf\u00e9"}"#,
    )
    .unwrap();
    let second = serde_json::from_str::<serde_json::Value>(
        r#"{"text":"café","z":-0.0,"outer":{"a":"雪","b":2.5}}"#,
    )
    .unwrap();

    let first = crate::agent::tool_universe::args_fingerprint("search", &first).unwrap();
    let second = crate::agent::tool_universe::args_fingerprint("search", &second).unwrap();

    assert_eq!(first, second);
    assert!(first.starts_with("sha256:"));
    assert_ne!(
        first,
        crate::agent::tool_universe::args_fingerprint(
            "different-search",
            &serde_json::json!({
                "outer": {"a": "雪", "b": 2.5},
                "z": -0.0,
                "text": "café"
            })
        )
        .unwrap()
    );
}

#[test]
fn discovery_filter_restamps_the_hash() {
    let tools = vec![
        crate::agent::tool_universe::WitnessedToolContract::witness(
            &verlet_provider::ToolDefinition::new(
                "a.one",
                "first",
                serde_json::json!({"type": "object"}),
            ),
        )
        .unwrap(),
        crate::agent::tool_universe::WitnessedToolContract::witness(
            &verlet_provider::ToolDefinition::new(
                "b.two",
                "second",
                serde_json::json!({"type": "object"}),
            ),
        )
        .unwrap(),
    ];
    let discovery =
        crate::agent::tool_universe::ToolUniverseDiscovery::witness("mcp://arcade", tools, 1)
            .unwrap();
    let filtered = discovery
        .filtered(&std::collections::BTreeSet::from(["a.one".to_string()]))
        .unwrap();
    assert_eq!(filtered.tools.len(), 1);
    assert_ne!(filtered.discovery_hash, discovery.discovery_hash);
    assert!(discovery.contract("b.two").is_some());
    assert!(filtered.contract("b.two").is_none());
}

#[test]
fn validate_tool_arguments_accepts_the_mcp_schema_subset() {
    let echo_contract = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message", "tags"],
            "additionalProperties": false,
            "properties": {
                "message": {
                    "type": "string",
                    "enum": ["hello"]
                },
                "count": {"type": "integer"},
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "metadata": {
                    "type": "object",
                    "additionalProperties": {"type": "number"}
                }
            }
        }),
    );

    crate::agent::tool_universe::validate_tool_arguments(
        &echo_contract,
        &serde_json::json!({
            "message": "hello",
            "count": 2,
            "tags": ["a", "b"],
            "metadata": {"rank": 1.5}
        }),
    )
    .unwrap();
}

#[test]
fn validate_tool_arguments_fails_closed() {
    let echo_contract = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        }),
    );

    let missing = crate::agent::tool_universe::validate_tool_arguments(
        &echo_contract,
        &serde_json::json!({}),
    )
    .unwrap_err();
    assert!(missing.to_string().contains("missing required"));
    let extra = crate::agent::tool_universe::validate_tool_arguments(
        &echo_contract,
        &serde_json::json!({"message": "ok", "extra": true}),
    )
    .unwrap_err();
    assert!(extra.to_string().contains("unexpected property"));
    let wrong_type = crate::agent::tool_universe::validate_tool_arguments(
        &echo_contract,
        &serde_json::json!({"message": 1}),
    )
    .unwrap_err();
    assert!(wrong_type.to_string().contains("expected string"));

    let unsupported = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "oneOf": [{"type": "object"}]
        }),
    );
    let err =
        crate::agent::tool_universe::validate_tool_arguments(&unsupported, &serde_json::json!({}))
            .unwrap_err();
    assert!(err.to_string().contains("unsupported schema keyword"));
}

#[test]
fn validate_tool_arguments_preflights_unreached_schema_branches() {
    let invalid_optional = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "message": {"type": "string"},
                "unused": {"type": ["string", "wat"]}
            }
        }),
    );
    let err = crate::agent::tool_universe::validate_tool_arguments(
        &invalid_optional,
        &serde_json::json!({"message": "ok"}),
    )
    .unwrap_err();
    assert!(err.to_string().contains("unsupported schema type"));

    let invalid_items = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "array",
            "items": {"oneOf": [{"type": "string"}]}
        }),
    );
    let err = crate::agent::tool_universe::validate_tool_arguments(
        &invalid_items,
        &serde_json::json!([]),
    )
    .unwrap_err();
    assert!(err.to_string().contains("unsupported schema keyword"));

    let invalid_ignored_by_type = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "string",
            "properties": {
                "unused": {"oneOf": [{"type": "string"}]}
            }
        }),
    );
    let err = crate::agent::tool_universe::validate_tool_arguments(
        &invalid_ignored_by_type,
        &serde_json::json!("ok"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("unsupported schema keyword"));
}

#[test]
fn validate_tool_arguments_bounds_schema_recursion() {
    let mut schema = serde_json::json!({"type": "string"});
    for _ in 0..(crate::agent::tool_universe::MAX_SCHEMA_VALIDATION_DEPTH + 2) {
        schema = serde_json::json!({
            "type": "array",
            "items": schema
        });
    }
    let contract = contract("verlet_mcp_echo", schema);

    let err =
        crate::agent::tool_universe::validate_tool_arguments(&contract, &serde_json::json!([]))
            .unwrap_err();

    assert!(err.to_string().contains("schema nesting exceeds limit"));
}

#[test]
fn validate_tool_arguments_accepts_json_numbers_with_integer_value() {
    let integer_contract = contract("verlet_mcp_echo", serde_json::json!({"type": "integer"}));
    let value = serde_json::Value::Number(serde_json::Number::from_f64(1.0).unwrap());

    crate::agent::tool_universe::validate_tool_arguments(&integer_contract, &value).unwrap();

    let err = crate::agent::tool_universe::validate_tool_arguments(
        &integer_contract,
        &serde_json::Value::Number(serde_json::Number::from_f64(1.5).unwrap()),
    )
    .unwrap_err();
    assert!(err.to_string().contains("expected integer"));
}

#[tokio::test]
async fn search_describe_and_call_resolve_the_witnessed_snapshot() {
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![contract(
            "verlet_mcp_echo",
            serde_json::json!({
                "type": "object",
                "required": ["message"],
                "additionalProperties": false,
                "properties": {"message": {"type": "string"}}
            }),
        )],
        std::sync::Arc::new(RecordingCaller {
            calls: std::sync::Arc::clone(&calls),
            output: crate::agent::tool_universe::ToolUniverseCallOutput {
                content: "VERLET_MCP_TOOL_OK message=hello".to_string(),
                is_error: false,
            },
        }),
        None,
    );
    let surface = crate::agent::tool_universe::ToolUniverseSearchSurface::new(vec![mounted]);

    let search = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "search_1".to_string(),
            tool_name: crate::agent::tool_universe::TOOL_SEARCH_TOOL.to_string(),
            arguments: serde_json::json!({"query": "echo"}),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert!(tool_text(&search).contains("verlet_mcp_echo"));

    let describe = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "describe_1".to_string(),
            tool_name: crate::agent::tool_universe::TOOL_DESCRIBE_TOOL.to_string(),
            arguments: serde_json::json!({"tool": "verlet_mcp_echo"}),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let describe_text = tool_text(&describe);
    assert!(describe_text.contains("SCHEMA HASH"));
    assert!(describe_text.contains("mcp://arcade"));

    let call = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: crate::agent::tool_universe::TOOL_CALL_TOOL.to_string(),
            arguments: serde_json::json!({
                "tool": "verlet_mcp_echo",
                "arguments": {"message": "hello"}
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tool_text(&call), "VERLET_MCP_TOOL_OK message=hello");
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [serde_json::json!({"message": "hello"})]
    );
}

#[tokio::test]
async fn tool_call_validation_failure_does_not_touch_the_universe() {
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![contract(
            "verlet_mcp_echo",
            serde_json::json!({
                "type": "object",
                "required": ["message"],
                "additionalProperties": false,
                "properties": {"message": {"type": "string"}}
            }),
        )],
        std::sync::Arc::new(RecordingCaller {
            calls: std::sync::Arc::clone(&calls),
            output: crate::agent::tool_universe::ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        None,
    );
    let surface = crate::agent::tool_universe::ToolUniverseSearchSurface::new(vec![mounted]);

    let result = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: crate::agent::tool_universe::TOOL_CALL_TOOL.to_string(),
            arguments: serde_json::json!({
                "tool": "verlet_mcp_echo",
                "arguments": {}
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult { is_error: true, .. }
    ));
    assert!(tool_text(&result).contains("missing required"));
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn validation_failure_records_error_receipt_when_context_is_available() {
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![contract(
            "verlet_mcp_echo",
            serde_json::json!({
                "type": "object",
                "required": ["message"],
                "additionalProperties": false,
                "properties": {"message": {"type": "string"}}
            }),
        )],
        std::sync::Arc::new(RecordingCaller {
            calls: std::sync::Arc::clone(&calls),
            output: crate::agent::tool_universe::ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        None,
    );
    let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let surface = crate::agent::tool_universe::ToolUniverseSearchSurface {
        universes: vec![mounted],
        event_store: Some(store.clone()),
        live_discoverer: None,
    };
    let turn_context = crate::kernel::runtime_host::turn::TurnContext::new(
        verlet_runtime_contracts::ThreadContext::root(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
        ),
        "turn_1",
        &crate::kernel::runtime_host::turn::TurnInput::text("call it"),
        tokio_util::sync::CancellationToken::new(),
    )
    .snapshot();

    let result = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: crate::agent::tool_universe::TOOL_CALL_TOOL.to_string(),
            arguments: serde_json::json!({
                "tool": "verlet_mcp_echo",
                "arguments": {}
            }),
            turn_context: Some(turn_context.clone()),
        })
        .await
        .unwrap()
        .unwrap();

    let content = tool_text(&result);
    assert!(content.contains("missing required"));
    assert!(calls.lock().unwrap().is_empty());
    let events = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&turn_context.coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].kind,
        verlet_history::EventKind::ToolUniverseCallCompleted
    );
    assert_eq!(events[0].payload["is_error"].as_bool(), Some(true));
    let output_hash = verlet_agent::contracts::sha256_hex(content.as_bytes());
    assert_eq!(
        events[0].payload["output_hash"].as_str(),
        Some(output_hash.as_str())
    );
}

#[tokio::test]
async fn unqualified_tool_names_must_be_unambiguous() {
    let caller = std::sync::Arc::new(RecordingCaller {
        calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        output: crate::agent::tool_universe::ToolUniverseCallOutput {
            content: "ok".to_string(),
            is_error: false,
        },
    });
    let surface = crate::agent::tool_universe::ToolUniverseSearchSurface::new(vec![
        mounted_universe(
            "mcp://arcade",
            vec![contract(
                "shared.search",
                serde_json::json!({"type": "object", "additionalProperties": false}),
            )],
            caller.clone(),
            None,
        ),
        mounted_universe(
            "mcp://research",
            vec![contract(
                "shared.search",
                serde_json::json!({"type": "object", "additionalProperties": false}),
            )],
            caller,
            None,
        ),
    ]);

    let err = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "describe_1".to_string(),
            tool_name: crate::agent::tool_universe::TOOL_DESCRIBE_TOOL.to_string(),
            arguments: serde_json::json!({"tool": "shared.search"}),
            turn_context: None,
        })
        .await
        .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("ambiguous"));
    assert!(text.contains("mcp://arcade::shared.search"));
    assert!(text.contains("mcp://research::shared.search"));
}

#[tokio::test]
async fn pinned_direct_row_rechecks_live_schema_hash_before_calling() {
    let pinned = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        }),
    );
    let live_drift = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "number"}}
        }),
    );
    let hash = pinned.schema_hash.trim_start_matches("sha256:");
    let pin = verlet_agent::tool_ref::PinnedToolRef::parse(&format!(
        "mcptool://arcade/verlet_mcp_echo@sha256:{hash}"
    ))
    .unwrap();
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![pinned],
        std::sync::Arc::new(RecordingCaller {
            calls: std::sync::Arc::clone(&calls),
            output: crate::agent::tool_universe::ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        Some(pin),
    );
    let discoverer = std::sync::Arc::new(StaticDiscoverer {
        discovery: crate::agent::tool_universe::ToolUniverseDiscovery::witness(
            "mcp://arcade",
            vec![live_drift],
            2,
        )
        .unwrap(),
    });
    let surface = crate::agent::tool_universe::ToolUniverseSearchSurface {
        universes: vec![mounted],
        event_store: None,
        live_discoverer: Some(discoverer),
    };

    let result = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: "verlet_mcp_echo".to_string(),
            arguments: serde_json::json!({"message": "hello"}),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult { is_error: true, .. }
    ));
    assert!(tool_text(&result).contains("drifted"));
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pinned_direct_row_rechecks_live_schema_before_argument_validation() {
    let pinned = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        }),
    );
    let live_drift = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "number"}}
        }),
    );
    let hash = pinned.schema_hash.trim_start_matches("sha256:");
    let pin = verlet_agent::tool_ref::PinnedToolRef::parse(&format!(
        "mcptool://arcade/verlet_mcp_echo@sha256:{hash}"
    ))
    .unwrap();
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![pinned],
        std::sync::Arc::new(RecordingCaller {
            calls: std::sync::Arc::clone(&calls),
            output: crate::agent::tool_universe::ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        Some(pin),
    );
    let discoverer = std::sync::Arc::new(StaticDiscoverer {
        discovery: crate::agent::tool_universe::ToolUniverseDiscovery::witness(
            "mcp://arcade",
            vec![live_drift],
            2,
        )
        .unwrap(),
    });
    let surface = crate::agent::tool_universe::ToolUniverseSearchSurface {
        universes: vec![mounted],
        event_store: None,
        live_discoverer: Some(discoverer),
    };

    let result = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: "verlet_mcp_echo".to_string(),
            arguments: serde_json::json!({"message": 1}),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();

    assert!(tool_text(&result).contains("drifted"));
    assert!(!tool_text(&result).contains("expected string"));
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pinned_direct_row_refuses_binding_schema_drift_before_live_call() {
    let pinned = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        }),
    );
    let drifted_snapshot = contract(
        "verlet_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "number"}}
        }),
    );
    let hash = pinned.schema_hash.trim_start_matches("sha256:");
    let pin = verlet_agent::tool_ref::PinnedToolRef::parse(&format!(
        "mcptool://arcade/verlet_mcp_echo@sha256:{hash}"
    ))
    .unwrap();
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![drifted_snapshot],
        std::sync::Arc::new(RecordingCaller {
            calls: std::sync::Arc::clone(&calls),
            output: crate::agent::tool_universe::ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        Some(pin),
    );
    let discoverer = std::sync::Arc::new(StaticDiscoverer {
        discovery: crate::agent::tool_universe::ToolUniverseDiscovery::witness(
            "mcp://arcade",
            vec![pinned],
            2,
        )
        .unwrap(),
    });
    let surface = crate::agent::tool_universe::ToolUniverseSearchSurface {
        universes: vec![mounted],
        event_store: None,
        live_discoverer: Some(discoverer),
    };

    let err = surface
        .invoke_tool_call(crate::agent::agent_tool_router::AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: "verlet_mcp_echo".to_string(),
            arguments: serde_json::json!({"message": 1}),
            turn_context: None,
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("binding drift"));
    assert!(calls.lock().unwrap().is_empty());
}

fn contract(
    tool_name: &str,
    input_schema: serde_json::Value,
) -> crate::agent::tool_universe::WitnessedToolContract {
    crate::agent::tool_universe::WitnessedToolContract::witness(
        &verlet_provider::ToolDefinition::new(
            tool_name,
            format!("Description for {tool_name}."),
            input_schema,
        ),
    )
    .unwrap()
}

fn mounted_universe(
    server_ref: &str,
    tools: Vec<crate::agent::tool_universe::WitnessedToolContract>,
    caller: std::sync::Arc<dyn crate::agent::tool_universe::ToolUniverseCaller>,
    pin: Option<verlet_agent::tool_ref::PinnedToolRef>,
) -> crate::agent::tool_universe::MountedToolUniverse {
    crate::agent::tool_universe::MountedToolUniverse {
        binding: crate::agent::tool_universe::ToolUniverseBinding {
            import_id: server_ref.trim_start_matches("mcp://").to_string(),
            server_ref: server_ref.to_string(),
            effect_class: verlet_agent::manifest_schema::EffectClass::AtMostOnce,
            include_tools: None,
            pin,
            discovery: crate::agent::tool_universe::ToolUniverseDiscovery::witness(
                server_ref, tools, 1,
            )
            .unwrap(),
        },
        caller,
    }
}

fn tool_text(message: &verlet_history::CanonicalMessage) -> String {
    match message {
        verlet_history::CanonicalMessage::ToolResult { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}
