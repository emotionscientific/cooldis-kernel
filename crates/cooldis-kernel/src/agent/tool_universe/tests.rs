use super::*;
use crate::{
    EventStore, EventStreamId, InMemorySessionStore, ThreadContext, ThreadCoordinates, TurnContext,
    TurnInput,
};
use async_trait::async_trait;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

struct RecordingCaller {
    calls: Arc<Mutex<Vec<Value>>>,
    output: ToolUniverseCallOutput,
}

#[async_trait]
impl ToolUniverseCaller for RecordingCaller {
    async fn call_tool(
        &self,
        _tool_name: &str,
        arguments: Value,
    ) -> CooldisResult<ToolUniverseCallOutput> {
        self.calls.lock().unwrap().push(arguments);
        Ok(self.output.clone())
    }
}

struct StaticDiscoverer {
    discovery: ToolUniverseDiscovery,
}

#[async_trait]
impl ToolUniverseDiscoverer for StaticDiscoverer {
    async fn discover(&self, _server_ref: &str) -> CooldisResult<ToolUniverseDiscovery> {
        Ok(self.discovery.clone())
    }
}

#[test]
fn pin_refs_parse_and_fail_closed() {
    let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let pin = PinnedToolRef::parse(&format!(
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
        assert!(PinnedToolRef::parse(bad).is_err(), "{bad} should fail");
    }
}

#[test]
fn witnessed_contracts_are_schema_hash_addressed() {
    let definition = ToolDefinition::new(
        "GoogleSearch.search",
        "Search the web.",
        serde_json::json!({
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }),
    );
    let contract = WitnessedToolContract::witness(&definition).unwrap();
    assert_eq!(
        contract.schema_hash,
        schema_hash_of(&definition.input_schema).unwrap()
    );
    assert!(contract.schema_hash.starts_with("sha256:"));

    let hex = contract
        .schema_hash
        .trim_start_matches("sha256:")
        .to_string();
    let pin = PinnedToolRef::parse(&format!(
        "mcptool://arcade/GoogleSearch.search@sha256:{hex}"
    ))
    .unwrap();
    assert!(contract.matches_pin(&pin));

    let drifted = PinnedToolRef {
        schema_hash: format!("sha256:{}", "f".repeat(64)),
        ..pin
    };
    assert!(!contract.matches_pin(&drifted));
}

#[test]
fn argument_fingerprint_is_stable_across_object_key_order() {
    let first = serde_json::from_str::<Value>(
        r#"{"outer":{"b":2.50,"a":"\u96ea"},"z":-0.0,"text":"caf\u00e9"}"#,
    )
    .unwrap();
    let second =
        serde_json::from_str::<Value>(r#"{"text":"café","z":-0.0,"outer":{"a":"雪","b":2.5}}"#)
            .unwrap();

    let first = args_fingerprint("search", &first).unwrap();
    let second = args_fingerprint("search", &second).unwrap();

    assert_eq!(first, second);
    assert!(first.starts_with("sha256:"));
    assert_ne!(
        first,
        args_fingerprint(
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
        WitnessedToolContract::witness(&ToolDefinition::new(
            "a.one",
            "first",
            serde_json::json!({"type": "object"}),
        ))
        .unwrap(),
        WitnessedToolContract::witness(&ToolDefinition::new(
            "b.two",
            "second",
            serde_json::json!({"type": "object"}),
        ))
        .unwrap(),
    ];
    let discovery = ToolUniverseDiscovery::witness("mcp://arcade", tools, 1).unwrap();
    let filtered = discovery
        .filtered(&BTreeSet::from(["a.one".to_string()]))
        .unwrap();
    assert_eq!(filtered.tools.len(), 1);
    assert_ne!(filtered.discovery_hash, discovery.discovery_hash);
    assert!(discovery.contract("b.two").is_some());
    assert!(filtered.contract("b.two").is_none());
}

#[test]
fn validate_tool_arguments_accepts_the_mcp_schema_subset() {
    let echo_contract = contract(
        "cooldis_mcp_echo",
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

    validate_tool_arguments(
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
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        }),
    );

    let missing = validate_tool_arguments(&echo_contract, &serde_json::json!({})).unwrap_err();
    assert!(missing.to_string().contains("missing required"));
    let extra = validate_tool_arguments(
        &echo_contract,
        &serde_json::json!({"message": "ok", "extra": true}),
    )
    .unwrap_err();
    assert!(extra.to_string().contains("unexpected property"));
    let wrong_type =
        validate_tool_arguments(&echo_contract, &serde_json::json!({"message": 1})).unwrap_err();
    assert!(wrong_type.to_string().contains("expected string"));

    let unsupported = contract(
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "oneOf": [{"type": "object"}]
        }),
    );
    let err = validate_tool_arguments(&unsupported, &serde_json::json!({})).unwrap_err();
    assert!(err.to_string().contains("unsupported schema keyword"));
}

#[test]
fn validate_tool_arguments_preflights_unreached_schema_branches() {
    let invalid_optional = contract(
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "message": {"type": "string"},
                "unused": {"type": ["string", "wat"]}
            }
        }),
    );
    let err = validate_tool_arguments(&invalid_optional, &serde_json::json!({"message": "ok"}))
        .unwrap_err();
    assert!(err.to_string().contains("unsupported schema type"));

    let invalid_items = contract(
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "array",
            "items": {"oneOf": [{"type": "string"}]}
        }),
    );
    let err = validate_tool_arguments(&invalid_items, &serde_json::json!([])).unwrap_err();
    assert!(err.to_string().contains("unsupported schema keyword"));

    let invalid_ignored_by_type = contract(
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "string",
            "properties": {
                "unused": {"oneOf": [{"type": "string"}]}
            }
        }),
    );
    let err =
        validate_tool_arguments(&invalid_ignored_by_type, &serde_json::json!("ok")).unwrap_err();
    assert!(err.to_string().contains("unsupported schema keyword"));
}

#[test]
fn validate_tool_arguments_bounds_schema_recursion() {
    let mut schema = serde_json::json!({"type": "string"});
    for _ in 0..(MAX_SCHEMA_VALIDATION_DEPTH + 2) {
        schema = serde_json::json!({
            "type": "array",
            "items": schema
        });
    }
    let contract = contract("cooldis_mcp_echo", schema);

    let err = validate_tool_arguments(&contract, &serde_json::json!([])).unwrap_err();

    assert!(err.to_string().contains("schema nesting exceeds limit"));
}

#[test]
fn validate_tool_arguments_accepts_json_numbers_with_integer_value() {
    let integer_contract = contract("cooldis_mcp_echo", serde_json::json!({"type": "integer"}));
    let value = Value::Number(serde_json::Number::from_f64(1.0).unwrap());

    validate_tool_arguments(&integer_contract, &value).unwrap();

    let err = validate_tool_arguments(
        &integer_contract,
        &Value::Number(serde_json::Number::from_f64(1.5).unwrap()),
    )
    .unwrap_err();
    assert!(err.to_string().contains("expected integer"));
}

#[tokio::test]
async fn search_describe_and_call_resolve_the_witnessed_snapshot() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![contract(
            "cooldis_mcp_echo",
            serde_json::json!({
                "type": "object",
                "required": ["message"],
                "additionalProperties": false,
                "properties": {"message": {"type": "string"}}
            }),
        )],
        Arc::new(RecordingCaller {
            calls: Arc::clone(&calls),
            output: ToolUniverseCallOutput {
                content: "COOLDIS_MCP_TOOL_OK message=hello".to_string(),
                is_error: false,
            },
        }),
        None,
    );
    let surface = ToolUniverseSearchSurface::new(vec![mounted]);

    let search = surface
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "search_1".to_string(),
            tool_name: TOOL_SEARCH_TOOL.to_string(),
            arguments: serde_json::json!({"query": "echo"}),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert!(tool_text(&search).contains("cooldis_mcp_echo"));

    let describe = surface
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "describe_1".to_string(),
            tool_name: TOOL_DESCRIBE_TOOL.to_string(),
            arguments: serde_json::json!({"tool": "cooldis_mcp_echo"}),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    let describe_text = tool_text(&describe);
    assert!(describe_text.contains("SCHEMA HASH"));
    assert!(describe_text.contains("mcp://arcade"));

    let call = surface
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: TOOL_CALL_TOOL.to_string(),
            arguments: serde_json::json!({
                "tool": "cooldis_mcp_echo",
                "arguments": {"message": "hello"}
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tool_text(&call), "COOLDIS_MCP_TOOL_OK message=hello");
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [serde_json::json!({"message": "hello"})]
    );
}

#[tokio::test]
async fn protocol_tool_import_grant_lapse_fails_before_the_live_caller() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mounted = mounted_universe(
        "mcp://arcade",
        vec![contract(
            "cooldis_mcp_echo",
            serde_json::json!({"type": "object", "additionalProperties": true}),
        )],
        Arc::new(RecordingCaller {
            calls: Arc::clone(&calls),
            output: ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        None,
    );
    mounted.binding.grant_expiries = vec![crate::AgentManifestGrantExpiry {
        capability: "net.localhost".to_string(),
        expires_at: "1970-01-01T00:00:01Z".to_string(),
    }];
    let surface = ToolUniverseSearchSurface::new(vec![mounted]);

    let err = surface
        .invoke_tool_call_at(
            AgentKernelToolCall {
                call_id: "call_expired".to_string(),
                tool_name: TOOL_CALL_TOOL.to_string(),
                arguments: serde_json::json!({
                    "tool": "cooldis_mcp_echo",
                    "arguments": {"message": "hello"}
                }),
                turn_context: None,
            },
            1_001,
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("missing capability grants: net.localhost")
    );
    assert!(err.to_string().contains("1970-01-01T00:00:01Z"));
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancellable_protocol_tool_call_honors_the_injected_expiry_time() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mounted = mounted_universe(
        "mcp://arcade",
        vec![contract(
            "cooldis_mcp_echo",
            serde_json::json!({"type": "object", "additionalProperties": true}),
        )],
        Arc::new(RecordingCaller {
            calls: Arc::clone(&calls),
            output: ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        None,
    );
    mounted.binding.grant_expiries = vec![crate::AgentManifestGrantExpiry {
        capability: "net.localhost".to_string(),
        expires_at: "2050-01-01T00:00:00Z".to_string(),
    }];
    let surface = ToolUniverseSearchSurface::new(vec![mounted]);

    let err = surface
        .invoke_tool_call_cancellable_at(
            AgentKernelToolCall {
                call_id: "call_expired".to_string(),
                tool_name: TOOL_CALL_TOOL.to_string(),
                arguments: serde_json::json!({
                    "tool": "cooldis_mcp_echo",
                    "arguments": {"message": "hello"}
                }),
                turn_context: None,
            },
            crate::ToolInvocationCancellation::never(),
            2_524_608_000_001,
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("missing capability grants: net.localhost")
    );
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn tool_call_validation_failure_does_not_touch_the_universe() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![contract(
            "cooldis_mcp_echo",
            serde_json::json!({
                "type": "object",
                "required": ["message"],
                "additionalProperties": false,
                "properties": {"message": {"type": "string"}}
            }),
        )],
        Arc::new(RecordingCaller {
            calls: Arc::clone(&calls),
            output: ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        None,
    );
    let surface = ToolUniverseSearchSurface::new(vec![mounted]);

    let result = surface
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: TOOL_CALL_TOOL.to_string(),
            arguments: serde_json::json!({
                "tool": "cooldis_mcp_echo",
                "arguments": {}
            }),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult { is_error: true, .. }
    ));
    assert!(tool_text(&result).contains("missing required"));
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn validation_failure_records_error_receipt_when_context_is_available() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![contract(
            "cooldis_mcp_echo",
            serde_json::json!({
                "type": "object",
                "required": ["message"],
                "additionalProperties": false,
                "properties": {"message": {"type": "string"}}
            }),
        )],
        Arc::new(RecordingCaller {
            calls: Arc::clone(&calls),
            output: ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        None,
    );
    let store = Arc::new(InMemorySessionStore::new());
    let surface = ToolUniverseSearchSurface {
        universes: vec![mounted],
        event_store: Some(store.clone()),
        live_discoverer: None,
    };
    let turn_context = TurnContext::new(
        ThreadContext::root(ThreadCoordinates::new("tenant_a", "user_1", "session_1")),
        "turn_1",
        &TurnInput::text("call it"),
        CancellationToken::new(),
    )
    .snapshot();

    let result = surface
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: TOOL_CALL_TOOL.to_string(),
            arguments: serde_json::json!({
                "tool": "cooldis_mcp_echo",
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
        .read_events(&EventStreamId::for_thread(&turn_context.coordinates), None)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, EventKind::ToolUniverseCallCompleted);
    assert_eq!(events[0].payload["is_error"].as_bool(), Some(true));
    let output_hash = sha256_hex(content.as_bytes());
    assert_eq!(
        events[0].payload["output_hash"].as_str(),
        Some(output_hash.as_str())
    );
}

#[tokio::test]
async fn unqualified_tool_names_must_be_unambiguous() {
    let caller = Arc::new(RecordingCaller {
        calls: Arc::new(Mutex::new(Vec::new())),
        output: ToolUniverseCallOutput {
            content: "ok".to_string(),
            is_error: false,
        },
    });
    let surface = ToolUniverseSearchSurface::new(vec![
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
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "describe_1".to_string(),
            tool_name: TOOL_DESCRIBE_TOOL.to_string(),
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
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        }),
    );
    let live_drift = contract(
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "number"}}
        }),
    );
    let hash = pinned.schema_hash.trim_start_matches("sha256:");
    let pin =
        PinnedToolRef::parse(&format!("mcptool://arcade/cooldis_mcp_echo@sha256:{hash}")).unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![pinned],
        Arc::new(RecordingCaller {
            calls: Arc::clone(&calls),
            output: ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        Some(pin),
    );
    let discoverer = Arc::new(StaticDiscoverer {
        discovery: ToolUniverseDiscovery::witness("mcp://arcade", vec![live_drift], 2).unwrap(),
    });
    let surface = ToolUniverseSearchSurface {
        universes: vec![mounted],
        event_store: None,
        live_discoverer: Some(discoverer),
    };

    let result = surface
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: "cooldis_mcp_echo".to_string(),
            arguments: serde_json::json!({"message": "hello"}),
            turn_context: None,
        })
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        result,
        CanonicalMessage::ToolResult { is_error: true, .. }
    ));
    assert!(tool_text(&result).contains("drifted"));
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pinned_direct_row_rechecks_live_schema_before_argument_validation() {
    let pinned = contract(
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        }),
    );
    let live_drift = contract(
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "number"}}
        }),
    );
    let hash = pinned.schema_hash.trim_start_matches("sha256:");
    let pin =
        PinnedToolRef::parse(&format!("mcptool://arcade/cooldis_mcp_echo@sha256:{hash}")).unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![pinned],
        Arc::new(RecordingCaller {
            calls: Arc::clone(&calls),
            output: ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        Some(pin),
    );
    let discoverer = Arc::new(StaticDiscoverer {
        discovery: ToolUniverseDiscovery::witness("mcp://arcade", vec![live_drift], 2).unwrap(),
    });
    let surface = ToolUniverseSearchSurface {
        universes: vec![mounted],
        event_store: None,
        live_discoverer: Some(discoverer),
    };

    let result = surface
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: "cooldis_mcp_echo".to_string(),
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
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        }),
    );
    let drifted_snapshot = contract(
        "cooldis_mcp_echo",
        serde_json::json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "number"}}
        }),
    );
    let hash = pinned.schema_hash.trim_start_matches("sha256:");
    let pin =
        PinnedToolRef::parse(&format!("mcptool://arcade/cooldis_mcp_echo@sha256:{hash}")).unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mounted = mounted_universe(
        "mcp://arcade",
        vec![drifted_snapshot],
        Arc::new(RecordingCaller {
            calls: Arc::clone(&calls),
            output: ToolUniverseCallOutput {
                content: "should not run".to_string(),
                is_error: false,
            },
        }),
        Some(pin),
    );
    let discoverer = Arc::new(StaticDiscoverer {
        discovery: ToolUniverseDiscovery::witness("mcp://arcade", vec![pinned], 2).unwrap(),
    });
    let surface = ToolUniverseSearchSurface {
        universes: vec![mounted],
        event_store: None,
        live_discoverer: Some(discoverer),
    };

    let err = surface
        .invoke_tool_call(AgentKernelToolCall {
            call_id: "call_1".to_string(),
            tool_name: "cooldis_mcp_echo".to_string(),
            arguments: serde_json::json!({"message": 1}),
            turn_context: None,
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("binding drift"));
    assert!(calls.lock().unwrap().is_empty());
}

fn contract(tool_name: &str, input_schema: Value) -> WitnessedToolContract {
    WitnessedToolContract::witness(&ToolDefinition::new(
        tool_name,
        format!("Description for {tool_name}."),
        input_schema,
    ))
    .unwrap()
}

fn mounted_universe(
    server_ref: &str,
    tools: Vec<WitnessedToolContract>,
    caller: Arc<dyn ToolUniverseCaller>,
    pin: Option<PinnedToolRef>,
) -> MountedToolUniverse {
    MountedToolUniverse {
        binding: ToolUniverseBinding {
            import_id: server_ref.trim_start_matches("mcp://").to_string(),
            server_ref: server_ref.to_string(),
            effect_class: EffectClass::AtMostOnce,
            include_tools: None,
            pin,
            grant_expiries: Vec::new(),
            discovery: ToolUniverseDiscovery::witness(server_ref, tools, 1).unwrap(),
        },
        caller,
    }
}

fn tool_text(message: &CanonicalMessage) -> String {
    match message {
        CanonicalMessage::ToolResult { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                crate::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}
