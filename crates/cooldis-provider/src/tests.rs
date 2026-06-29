use super::*;
use cooldis_history::{CacheControl, ThinkingMetadata, ThinkingProvider};

fn base_request(api: ProviderApi) -> ProviderRequest {
    let mut request = ProviderRequest::new(api, "provider", "model-test");
    request.system = vec![SystemBlock::text("base")];
    request.tools = vec![ToolDefinition::new(
        "bash",
        "run command",
        json!({"type":"object","properties":{"command":{"type":"string"}}}),
    )];
    request
}

#[test]
fn provider_endpoint_openai_helpers_accept_root_or_v1_base_urls() {
    assert_eq!(
        ProviderEndpoint::openai_responses("https://api.example.test", "token").url,
        "https://api.example.test/v1/responses"
    );
    assert_eq!(
        ProviderEndpoint::openai_responses("https://api.example.test/v1", "token").url,
        "https://api.example.test/v1/responses"
    );
    assert_eq!(
        ProviderEndpoint::openai_chat_completions("https://api.example.invalid/v1", "token").url,
        "https://api.example.invalid/v1/chat/completions"
    );
    assert_eq!(
        ProviderEndpoint::anthropic_messages("https://api.anthropic.com", "token").url,
        "https://api.anthropic.com/v1/messages"
    );
    assert_eq!(
        ProviderEndpoint::anthropic_messages("https://proxy.example.test/anthropic/v1", "token")
            .url,
        "https://proxy.example.test/anthropic/v1/messages"
    );
    assert_eq!(
        ProviderEndpoint::anthropic_bedrock(
            "us-east-1",
            "anthropic.claude-sonnet-4-5-20250929-v1:0",
            "akid",
            "secret",
            None,
        )
        .url,
        "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-sonnet-4-5-20250929-v1%3A0/invoke"
    );
}

#[test]
fn aws_sigv4_canonical_uri_double_encodes_escaped_model_suffix() {
    let endpoint = ProviderEndpoint::anthropic_bedrock(
        "us-east-1",
        "anthropic.claude-sonnet-4-5-20250929-v1:0",
        "akid",
        "secret",
        None,
    );
    let url = reqwest::Url::parse(&endpoint.url).unwrap();

    assert_eq!(
        canonical_uri(&url),
        "/model/anthropic.claude-sonnet-4-5-20250929-v1%253A0/invoke"
    );
}

#[test]
fn provider_capabilities_are_explicit_and_queryable() {
    let responses = OpenAIResponsesAdapter::default().capabilities();
    assert!(responses.supports_tools);
    assert!(responses.supports_streaming);
    assert!(responses.supports_reasoning);
    assert!(responses.supports_images);
    assert!(!responses.supports_cache_control);
    assert!(
        responses
            .supported_abi_projections
            .contains(&ProviderAbiProjection::LlmTool)
    );

    let chat = OpenAIChatCompletionsAdapter.capabilities();
    assert!(chat.supports_tools);
    assert!(chat.supports_streaming);
    assert!(chat.supports_reasoning);
    assert!(!chat.supports_images);

    let anthropic = AnthropicMessagesAdapter.capabilities();
    assert!(anthropic.supports_cache_control);
    assert!(anthropic.supports_reasoning);
    assert!(anthropic.supports_images);

    let bedrock = AnthropicBedrockMessagesAdapter.capabilities();
    assert!(bedrock.supports_cache_control);
    assert!(bedrock.supports_reasoning);
    assert!(bedrock.supports_images);
    assert!(!bedrock.supports_streaming);

    let local = LocalOfflineProviderClient::new("local_offline", "echo")
        .capabilities()
        .unwrap();
    assert_eq!(local.api, ProviderApi::Other("local_offline".to_string()));
    assert_eq!(
        local.supported_abi_projections,
        BTreeSet::from([ProviderAbiProjection::Text])
    );
    assert!(!local.supports_tools);
}

#[test]
fn openai_responses_budget_thinking_fails_closed() {
    let mut request = base_request(ProviderApi::OpenAIResponses);
    request.thinking = Some(ThinkingConfig::Budget {
        budget_tokens: 1024,
    });
    for body in [
        OpenAIResponsesAdapter::default().build_request_body(&request),
        OpenAIResponsesAdapter::default().build_stream_request_body(&request),
    ] {
        assert!(matches!(
            body.unwrap_err(),
            ProviderError::UnsupportedCapability {
                capability: "thinking_budget",
                ..
            }
        ));
    }

    request.thinking = Some(ThinkingConfig::Effort {
        effort: ThinkingEffort::High,
    });
    let body = OpenAIResponsesAdapter::default()
        .build_request_body(&request)
        .unwrap();
    assert_eq!(
        body["reasoning"]["effort"],
        json!(ThinkingEffort::High.as_openai_wire())
    );

    request.thinking = None;
    let body = OpenAIResponsesAdapter::default()
        .build_request_body(&request)
        .unwrap();
    assert!(body.get("reasoning").is_none());
}

#[test]
fn unsupported_provider_capabilities_fail_closed() {
    let mut cached = base_request(ProviderApi::OpenAIResponses);
    cached.system = vec![SystemBlock::cached("base")];
    let err = OpenAIResponsesAdapter::default()
        .build_request_body(&cached)
        .unwrap_err();
    assert!(matches!(
        err,
        ProviderError::UnsupportedCapability {
            capability: "cache_control",
            ..
        }
    ));

    let mut image = base_request(ProviderApi::OpenAIChatCompletions);
    image.messages = vec![CanonicalMessage::User {
        content: vec![CanonicalContent::Image {
            data: "aW1hZ2U=".to_string(),
            mime_type: "image/png".to_string(),
        }],
        timestamp_ms: 0,
    }];
    let err = OpenAIChatCompletionsAdapter
        .build_request_body(&image)
        .unwrap_err();
    assert!(matches!(
        err,
        ProviderError::UnsupportedCapability {
            capability: "images",
            ..
        }
    ));
}

#[test]
fn openai_chat_thinking_request_mapping_covers_efforts_and_provider_gate() {
    for effort in [
        ThinkingEffort::Low,
        ThinkingEffort::Medium,
        ThinkingEffort::High,
    ] {
        for (provider, zhipu_convention) in [("openai_compatible", true), ("provider", false)] {
            let mut request = base_request(ProviderApi::OpenAIChatCompletions);
            request.provider = provider.to_string();
            request.thinking = Some(ThinkingConfig::Effort {
                effort: effort.clone(),
            });

            let complete_body = OpenAIChatCompletionsAdapter
                .build_request_body(&request)
                .unwrap();
            let stream_body = OpenAIChatCompletionsAdapter
                .build_stream_request_body(&request)
                .unwrap();

            for body in [complete_body, stream_body] {
                assert_eq!(body["reasoning_effort"], effort.as_openai_wire());
                if zhipu_convention {
                    assert_eq!(body["thinking"], json!({"type": "enabled"}));
                } else {
                    assert!(body.get("thinking").is_none());
                }
            }
        }
    }
}

#[test]
fn openai_chat_thinking_request_mapping_fails_closed_for_unsupported_modes() {
    for (thinking, expected_capability) in [
        (
            ThinkingConfig::Budget {
                budget_tokens: 1024,
            },
            "thinking_budget",
        ),
        (
            ThinkingConfig::Effort {
                effort: ThinkingEffort::XHigh,
            },
            "thinking_effort",
        ),
        (
            ThinkingConfig::Effort {
                effort: ThinkingEffort::Max,
            },
            "thinking_effort",
        ),
        (
            ThinkingConfig::Effort {
                effort: ThinkingEffort::Other("extreme".to_string()),
            },
            "thinking_effort",
        ),
    ] {
        let mut request = base_request(ProviderApi::OpenAIChatCompletions);
        request.thinking = Some(thinking);

        for body in [
            OpenAIChatCompletionsAdapter.build_request_body(&request),
            OpenAIChatCompletionsAdapter.build_stream_request_body(&request),
        ] {
            assert!(matches!(
                body.unwrap_err(),
                ProviderError::UnsupportedCapability {
                    capability,
                    ..
                } if capability == expected_capability
            ));
        }
    }
}

#[test]
fn openai_chat_disabled_thinking_only_serializes_for_zhipu_convention_providers() {
    for (provider, expected_thinking) in [
        ("openai_compatible", Some(json!({"type": "disabled"}))),
        ("zhipu", Some(json!({"type": "disabled"}))),
        ("glm", Some(json!({"type": "disabled"}))),
        ("provider", None),
    ] {
        let mut request = base_request(ProviderApi::OpenAIChatCompletions);
        request.provider = provider.to_string();
        request.thinking = Some(ThinkingConfig::Disabled);

        let body = OpenAIChatCompletionsAdapter
            .build_request_body(&request)
            .unwrap();
        assert_eq!(body.get("thinking"), expected_thinking.as_ref());
        assert!(body.get("reasoning_effort").is_none());
    }

    let mut request = base_request(ProviderApi::OpenAIChatCompletions);
    request.provider = "openai_compatible".to_string();
    request.thinking = None;
    let body = OpenAIChatCompletionsAdapter
        .build_request_body(&request)
        .unwrap();
    assert!(body.get("thinking").is_none());
    assert!(body.get("reasoning_effort").is_none());
}

#[test]
fn provider_context_compilation_is_deterministic() {
    let messages = vec![
        CanonicalMessage::user_text("old"),
        CanonicalMessage::assistant(
            "openai",
            ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![CanonicalContent::text("middle")],
            CanonicalStopReason::EndTurn,
        ),
        CanonicalMessage::user_text("abcdef"),
    ];
    let policy = ProviderContextPolicy {
        max_messages: Some(2),
        max_text_bytes: Some(5),
    };

    let first = compile_provider_context(messages.clone(), &policy);
    let second = compile_provider_context(messages, &policy);

    assert_eq!(first, second);
    assert_eq!(first.dropped_messages, 1);
    assert_eq!(first.retained_text_bytes, 5);
    assert_eq!(first.truncated_text_bytes, 7);
    assert!(matches!(
        &first.messages[1],
        CanonicalMessage::User { content, .. }
            if matches!(
                &content[0],
                CanonicalContent::Text { text, .. } if text == "bcdef"
            )
    ));

    let tool_messages = vec![
        CanonicalMessage::user_text("question"),
        CanonicalMessage::assistant(
            "openai",
            ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![CanonicalContent::tool_call(
                "call_1",
                "bash",
                json!({"command": "pwd"}),
            )],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result("call_1", "bash", "ok", false),
    ];
    let tool_compiled = compile_provider_context(
        tool_messages,
        &ProviderContextPolicy {
            max_messages: Some(1),
            max_text_bytes: None,
        },
    );
    assert_eq!(tool_compiled.dropped_messages, 1);
    assert_eq!(tool_compiled.messages.len(), 2);
    assert!(matches!(
        tool_compiled.messages[0],
        CanonicalMessage::Assistant { .. }
    ));
    assert!(matches!(
        tool_compiled.messages[1],
        CanonicalMessage::ToolResult { .. }
    ));
}

#[tokio::test]
async fn local_offline_provider_is_deterministic_and_capability_limited() {
    let client = LocalOfflineProviderClient::new("local_offline", "echo");
    let mut request = ProviderRequest::new(
        ProviderApi::Other("local_offline".to_string()),
        "local_offline",
        "echo",
    );
    request.messages = vec![CanonicalMessage::user_text("hello local")];

    let response = client.complete(&request).await.unwrap();
    assert_eq!(
        response.content,
        vec![CanonicalContent::text("local:hello local")]
    );

    let stream_err = client.stream(&request).await.unwrap_err();
    assert!(matches!(
        stream_err,
        ProviderError::UnsupportedCapability {
            capability: "streaming",
            ..
        }
    ));

    request.tools = vec![ToolDefinition::new("bash", "run", json!({"type":"object"}))];
    let tool_err = client.complete(&request).await.unwrap_err();
    assert!(matches!(
        tool_err,
        ProviderError::UnsupportedCapability {
            capability: "tools",
            ..
        }
    ));
}

#[test]
fn openai_responses_replays_reasoning_and_function_state() {
    let mut request = base_request(ProviderApi::OpenAIResponses);
    request.thinking = Some(ThinkingConfig::Effort {
        effort: ThinkingEffort::Medium,
    });
    request.messages = vec![
        CanonicalMessage::assistant(
            "openai",
            ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![
                CanonicalContent::Thinking {
                    text: "summary".to_string(),
                    provider: ThinkingProvider::OpenAIResponses,
                    metadata: ThinkingMetadata::OpenAIResponses {
                        item_id: Some("rs_1".to_string()),
                        output_index: Some(0),
                        summary_index: 0,
                        encrypted_content: Some("enc".to_string()),
                    },
                },
                CanonicalContent::tool_call("call_1|fc_1", "bash", json!({"command":"echo hi"})),
            ],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result("call_1|fc_1", "bash", "hi", false),
    ];

    let body = OpenAIResponsesAdapter::default()
        .build_request_body(&request)
        .unwrap();

    assert_eq!(body["reasoning"]["effort"], "medium");
    assert_eq!(body["include"][0], "reasoning.encrypted_content");
    assert_eq!(body["input"][0]["type"], "reasoning");
    assert_eq!(body["input"][0]["id"], "rs_1");
    assert_eq!(body["input"][0]["encrypted_content"], "enc");
    assert_eq!(body["input"][1]["type"], "function_call");
    assert_eq!(body["input"][1]["call_id"], "call_1");
    assert_eq!(body["input"][1]["id"], "fc_1");
    assert_eq!(body["input"][2]["type"], "function_call_output");
    assert_eq!(body["input"][2]["call_id"], "call_1");
}

#[test]
fn openai_chat_fans_tool_results_into_tool_messages() {
    let mut request = base_request(ProviderApi::OpenAIChatCompletions);
    request.messages = vec![
        CanonicalMessage::assistant(
            "openai",
            ProviderApi::OpenAIChatCompletions,
            "gpt-test",
            vec![CanonicalContent::tool_call(
                "call_1",
                "bash",
                json!({"command":"pwd"}),
            )],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result("call_1", "bash", "/tmp", false),
    ];

    let body = OpenAIChatCompletionsAdapter
        .build_request_body(&request)
        .unwrap();

    assert_eq!(body["messages"][0]["role"], "system");
    assert_eq!(body["messages"][1]["role"], "assistant");
    assert_eq!(
        body["messages"][1]["tool_calls"][0]["function"]["arguments"],
        "{\"command\":\"pwd\"}"
    );
    assert_eq!(body["messages"][2]["role"], "tool");
    assert_eq!(body["messages"][2]["tool_call_id"], "call_1");
}

#[test]
fn openai_chat_drops_thinking_history_from_wire_replay() {
    let mut request = base_request(ProviderApi::OpenAIChatCompletions);
    request.provider = "provider".to_string();
    request.thinking = None;
    request.messages = vec![
        CanonicalMessage::assistant(
            "provider",
            ProviderApi::OpenAIChatCompletions,
            "chat-test",
            vec![
                CanonicalContent::Thinking {
                    text: "plan first".to_string(),
                    provider: ThinkingProvider::OpenAICompatible,
                    metadata: ThinkingMetadata::None,
                },
                CanonicalContent::text("answer"),
            ],
            CanonicalStopReason::EndTurn,
        ),
        CanonicalMessage::assistant(
            "provider",
            ProviderApi::OpenAIChatCompletions,
            "chat-test",
            vec![CanonicalContent::Thinking {
                text: "thinking only".to_string(),
                provider: ThinkingProvider::OpenAICompatible,
                metadata: ThinkingMetadata::None,
            }],
            CanonicalStopReason::EndTurn,
        ),
    ];

    let body = OpenAIChatCompletionsAdapter
        .build_request_body(&request)
        .unwrap();

    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1]["role"], "assistant");
    assert_eq!(messages[1]["content"], "answer");
    assert!(body.get("thinking").is_none());
    assert!(body.get("reasoning_effort").is_none());
}

#[test]
fn anthropic_preserves_cache_and_thinking_signature() {
    let mut request = base_request(ProviderApi::AnthropicMessages);
    request.system = vec![SystemBlock::cached("base")];
    request.thinking = Some(ThinkingConfig::Budget {
        budget_tokens: 2048,
    });
    request.messages = vec![CanonicalMessage::assistant(
        "anthropic",
        ProviderApi::AnthropicMessages,
        "claude-test",
        vec![
            CanonicalContent::Thinking {
                text: "reason".to_string(),
                provider: ThinkingProvider::Anthropic,
                metadata: ThinkingMetadata::Anthropic {
                    signature: Some("sig".to_string()),
                },
            },
            CanonicalContent::cached_text("answer", CacheControl::ephemeral()),
        ],
        CanonicalStopReason::EndTurn,
    )];

    let body = AnthropicMessagesAdapter
        .build_request_body(&request)
        .unwrap();

    assert_eq!(body["system"][0]["cache_control"]["type"], "ephemeral");
    assert_eq!(body["thinking"]["type"], "enabled");
    assert_eq!(body["thinking"]["budget_tokens"], 2048);
    assert_eq!(body["messages"][0]["content"][0]["type"], "thinking");
    assert_eq!(body["messages"][0]["content"][0]["signature"], "sig");
    assert_eq!(
        body["messages"][0]["content"][1]["cache_control"]["type"],
        "ephemeral"
    );
}

#[test]
fn anthropic_bedrock_request_uses_invoke_model_body_shape() {
    let mut request = base_request(ProviderApi::AnthropicMessages);
    request.model = "anthropic.claude-sonnet-4-5-20250929-v1:0".to_string();
    request.messages = vec![CanonicalMessage::user_text("hello")];

    let body = AnthropicBedrockMessagesAdapter
        .build_request_body(&request)
        .unwrap();

    assert_eq!(body["anthropic_version"], "bedrock-2023-05-31");
    assert!(body.get("model").is_none());
    assert_eq!(body["messages"][0]["role"], "user");
    assert_eq!(body["tools"][0]["name"], "bash");
}

#[test]
fn anthropic_bedrock_streaming_fails_closed_until_eventstream_decode_lands() {
    let mut request = base_request(ProviderApi::AnthropicMessages);
    request.messages = vec![CanonicalMessage::user_text("hello")];

    let err = AnthropicBedrockMessagesAdapter
        .build_stream_request_body(&request)
        .unwrap_err();

    assert!(matches!(
        err,
        ProviderError::UnsupportedCapability {
            capability: "streaming",
            ..
        }
    ));
}

#[test]
fn tool_call_result_pairing_survives_provider_switch_transforms() {
    let canonical_messages = vec![
        CanonicalMessage::assistant(
            "openai",
            ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![CanonicalContent::tool_call(
                "call_from_openai|fc_item_with_extra_provider_state",
                "bash",
                json!({"command":"pwd"}),
            )],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result(
            "call_from_openai|fc_item_with_extra_provider_state",
            "bash",
            "/tmp",
            false,
        ),
    ];

    let mut anthropic_request = base_request(ProviderApi::AnthropicMessages);
    anthropic_request.messages = canonical_messages.clone();
    let anthropic_body = AnthropicMessagesAdapter
        .build_request_body(&anthropic_request)
        .unwrap();
    let anthropic_tool_use_id = anthropic_body["messages"][0]["content"][0]["id"]
        .as_str()
        .unwrap();
    let anthropic_tool_result_id = anthropic_body["messages"][1]["content"][0]["tool_use_id"]
        .as_str()
        .unwrap();
    assert_eq!(anthropic_tool_use_id, anthropic_tool_result_id);
    assert!(anthropic_tool_use_id.len() <= 64);
    assert_eq!(
        anthropic_body["messages"][0]["content"][0]["input"]["command"],
        "pwd"
    );
    assert_eq!(
        anthropic_body["messages"][1]["content"][0]["content"],
        "/tmp"
    );

    let mut responses_request = base_request(ProviderApi::OpenAIResponses);
    responses_request.messages = vec![
        CanonicalMessage::assistant(
            "anthropic",
            ProviderApi::AnthropicMessages,
            "claude-test",
            vec![CanonicalContent::tool_call(
                "toolu_01abcdef",
                "bash",
                json!({"command":"ls"}),
            )],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result("toolu_01abcdef", "bash", "file.txt", false),
    ];
    let responses_body = OpenAIResponsesAdapter::default()
        .build_request_body(&responses_request)
        .unwrap();
    assert_eq!(responses_body["input"][0]["call_id"], "toolu_01abcdef");
    assert_eq!(responses_body["input"][1]["call_id"], "toolu_01abcdef");
    assert_eq!(responses_body["input"][1]["output"], "file.txt");
}

#[test]
fn decodes_provider_responses_to_canonical_content() {
    let openai = OpenAIResponsesAdapter::default()
            .decode_response_body(&json!({
                "status": "completed",
                "output": [
                    {"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"why"}],"encrypted_content":"enc"},
                    {"id":"fc_1","type":"function_call","call_id":"call_1","name":"bash","arguments":"{\"command\":\"ls\"}"},
                    {"type":"message","content":[{"type":"output_text","text":"done"}]}
                ],
                "usage": {"input_tokens": 10, "output_tokens": 3}
            }))
            .unwrap();
    assert_eq!(openai.stop_reason, CanonicalStopReason::ToolUse);
    assert_eq!(openai.content.len(), 3);

    let chat = OpenAIChatCompletionsAdapter
            .decode_response_body(&json!({
                "choices": [{
                    "finish_reason": "tool_calls",
                    "message": {
                        "tool_calls": [{"id":"call_1","function":{"name":"bash","arguments":"{\"command\":\"pwd\"}"}}]
                    }
                }],
                "usage": {"prompt_tokens": 4, "completion_tokens": 5}
            }))
            .unwrap();
    assert_eq!(chat.stop_reason, CanonicalStopReason::ToolUse);

    let chat_thinking = OpenAIChatCompletionsAdapter
        .decode_response_body(&json!({
            "choices": [{
                "finish_reason": "stop",
                "message": {
                    "reasoning_content": "plan first",
                    "content": "answer"
                }
            }],
            "usage": {"prompt_tokens": 4, "completion_tokens": 5}
        }))
        .unwrap();
    assert_eq!(chat_thinking.content.len(), 2);
    assert!(matches!(
        &chat_thinking.content[0],
        CanonicalContent::Thinking {
            text,
            provider: ThinkingProvider::OpenAICompatible,
            metadata: ThinkingMetadata::None,
        } if text == "plan first"
    ));
    assert_eq!(chat_thinking.content[1], CanonicalContent::text("answer"));

    let chat_thinking_only = OpenAIChatCompletionsAdapter
        .decode_response_body(&json!({
            "choices": [{
                "finish_reason": "stop",
                "message": {
                    "reasoning_content": "tool plan"
                }
            }]
        }))
        .unwrap();
    assert_eq!(chat_thinking_only.content.len(), 1);
    assert!(matches!(
        &chat_thinking_only.content[0],
        CanonicalContent::Thinking {
            text,
            provider: ThinkingProvider::OpenAICompatible,
            metadata: ThinkingMetadata::None,
        } if text == "tool plan"
    ));

    let anthropic = AnthropicMessagesAdapter
        .decode_response_body(&json!({
            "stop_reason": "end_turn",
            "content": [{"type":"thinking","thinking":"why","signature":"sig"}],
            "usage": {"input_tokens": 7, "output_tokens": 8, "cache_read_input_tokens": 9}
        }))
        .unwrap();
    assert_eq!(anthropic.usage.cache_read_input_tokens, 9);
    assert!(matches!(
        anthropic.content[0],
        CanonicalContent::Thinking {
            provider: ThinkingProvider::Anthropic,
            ..
        }
    ));
}

#[test]
fn parses_openai_responses_sse_text_and_done() {
    let sse = concat!(
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"COOL\"}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"_OK\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}}\n\n",
    );

    let events = OpenAIResponsesAdapter::default()
        .decode_sse_events(sse)
        .unwrap();

    assert_eq!(
        events,
        vec![
            ProviderStreamEvent::TextDelta {
                text: "COOL".to_string()
            },
            ProviderStreamEvent::TextDelta {
                text: "_OK".to_string()
            },
            ProviderStreamEvent::Usage {
                usage: CanonicalUsage {
                    input_tokens: 1,
                    output_tokens: 2,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                }
            },
            ProviderStreamEvent::Done {
                stop_reason: CanonicalStopReason::EndTurn
            },
        ]
    );
}

#[test]
fn parses_openai_responses_sse_tool_call_delta_and_completed_item() {
    let sse = concat!(
        "event: response.function_call_arguments.delta\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"call_id\":\"call_1\",\"delta\":\"{\\\"command\\\"\"}\n\n",
        "event: response.function_call_arguments.delta\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"call_id\":\"call_1\",\"delta\":\":\\\"pwd\\\"}\"}\n\n",
        "event: response.output_item.done\n",
        "data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"bash\",\"arguments\":\"{\\\"command\\\":\\\"pwd\\\"}\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let events = OpenAIResponsesAdapter::default()
        .decode_sse_events(sse)
        .unwrap();

    assert!(matches!(
        &events[0],
        ProviderStreamEvent::ToolCallDelta {
            id,
            arguments_delta,
            ..
        } if id == "call_1|fc_1" && arguments_delta == "{\"command\""
    ));
    assert!(matches!(
        &events[2],
        ProviderStreamEvent::Content {
            content: CanonicalContent::ToolCall { id, name, arguments }
        } if id == "call_1|fc_1" && name == "bash" && arguments["command"] == "pwd"
    ));
    assert!(matches!(
        events.last(),
        Some(ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::EndTurn
        })
    ));
}

#[test]
fn parses_openai_chat_sse_text_tool_usage_and_done() {
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"bash\",\"arguments\":\"{\\\"command\\\"\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"pwd\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\n",
        "data: [DONE]\n\n",
    );

    let events = OpenAIChatCompletionsAdapter.decode_sse_events(sse).unwrap();

    assert!(matches!(
        &events[0],
        ProviderStreamEvent::TextDelta { text } if text == "hi"
    ));
    assert!(matches!(
        &events[1],
        ProviderStreamEvent::ToolCallDelta {
            id,
            name: Some(name),
            arguments_delta,
        } if id == "call_1" && name == "bash" && arguments_delta == "{\"command\""
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        ProviderStreamEvent::Usage { usage }
            if usage.input_tokens == 2 && usage.output_tokens == 3
    )));
    assert!(matches!(
        events.last(),
        Some(ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::ToolUse
        })
    ));
}

#[test]
fn parses_openai_chat_sse_reasoning_content_deltas_in_order() {
    let sse = concat!(
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":null},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"plan \"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"check\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\" same\",\"content\":\" there\"},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    let events = OpenAIChatCompletionsAdapter.decode_sse_events(sse).unwrap();

    assert_eq!(
        events,
        vec![
            ProviderStreamEvent::ThinkingDelta {
                text: "plan ".to_string()
            },
            ProviderStreamEvent::TextDelta {
                text: "hi".to_string()
            },
            ProviderStreamEvent::ThinkingDelta {
                text: "check".to_string()
            },
            ProviderStreamEvent::ThinkingDelta {
                text: " same".to_string()
            },
            ProviderStreamEvent::TextDelta {
                text: " there".to_string()
            },
            ProviderStreamEvent::Done {
                stop_reason: CanonicalStopReason::EndTurn
            },
        ]
    );
}

#[test]
fn parses_anthropic_sse_text_usage_and_done() {
    let sse = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"COOL\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"_OK\"}}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    let events = AnthropicMessagesAdapter.decode_sse_events(sse).unwrap();

    assert_eq!(
        events,
        vec![
            ProviderStreamEvent::Usage {
                usage: CanonicalUsage {
                    input_tokens: 3,
                    output_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                }
            },
            ProviderStreamEvent::TextDelta {
                text: "COOL".to_string()
            },
            ProviderStreamEvent::TextDelta {
                text: "_OK".to_string()
            },
            ProviderStreamEvent::Usage {
                usage: CanonicalUsage {
                    input_tokens: 0,
                    output_tokens: 4,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                }
            },
            ProviderStreamEvent::Done {
                stop_reason: CanonicalStopReason::EndTurn
            },
        ]
    );
}

#[test]
fn parses_anthropic_sse_tool_use_delta() {
    let sse = concat!(
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"bash\",\"input\":{}}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"command\\\"\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\":\\\"pwd\\\"}\"}}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    let events = AnthropicMessagesAdapter.decode_sse_events(sse).unwrap();

    assert!(matches!(
        &events[0],
        ProviderStreamEvent::ToolCallDelta {
            id,
            name: Some(name),
            arguments_delta,
        } if id == "toolu_1" && name == "bash" && arguments_delta.is_empty()
    ));
    assert!(matches!(
        &events[1],
        ProviderStreamEvent::ToolCallDelta {
            id,
            arguments_delta,
            ..
        } if id == "toolu_1" && arguments_delta == "{\"command\""
    ));
    assert!(matches!(
        events.last(),
        Some(ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::ToolUse
        })
    ));
}

#[test]
fn malformed_tool_arguments_are_rejected_at_wire_edge() {
    let chat = OpenAIChatCompletionsAdapter
            .decode_response_body(&json!({
                "choices": [{
                    "finish_reason": "tool_calls",
                    "message": {
                        "tool_calls": [{"id":"call_1","function":{"name":"bash","arguments":"not-json"}}]
                    }
                }]
            }))
            .unwrap_err();
    assert!(
        matches!(chat, ProviderError::Decode(message) if message.contains("invalid chat tool call arguments"))
    );

    let responses = OpenAIResponsesAdapter::default()
        .decode_response_body(&json!({
            "status": "completed",
            "output": [{
                "id":"fc_1",
                "type":"function_call",
                "call_id":"call_1",
                "name":"bash",
                "arguments":"not-json"
            }]
        }))
        .unwrap_err();
    assert!(
        matches!(responses, ProviderError::Decode(message) if message.contains("invalid OpenAI Responses function arguments"))
    );
}
