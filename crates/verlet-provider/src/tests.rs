use crate::ProviderClient as _;
use crate::ProviderWireAdapter as _;
use base64::Engine as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

fn base_request(api: verlet_history::ProviderApi) -> crate::ProviderRequest {
    let mut request = crate::ProviderRequest::new(api, "provider", "model-test");
    request.system = vec![crate::SystemBlock::text("base")];
    request.tools = vec![crate::ToolDefinition::new(
        "bash",
        "run command",
        serde_json::json!({"type":"object","properties":{"command":{"type":"string"}}}),
    )];
    request
}

#[test]
fn provider_endpoint_openai_helpers_accept_root_or_v1_base_urls() {
    assert_eq!(
        crate::ProviderEndpoint::openai_responses("https://api.example.test", "token").url,
        "https://api.example.test/v1/responses"
    );
    assert_eq!(
        crate::ProviderEndpoint::openai_responses("https://api.example.test/v1", "token").url,
        "https://api.example.test/v1/responses"
    );
    assert_eq!(
        crate::ProviderEndpoint::openai_chat_completions("https://api.example.invalid/v1", "token")
            .url,
        "https://api.example.invalid/v1/chat/completions"
    );
    assert_eq!(
        crate::ProviderEndpoint::anthropic_messages("https://api.anthropic.com", "token").url,
        "https://api.anthropic.com/v1/messages"
    );
    assert_eq!(
        crate::ProviderEndpoint::anthropic_messages(
            "https://proxy.example.test/anthropic/v1",
            "token"
        )
        .url,
        "https://proxy.example.test/anthropic/v1/messages"
    );
    assert_eq!(
        crate::ProviderEndpoint::anthropic_bedrock(
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
    let endpoint = crate::ProviderEndpoint::anthropic_bedrock(
        "us-east-1",
        "anthropic.claude-sonnet-4-5-20250929-v1:0",
        "akid",
        "secret",
        None,
    );
    let url = reqwest::Url::parse(&endpoint.url).unwrap();

    assert_eq!(
        crate::canonical_uri(&url),
        "/model/anthropic.claude-sonnet-4-5-20250929-v1%253A0/invoke"
    );
}

#[test]
fn provider_capabilities_are_explicit_and_queryable() {
    let responses = crate::OpenAIResponsesAdapter::default().capabilities();
    assert!(responses.supports_tools);
    assert!(responses.supports_streaming);
    assert!(responses.supports_reasoning);
    assert!(responses.supports_images);
    assert!(!responses.supports_cache_control);
    assert!(
        responses
            .supported_abi_projections
            .contains(&crate::ProviderAbiProjection::LlmTool)
    );

    let chat = crate::OpenAIChatCompletionsAdapter.capabilities();
    assert!(chat.supports_tools);
    assert!(chat.supports_streaming);
    assert!(chat.supports_reasoning);
    assert!(!chat.supports_images);

    let anthropic = crate::AnthropicMessagesAdapter.capabilities();
    assert!(anthropic.supports_cache_control);
    assert!(anthropic.supports_reasoning);
    assert!(anthropic.supports_images);

    let bedrock = crate::AnthropicBedrockMessagesAdapter.capabilities();
    assert!(bedrock.supports_cache_control);
    assert!(bedrock.supports_reasoning);
    assert!(bedrock.supports_images);
    assert!(bedrock.supports_streaming);

    let local = crate::LocalOfflineProviderClient::new("local_offline", "echo")
        .capabilities()
        .unwrap();
    assert_eq!(
        local.api,
        verlet_history::ProviderApi::Other("local_offline".to_string())
    );
    assert_eq!(
        local.supported_abi_projections,
        std::collections::BTreeSet::from([crate::ProviderAbiProjection::Text])
    );
    assert!(!local.supports_tools);
}

#[test]
fn openai_responses_budget_thinking_fails_closed() {
    let mut request = base_request(verlet_history::ProviderApi::OpenAIResponses);
    request.thinking = Some(crate::ThinkingConfig::Budget {
        budget_tokens: 1024,
    });
    for body in [
        crate::OpenAIResponsesAdapter::default().build_request_body(&request),
        crate::OpenAIResponsesAdapter::default().build_stream_request_body(&request),
    ] {
        assert!(matches!(
            body.unwrap_err(),
            crate::ProviderError::UnsupportedCapability {
                capability: "thinking_budget",
                ..
            }
        ));
    }

    request.thinking = Some(crate::ThinkingConfig::Effort {
        effort: crate::ThinkingEffort::High,
    });
    let body = crate::OpenAIResponsesAdapter::default()
        .build_request_body(&request)
        .unwrap();
    assert_eq!(
        body["reasoning"]["effort"],
        serde_json::json!(crate::ThinkingEffort::High.as_openai_wire())
    );

    request.thinking = None;
    let body = crate::OpenAIResponsesAdapter::default()
        .build_request_body(&request)
        .unwrap();
    assert!(body.get("reasoning").is_none());
}

#[test]
fn unsupported_provider_capabilities_fail_closed() {
    let mut cached = base_request(verlet_history::ProviderApi::OpenAIResponses);
    cached.system = vec![crate::SystemBlock::cached("base")];
    let err = crate::OpenAIResponsesAdapter::default()
        .build_request_body(&cached)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ProviderError::UnsupportedCapability {
            capability: "cache_control",
            ..
        }
    ));

    let mut image = base_request(verlet_history::ProviderApi::OpenAIChatCompletions);
    image.messages = vec![verlet_history::CanonicalMessage::User {
        content: vec![verlet_history::CanonicalContent::Image {
            data: "aW1hZ2U=".to_string(),
            mime_type: "image/png".to_string(),
        }],
        timestamp_ms: 0,
    }];
    let err = crate::OpenAIChatCompletionsAdapter
        .build_request_body(&image)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ProviderError::UnsupportedCapability {
            capability: "images",
            ..
        }
    ));
}

#[test]
fn openai_chat_thinking_request_mapping_covers_efforts_and_provider_gate() {
    for effort in [
        crate::ThinkingEffort::Low,
        crate::ThinkingEffort::Medium,
        crate::ThinkingEffort::High,
    ] {
        for (provider, zhipu_convention) in [("openai_compatible", true), ("provider", false)] {
            let mut request = base_request(verlet_history::ProviderApi::OpenAIChatCompletions);
            request.provider = provider.to_string();
            request.thinking = Some(crate::ThinkingConfig::Effort {
                effort: effort.clone(),
            });

            let complete_body = crate::OpenAIChatCompletionsAdapter
                .build_request_body(&request)
                .unwrap();
            let stream_body = crate::OpenAIChatCompletionsAdapter
                .build_stream_request_body(&request)
                .unwrap();

            for body in [complete_body, stream_body] {
                assert_eq!(body["reasoning_effort"], effort.as_openai_wire());
                if zhipu_convention {
                    assert_eq!(body["thinking"], serde_json::json!({"type": "enabled"}));
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
            crate::ThinkingConfig::Budget {
                budget_tokens: 1024,
            },
            "thinking_budget",
        ),
        (
            crate::ThinkingConfig::Effort {
                effort: crate::ThinkingEffort::XHigh,
            },
            "thinking_effort",
        ),
        (
            crate::ThinkingConfig::Effort {
                effort: crate::ThinkingEffort::Max,
            },
            "thinking_effort",
        ),
        (
            crate::ThinkingConfig::Effort {
                effort: crate::ThinkingEffort::Other("extreme".to_string()),
            },
            "thinking_effort",
        ),
    ] {
        let mut request = base_request(verlet_history::ProviderApi::OpenAIChatCompletions);
        request.thinking = Some(thinking);

        for body in [
            crate::OpenAIChatCompletionsAdapter.build_request_body(&request),
            crate::OpenAIChatCompletionsAdapter.build_stream_request_body(&request),
        ] {
            assert!(matches!(
                body.unwrap_err(),
                crate::ProviderError::UnsupportedCapability {
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
        (
            "openai_compatible",
            Some(serde_json::json!({"type": "disabled"})),
        ),
        ("zhipu", Some(serde_json::json!({"type": "disabled"}))),
        ("glm", Some(serde_json::json!({"type": "disabled"}))),
        ("provider", None),
    ] {
        let mut request = base_request(verlet_history::ProviderApi::OpenAIChatCompletions);
        request.provider = provider.to_string();
        request.thinking = Some(crate::ThinkingConfig::Disabled);

        let body = crate::OpenAIChatCompletionsAdapter
            .build_request_body(&request)
            .unwrap();
        assert_eq!(body.get("thinking"), expected_thinking.as_ref());
        assert!(body.get("reasoning_effort").is_none());
    }

    let mut request = base_request(verlet_history::ProviderApi::OpenAIChatCompletions);
    request.provider = "openai_compatible".to_string();
    request.thinking = None;
    let body = crate::OpenAIChatCompletionsAdapter
        .build_request_body(&request)
        .unwrap();
    assert!(body.get("thinking").is_none());
    assert!(body.get("reasoning_effort").is_none());
}

#[test]
fn provider_context_compilation_is_deterministic() {
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("old"),
        verlet_history::CanonicalMessage::assistant(
            "openai",
            verlet_history::ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![verlet_history::CanonicalContent::text("middle")],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
        verlet_history::CanonicalMessage::user_text("abcdef"),
    ];
    let policy = crate::ProviderContextPolicy {
        max_messages: Some(2),
        max_text_bytes: Some(5),
    };

    let first = crate::compile_provider_context(messages.clone(), &policy);
    let second = crate::compile_provider_context(messages, &policy);

    assert_eq!(first, second);
    assert_eq!(first.dropped_messages, 1);
    assert_eq!(first.retained_text_bytes, 5);
    assert_eq!(first.truncated_text_bytes, 7);
    assert!(matches!(
        &first.messages[1],
        verlet_history::CanonicalMessage::User { content, .. }
            if matches!(
                &content[0],
                verlet_history::CanonicalContent::Text { text, .. } if text == "bcdef"
            )
    ));

    let tool_messages = vec![
        verlet_history::CanonicalMessage::user_text("question"),
        verlet_history::CanonicalMessage::assistant(
            "openai",
            verlet_history::ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![verlet_history::CanonicalContent::tool_call(
                "call_1",
                "bash",
                serde_json::json!({"command": "pwd"}),
            )],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("call_1", "bash", "ok", false),
    ];
    let tool_compiled = crate::compile_provider_context(
        tool_messages,
        &crate::ProviderContextPolicy {
            max_messages: Some(1),
            max_text_bytes: None,
        },
    );
    assert_eq!(tool_compiled.dropped_messages, 1);
    assert_eq!(tool_compiled.messages.len(), 2);
    assert!(matches!(
        tool_compiled.messages[0],
        verlet_history::CanonicalMessage::Assistant { .. }
    ));
    assert!(matches!(
        tool_compiled.messages[1],
        verlet_history::CanonicalMessage::ToolResult { .. }
    ));
}

#[tokio::test]
async fn local_offline_provider_is_deterministic_and_capability_limited() {
    let client = crate::LocalOfflineProviderClient::new("local_offline", "echo");
    let mut request = crate::ProviderRequest::new(
        verlet_history::ProviderApi::Other("local_offline".to_string()),
        "local_offline",
        "echo",
    );
    request.messages = vec![verlet_history::CanonicalMessage::user_text("hello local")];

    let response = client.complete(&request).await.unwrap();
    assert_eq!(
        response.content,
        vec![verlet_history::CanonicalContent::text("local:hello local")]
    );

    let stream_err = client.stream(&request).await.unwrap_err();
    assert!(matches!(
        stream_err,
        crate::ProviderError::UnsupportedCapability {
            capability: "streaming",
            ..
        }
    ));

    request.tools = vec![crate::ToolDefinition::new(
        "bash",
        "run",
        serde_json::json!({"type":"object"}),
    )];
    let tool_err = client.complete(&request).await.unwrap_err();
    assert!(matches!(
        tool_err,
        crate::ProviderError::UnsupportedCapability {
            capability: "tools",
            ..
        }
    ));
}

#[test]
fn openai_responses_replays_reasoning_and_function_state() {
    let mut request = base_request(verlet_history::ProviderApi::OpenAIResponses);
    request.thinking = Some(crate::ThinkingConfig::Effort {
        effort: crate::ThinkingEffort::Medium,
    });
    request.messages = vec![
        verlet_history::CanonicalMessage::assistant(
            "openai",
            verlet_history::ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![
                verlet_history::CanonicalContent::Thinking {
                    text: "summary".to_string(),
                    provider: verlet_history::ThinkingProvider::OpenAIResponses,
                    metadata: verlet_history::ThinkingMetadata::OpenAIResponses {
                        item_id: Some("rs_1".to_string()),
                        output_index: Some(0),
                        summary_index: 0,
                        encrypted_content: Some("enc".to_string()),
                    },
                },
                verlet_history::CanonicalContent::tool_call(
                    "call_1|fc_1",
                    "bash",
                    serde_json::json!({"command":"echo hi"}),
                ),
            ],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("call_1|fc_1", "bash", "hi", false),
    ];

    let body = crate::OpenAIResponsesAdapter::default()
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
    let mut request = base_request(verlet_history::ProviderApi::OpenAIChatCompletions);
    request.messages = vec![
        verlet_history::CanonicalMessage::assistant(
            "openai",
            verlet_history::ProviderApi::OpenAIChatCompletions,
            "gpt-test",
            vec![verlet_history::CanonicalContent::tool_call(
                "call_1",
                "bash",
                serde_json::json!({"command":"pwd"}),
            )],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("call_1", "bash", "/tmp", false),
    ];

    let body = crate::OpenAIChatCompletionsAdapter
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
    let mut request = base_request(verlet_history::ProviderApi::OpenAIChatCompletions);
    request.provider = "provider".to_string();
    request.thinking = None;
    request.messages = vec![
        verlet_history::CanonicalMessage::assistant(
            "provider",
            verlet_history::ProviderApi::OpenAIChatCompletions,
            "chat-test",
            vec![
                verlet_history::CanonicalContent::Thinking {
                    text: "plan first".to_string(),
                    provider: verlet_history::ThinkingProvider::OpenAICompatible,
                    metadata: verlet_history::ThinkingMetadata::None,
                },
                verlet_history::CanonicalContent::text("answer"),
            ],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
        verlet_history::CanonicalMessage::assistant(
            "provider",
            verlet_history::ProviderApi::OpenAIChatCompletions,
            "chat-test",
            vec![verlet_history::CanonicalContent::Thinking {
                text: "thinking only".to_string(),
                provider: verlet_history::ThinkingProvider::OpenAICompatible,
                metadata: verlet_history::ThinkingMetadata::None,
            }],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
    ];

    let body = crate::OpenAIChatCompletionsAdapter
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
    let mut request = base_request(verlet_history::ProviderApi::AnthropicMessages);
    request.system = vec![crate::SystemBlock::cached("base")];
    request.thinking = Some(crate::ThinkingConfig::Budget {
        budget_tokens: 2048,
    });
    request.messages = vec![verlet_history::CanonicalMessage::assistant(
        "anthropic",
        verlet_history::ProviderApi::AnthropicMessages,
        "claude-test",
        vec![
            verlet_history::CanonicalContent::Thinking {
                text: "reason".to_string(),
                provider: verlet_history::ThinkingProvider::Anthropic,
                metadata: verlet_history::ThinkingMetadata::Anthropic {
                    signature: Some("sig".to_string()),
                },
            },
            verlet_history::CanonicalContent::cached_text(
                "answer",
                verlet_history::CacheControl::ephemeral(),
            ),
        ],
        verlet_history::CanonicalStopReason::EndTurn,
    )];

    let body = crate::AnthropicMessagesAdapter
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
    let mut request = base_request(verlet_history::ProviderApi::AnthropicMessages);
    request.model = "anthropic.claude-sonnet-4-5-20250929-v1:0".to_string();
    request.messages = vec![verlet_history::CanonicalMessage::user_text("hello")];

    let body = crate::AnthropicBedrockMessagesAdapter
        .build_request_body(&request)
        .unwrap();

    assert_eq!(body["anthropic_version"], "bedrock-2023-05-31");
    assert!(body.get("model").is_none());
    assert_eq!(body["messages"][0]["role"], "user");
    assert_eq!(body["tools"][0]["name"], "bash");
}

#[test]
fn anthropic_bedrock_streaming_uses_response_stream_endpoint_and_body_shape() {
    let mut request = base_request(verlet_history::ProviderApi::AnthropicMessages);
    request.messages = vec![verlet_history::CanonicalMessage::user_text("hello")];

    let body = crate::AnthropicBedrockMessagesAdapter
        .build_stream_request_body(&request)
        .unwrap();

    assert_eq!(body["anthropic_version"], "bedrock-2023-05-31");
    assert!(body.get("model").is_none());
    assert!(body.get("stream").is_none());
    assert_eq!(
        crate::AnthropicBedrockMessagesAdapter.stream_endpoint_url(
            "https://bedrock-runtime.us-east-1.amazonaws.com/model/claude/invoke"
        ),
        "https://bedrock-runtime.us-east-1.amazonaws.com/model/claude/invoke-with-response-stream"
    );
}

const BEDROCK_GOLDEN_MULTI_FRAME_HEX: &str = concat!(
    "000000c50000004ba5d920da0d3a6d6573736167652d747970650700056576656e740b3a6576656e",
    "742d747970650700056368756e6b0d3a636f6e74656e742d747970650700106170706c6963617469",
    "6f6e2f6a736f6e7b226368756e6b223a7b226279746573223a2265794a306558426c496a6f696257",
    "567a6332466e5a56397a6447467964434973496d316c63334e685a3255694f6e736964584e685a32",
    "55694f6e73696157357764585266644739725a57357a496a6f7a66583139227d7d4223b8c0000000",
    "e10000004b9198a91e0d3a6d6573736167652d747970650700056576656e740b3a6576656e742d74",
    "7970650700056368756e6b0d3a636f6e74656e742d747970650700106170706c69636174696f6e2f",
    "6a736f6e7b226368756e6b223a7b226279746573223a2265794a306558426c496a6f695932397564",
    "475675644639696247396a6131396b5a57783059534973496d6c755a475634496a6f774c434a6b5a",
    "5778305953493665794a306558426c496a6f69644756346446396b5a57783059534973496e526c65",
    "4851694f694a445430394d496e3139227d7d4536c8a4"
);

#[test]
fn aws_eventstream_decoder_accepts_golden_multi_frame_fixture() {
    let bytes = hex::decode(BEDROCK_GOLDEN_MULTI_FRAME_HEX).unwrap();
    let frames = crate::decode_aws_eventstream_frames(&bytes).unwrap();

    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[0].headers.get(":event-type"),
        Some(&crate::AwsEventStreamHeaderValue::String(
            "chunk".to_string()
        ))
    );
    assert!(decoded_bedrock_chunk_payload(&frames[0].payload).contains("message_start"));
    assert!(decoded_bedrock_chunk_payload(&frames[1].payload).contains("content_block_delta"));
}

#[test]
fn aws_eventstream_decoder_handles_split_read_boundary() {
    let bytes = hex::decode(BEDROCK_GOLDEN_MULTI_FRAME_HEX).unwrap();
    let mut decoder = crate::AwsEventStreamDecoder::new();

    assert!(decoder.push(&bytes[..31]).unwrap().is_empty());
    let frames = decoder.push(&bytes[31..]).unwrap();
    assert_eq!(frames.len(), 2);
    assert!(decoder.finish().unwrap().is_empty());
}

#[test]
fn aws_eventstream_decoder_rejects_corrupt_crc() {
    let mut bytes = hex::decode(BEDROCK_GOLDEN_MULTI_FRAME_HEX).unwrap();
    let last = bytes.last_mut().unwrap();
    *last ^= 0xff;

    let err = crate::decode_aws_eventstream_frames(&bytes).unwrap_err();
    assert!(matches!(
        err,
        crate::ProviderError::Decode(message) if message.contains("message CRC mismatch")
    ));
}

#[test]
fn aws_eventstream_decoder_rejects_truncated_prelude() {
    let mut decoder = crate::AwsEventStreamDecoder::new();
    assert!(decoder.push(&[0, 0, 0, 16, 0]).unwrap().is_empty());

    let err = decoder.finish().unwrap_err();
    assert!(matches!(
        err,
        crate::ProviderError::Decode(message) if message.contains("truncated AWS eventstream prelude")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn anthropic_bedrock_http_stream_decodes_events_and_signs_stream_endpoint() {
    let eventstream_body = bedrock_eventstream_body(ANTHROPIC_STREAM_EVENT_JSONS);
    let (base_url, request_rx) =
        serve_fake_http_response(eventstream_body.clone(), Some(eventstream_body.len())).await;
    let endpoint = crate::ProviderEndpoint::anthropic_bedrock_with_base_url(
        base_url,
        "us-east-1",
        "anthropic.claude-test-v1:0",
        "AKIA_TEST",
        "secret",
        Some("session-token".to_string()),
    );
    let client = crate::ProviderHttpClient::with_http(
        reqwest::Client::builder()
            .timeout(tokio::time::Duration::from_secs(30))
            .build()
            .unwrap(),
        endpoint,
        std::sync::Arc::new(crate::AnthropicBedrockMessagesAdapter),
    );
    let mut request = crate::ProviderRequest::new(
        verlet_history::ProviderApi::AnthropicMessages,
        "anthropic_bedrock",
        "anthropic.claude-test-v1:0",
    );
    request.max_tokens = 32;
    request.messages = vec![verlet_history::CanonicalMessage::user_text("hello")];

    let events = client.stream(&request).await.unwrap();
    let parity_events = crate::AnthropicMessagesAdapter
        .decode_sse_events(ANTHROPIC_STREAM_SSE)
        .unwrap();
    assert_eq!(events, parity_events);
    assert_eq!(stream_text(&events), "COOL_OK");

    let terminal = crate::AnthropicBedrockMessagesAdapter
        .decode_response_body(&serde_json::json!({
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": "COOL_OK"}],
            "usage": {"input_tokens": 3, "output_tokens": 4}
        }))
        .unwrap();
    assert_eq!(stream_usage(&events), terminal.usage);
    assert_eq!(stream_stop_reason(&events), terminal.stop_reason);
    assert_eq!(stream_text(&events), test_content_text(&terminal.content));

    let raw_request = request_rx.await.unwrap();
    assert!(raw_request.starts_with(
        "POST /model/anthropic.claude-test-v1%3A0/invoke-with-response-stream HTTP/1.1"
    ));
    let raw_request_lower = raw_request.to_ascii_lowercase();
    assert!(raw_request_lower.contains("authorization: aws4-hmac-sha256"));
    assert!(raw_request_lower.contains(
        "signedheaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token;x-amzn-bedrock-accept"
    ));
    assert!(raw_request_lower.contains("x-amz-content-sha256:"));
    assert!(raw_request_lower.contains("x-amz-security-token: session-token"));
    assert!(raw_request_lower.contains("x-amzn-bedrock-accept: application/json"));
}

#[test]
fn anthropic_bedrock_eventstream_decodes_raw_chunk_payloads() {
    let body = bedrock_raw_eventstream_body(ANTHROPIC_STREAM_EVENT_JSONS);
    let events = crate::AnthropicBedrockMessagesAdapter
        .decode_stream_response(&body)
        .unwrap();
    let parity_events = crate::AnthropicMessagesAdapter
        .decode_sse_events(ANTHROPIC_STREAM_SSE)
        .unwrap();

    assert_eq!(events, parity_events);
}

#[test]
fn anthropic_bedrock_eventstream_decodes_top_level_bytes_payloads() {
    let body = bedrock_top_level_bytes_eventstream_body(ANTHROPIC_STREAM_EVENT_JSONS);
    let events = crate::AnthropicBedrockMessagesAdapter
        .decode_stream_response(&body)
        .unwrap();
    let parity_events = crate::AnthropicMessagesAdapter
        .decode_sse_events(ANTHROPIC_STREAM_SSE)
        .unwrap();

    assert_eq!(events, parity_events);
}

#[tokio::test(flavor = "current_thread")]
async fn anthropic_bedrock_mid_stream_disconnect_returns_terminal_error() {
    let eventstream_body = bedrock_eventstream_body(ANTHROPIC_STREAM_EVENT_JSONS);
    let partial = eventstream_body[..25].to_vec();
    let (base_url, _request_rx) = serve_fake_http_response(partial, None).await;
    let endpoint = crate::ProviderEndpoint::anthropic_bedrock_with_base_url(
        base_url,
        "us-east-1",
        "anthropic.claude-test-v1:0",
        "AKIA_TEST",
        "secret",
        None,
    );
    let client = crate::ProviderHttpClient::with_http(
        reqwest::Client::builder()
            .timeout(tokio::time::Duration::from_secs(30))
            .build()
            .unwrap(),
        endpoint,
        std::sync::Arc::new(crate::AnthropicBedrockMessagesAdapter),
    );
    let mut request = crate::ProviderRequest::new(
        verlet_history::ProviderApi::AnthropicMessages,
        "anthropic_bedrock",
        "anthropic.claude-test-v1:0",
    );
    request.messages = vec![verlet_history::CanonicalMessage::user_text("hello")];

    let err = client.stream(&request).await.unwrap_err();
    assert!(matches!(
        err,
        crate::ProviderError::Decode(message) if message.contains("truncated AWS eventstream frame")
    ));
}

#[test]
fn tool_call_result_pairing_survives_provider_switch_transforms() {
    let canonical_messages = vec![
        verlet_history::CanonicalMessage::assistant(
            "openai",
            verlet_history::ProviderApi::OpenAIResponses,
            "gpt-test",
            vec![verlet_history::CanonicalContent::tool_call(
                "call_from_openai|fc_item_with_extra_provider_state",
                "bash",
                serde_json::json!({"command":"pwd"}),
            )],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result(
            "call_from_openai|fc_item_with_extra_provider_state",
            "bash",
            "/tmp",
            false,
        ),
    ];

    let mut anthropic_request = base_request(verlet_history::ProviderApi::AnthropicMessages);
    anthropic_request.messages = canonical_messages.clone();
    let anthropic_body = crate::AnthropicMessagesAdapter
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

    let mut responses_request = base_request(verlet_history::ProviderApi::OpenAIResponses);
    responses_request.messages = vec![
        verlet_history::CanonicalMessage::assistant(
            "anthropic",
            verlet_history::ProviderApi::AnthropicMessages,
            "claude-test",
            vec![verlet_history::CanonicalContent::tool_call(
                "toolu_01abcdef",
                "bash",
                serde_json::json!({"command":"ls"}),
            )],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("toolu_01abcdef", "bash", "file.txt", false),
    ];
    let responses_body = crate::OpenAIResponsesAdapter::default()
        .build_request_body(&responses_request)
        .unwrap();
    assert_eq!(responses_body["input"][0]["call_id"], "toolu_01abcdef");
    assert_eq!(responses_body["input"][1]["call_id"], "toolu_01abcdef");
    assert_eq!(responses_body["input"][1]["output"], "file.txt");
}

#[test]
fn decodes_provider_responses_to_canonical_content() {
    let openai = crate::OpenAIResponsesAdapter::default()
            .decode_response_body(&serde_json::json!({
                "status": "completed",
                "output": [
                    {"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"why"}],"encrypted_content":"enc"},
                    {"id":"fc_1","type":"function_call","call_id":"call_1","name":"bash","arguments":"{\"command\":\"ls\"}"},
                    {"type":"message","content":[{"type":"output_text","text":"done"}]}
                ],
                "usage": {"input_tokens": 10, "output_tokens": 3}
            }))
            .unwrap();
    assert_eq!(
        openai.stop_reason,
        verlet_history::CanonicalStopReason::ToolUse
    );
    assert_eq!(openai.content.len(), 3);

    let chat = crate::OpenAIChatCompletionsAdapter
            .decode_response_body(&serde_json::json!({
                "choices": [{
                    "finish_reason": "tool_calls",
                    "message": {
                        "tool_calls": [{"id":"call_1","function":{"name":"bash","arguments":"{\"command\":\"pwd\"}"}}]
                    }
                }],
                "usage": {"prompt_tokens": 4, "completion_tokens": 5}
            }))
            .unwrap();
    assert_eq!(
        chat.stop_reason,
        verlet_history::CanonicalStopReason::ToolUse
    );

    let chat_thinking = crate::OpenAIChatCompletionsAdapter
        .decode_response_body(&serde_json::json!({
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
        verlet_history::CanonicalContent::Thinking {
            text,
            provider: verlet_history::ThinkingProvider::OpenAICompatible,
            metadata: verlet_history::ThinkingMetadata::None,
        } if text == "plan first"
    ));
    assert_eq!(
        chat_thinking.content[1],
        verlet_history::CanonicalContent::text("answer")
    );

    let chat_thinking_only = crate::OpenAIChatCompletionsAdapter
        .decode_response_body(&serde_json::json!({
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
        verlet_history::CanonicalContent::Thinking {
            text,
            provider: verlet_history::ThinkingProvider::OpenAICompatible,
            metadata: verlet_history::ThinkingMetadata::None,
        } if text == "tool plan"
    ));

    let anthropic = crate::AnthropicMessagesAdapter
        .decode_response_body(&serde_json::json!({
            "stop_reason": "end_turn",
            "content": [{"type":"thinking","thinking":"why","signature":"sig"}],
            "usage": {"input_tokens": 7, "output_tokens": 8, "cache_read_input_tokens": 9}
        }))
        .unwrap();
    assert_eq!(anthropic.usage.cache_read_input_tokens, 9);
    assert!(matches!(
        anthropic.content[0],
        verlet_history::CanonicalContent::Thinking {
            provider: verlet_history::ThinkingProvider::Anthropic,
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

    let events = crate::OpenAIResponsesAdapter::default()
        .decode_sse_events(sse)
        .unwrap();

    assert_eq!(
        events,
        vec![
            crate::ProviderStreamEvent::TextDelta {
                text: "COOL".to_string()
            },
            crate::ProviderStreamEvent::TextDelta {
                text: "_OK".to_string()
            },
            crate::ProviderStreamEvent::Usage {
                usage: verlet_history::CanonicalUsage {
                    input_tokens: 1,
                    output_tokens: 2,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                }
            },
            crate::ProviderStreamEvent::Done {
                stop_reason: verlet_history::CanonicalStopReason::EndTurn
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

    let events = crate::OpenAIResponsesAdapter::default()
        .decode_sse_events(sse)
        .unwrap();

    assert!(matches!(
        &events[0],
        crate::ProviderStreamEvent::ToolCallDelta {
            id,
            arguments_delta,
            ..
        } if id == "call_1|fc_1" && arguments_delta == "{\"command\""
    ));
    assert!(matches!(
        &events[2],
        crate::ProviderStreamEvent::Content {
            content: verlet_history::CanonicalContent::ToolCall { id, name, arguments }
        } if id == "call_1|fc_1" && name == "bash" && arguments["command"] == "pwd"
    ));
    assert!(matches!(
        events.last(),
        Some(crate::ProviderStreamEvent::Done {
            stop_reason: verlet_history::CanonicalStopReason::EndTurn
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

    let events = crate::OpenAIChatCompletionsAdapter
        .decode_sse_events(sse)
        .unwrap();

    assert!(matches!(
        &events[0],
        crate::ProviderStreamEvent::TextDelta { text } if text == "hi"
    ));
    assert!(matches!(
        &events[1],
        crate::ProviderStreamEvent::ToolCallDelta {
            id,
            name: Some(name),
            arguments_delta,
        } if id == "call_1" && name == "bash" && arguments_delta == "{\"command\""
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        crate::ProviderStreamEvent::Usage { usage }
            if usage.input_tokens == 2 && usage.output_tokens == 3
    )));
    assert!(matches!(
        events.last(),
        Some(crate::ProviderStreamEvent::Done {
            stop_reason: verlet_history::CanonicalStopReason::ToolUse
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

    let events = crate::OpenAIChatCompletionsAdapter
        .decode_sse_events(sse)
        .unwrap();

    assert_eq!(
        events,
        vec![
            crate::ProviderStreamEvent::ThinkingDelta {
                text: "plan ".to_string()
            },
            crate::ProviderStreamEvent::TextDelta {
                text: "hi".to_string()
            },
            crate::ProviderStreamEvent::ThinkingDelta {
                text: "check".to_string()
            },
            crate::ProviderStreamEvent::ThinkingDelta {
                text: " same".to_string()
            },
            crate::ProviderStreamEvent::TextDelta {
                text: " there".to_string()
            },
            crate::ProviderStreamEvent::Done {
                stop_reason: verlet_history::CanonicalStopReason::EndTurn
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

    let events = crate::AnthropicMessagesAdapter
        .decode_sse_events(sse)
        .unwrap();

    assert_eq!(
        events,
        vec![
            crate::ProviderStreamEvent::Usage {
                usage: verlet_history::CanonicalUsage {
                    input_tokens: 3,
                    output_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                }
            },
            crate::ProviderStreamEvent::TextDelta {
                text: "COOL".to_string()
            },
            crate::ProviderStreamEvent::TextDelta {
                text: "_OK".to_string()
            },
            crate::ProviderStreamEvent::Usage {
                usage: verlet_history::CanonicalUsage {
                    input_tokens: 0,
                    output_tokens: 4,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                }
            },
            crate::ProviderStreamEvent::Done {
                stop_reason: verlet_history::CanonicalStopReason::EndTurn
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

    let events = crate::AnthropicMessagesAdapter
        .decode_sse_events(sse)
        .unwrap();

    assert!(matches!(
        &events[0],
        crate::ProviderStreamEvent::ToolCallDelta {
            id,
            name: Some(name),
            arguments_delta,
        } if id == "toolu_1" && name == "bash" && arguments_delta.is_empty()
    ));
    assert!(matches!(
        &events[1],
        crate::ProviderStreamEvent::ToolCallDelta {
            id,
            arguments_delta,
            ..
        } if id == "toolu_1" && arguments_delta == "{\"command\""
    ));
    assert!(matches!(
        events.last(),
        Some(crate::ProviderStreamEvent::Done {
            stop_reason: verlet_history::CanonicalStopReason::ToolUse
        })
    ));
}

#[test]
fn malformed_tool_arguments_are_rejected_at_wire_edge() {
    let chat = crate::OpenAIChatCompletionsAdapter
            .decode_response_body(&serde_json::json!({
                "choices": [{
                    "finish_reason": "tool_calls",
                    "message": {
                        "tool_calls": [{"id":"call_1","function":{"name":"bash","arguments":"not-json"}}]
                    }
                }]
            }))
            .unwrap_err();
    assert!(
        matches!(chat, crate::ProviderError::Decode(message) if message.contains("invalid chat tool call arguments"))
    );

    let responses = crate::OpenAIResponsesAdapter::default()
        .decode_response_body(&serde_json::json!({
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
        matches!(responses, crate::ProviderError::Decode(message) if message.contains("invalid OpenAI Responses function arguments"))
    );
}

const ANTHROPIC_STREAM_EVENT_JSONS: &[&str] = &[
    r#"{"type":"message_start","message":{"usage":{"input_tokens":3}}}"#,
    r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"COOL"}}"#,
    r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"_OK"}}"#,
    r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}"#,
    r#"{"type":"message_stop"}"#,
];

const ANTHROPIC_STREAM_SSE: &str = concat!(
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

fn bedrock_eventstream_body(anthropic_events: &[&str]) -> Vec<u8> {
    let mut body = Vec::new();
    for event in anthropic_events {
        let bytes = base64::engine::general_purpose::STANDARD.encode(event.as_bytes());
        let payload = serde_json::json!({ "chunk": { "bytes": bytes } }).to_string();
        body.extend(test_eventstream_frame(payload.as_bytes()));
    }
    body
}

fn bedrock_raw_eventstream_body(anthropic_events: &[&str]) -> Vec<u8> {
    let mut body = Vec::new();
    for event in anthropic_events {
        body.extend(test_eventstream_frame(event.as_bytes()));
    }
    body
}

fn bedrock_top_level_bytes_eventstream_body(anthropic_events: &[&str]) -> Vec<u8> {
    let mut body = Vec::new();
    for event in anthropic_events {
        let bytes = base64::engine::general_purpose::STANDARD.encode(event.as_bytes());
        let payload = serde_json::json!({ "bytes": bytes }).to_string();
        body.extend(test_eventstream_frame(payload.as_bytes()));
    }
    body
}

fn decoded_bedrock_chunk_payload(payload: &[u8]) -> String {
    let value: serde_json::Value = serde_json::from_slice(payload).unwrap();
    let bytes = value
        .pointer("/chunk/bytes")
        .and_then(serde_json::Value::as_str)
        .unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(bytes)
        .unwrap();
    String::from_utf8(decoded).unwrap()
}

fn test_eventstream_frame(payload: &[u8]) -> Vec<u8> {
    let headers = [
        test_eventstream_string_header(":message-type", "event"),
        test_eventstream_string_header(":event-type", "chunk"),
        test_eventstream_string_header(":content-type", "application/json"),
    ]
    .concat();
    let total_length = 16 + headers.len() + payload.len();
    let mut frame = Vec::new();
    frame.extend_from_slice(&(total_length as u32).to_be_bytes());
    frame.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    frame.extend_from_slice(&crate::crc32(&frame).to_be_bytes());
    frame.extend_from_slice(&headers);
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&crate::crc32(&frame).to_be_bytes());
    frame
}

fn test_eventstream_string_header(name: &str, value: &str) -> Vec<u8> {
    let mut header = Vec::new();
    header.push(name.len() as u8);
    header.extend_from_slice(name.as_bytes());
    header.push(7);
    header.extend_from_slice(&(value.len() as u16).to_be_bytes());
    header.extend_from_slice(value.as_bytes());
    header
}

async fn serve_fake_http_response(
    response_body: Vec<u8>,
    content_length: Option<usize>,
) -> (String, tokio::sync::oneshot::Receiver<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (request_tx, request_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_http_request(&mut socket).await;
        let _ = request_tx.send(String::from_utf8_lossy(&request).into_owned());
        let mut headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.amazon.eventstream\r\nconnection: close\r\n"
        );
        if let Some(content_length) = content_length {
            headers.push_str(&format!("content-length: {content_length}\r\n"));
        }
        headers.push_str("\r\n");
        socket.write_all(headers.as_bytes()).await.unwrap();
        for chunk in response_body.chunks(17) {
            socket.write_all(chunk).await.unwrap();
        }
    });
    (format!("http://{address}"), request_rx)
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = socket.read(&mut buffer).await.unwrap();
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if let Some(header_end) = find_header_end(&request) {
            let content_length = http_content_length(&request[..header_end]).unwrap_or(0);
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
    }
    request
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn http_content_length(headers: &[u8]) -> Option<usize> {
    let headers = String::from_utf8_lossy(headers);
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    })
}

fn stream_text(events: &[crate::ProviderStreamEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            crate::ProviderStreamEvent::TextDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn stream_usage(events: &[crate::ProviderStreamEvent]) -> verlet_history::CanonicalUsage {
    let mut usage = verlet_history::CanonicalUsage::default();
    for event in events {
        if let crate::ProviderStreamEvent::Usage { usage: next } = event {
            usage.input_tokens += next.input_tokens;
            usage.output_tokens += next.output_tokens;
            usage.cache_creation_input_tokens += next.cache_creation_input_tokens;
            usage.cache_read_input_tokens += next.cache_read_input_tokens;
        }
    }
    usage
}

fn stream_stop_reason(
    events: &[crate::ProviderStreamEvent],
) -> verlet_history::CanonicalStopReason {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            crate::ProviderStreamEvent::Done { stop_reason } => Some(*stop_reason),
            _ => None,
        })
        .unwrap_or(verlet_history::CanonicalStopReason::EndTurn)
}

fn test_content_text(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}
