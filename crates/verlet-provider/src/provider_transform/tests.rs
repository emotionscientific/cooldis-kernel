use super::*;
use crate::{
    OpenAIChatCompletionsAdapter, OpenAIResponsesAdapter, ProviderRequest, ProviderWireAdapter,
};
use serde_json::json;
use verlet_history::{CacheControl, CanonicalUsage, ThinkingMetadata, ThinkingProvider};

const ANTHROPIC: (&str, ProviderApi) = ("anthropic", ProviderApi::AnthropicMessages);
const OPENAI: (&str, ProviderApi) = ("openai", ProviderApi::OpenAIResponses);
const OPENAI_COMPATIBLE: (&str, ProviderApi) =
    ("openai_compatible", ProviderApi::OpenAIChatCompletions);

fn assistant(
    source: (&str, ProviderApi),
    content: Vec<CanonicalContent>,
    stop_reason: CanonicalStopReason,
) -> CanonicalMessage {
    CanonicalMessage::assistant(source.0, source.1, "test-model", content, stop_reason)
}

fn anthropic_thinking(text: &str) -> CanonicalContent {
    CanonicalContent::Thinking {
        text: text.to_string(),
        provider: ThinkingProvider::Anthropic,
        metadata: ThinkingMetadata::Anthropic {
            signature: Some("sig".to_string()),
        },
    }
}

fn openai_thinking(text: &str) -> CanonicalContent {
    CanonicalContent::Thinking {
        text: text.to_string(),
        provider: ThinkingProvider::OpenAIResponses,
        metadata: ThinkingMetadata::OpenAIResponses {
            item_id: Some("rs_1".to_string()),
            output_index: Some(0),
            summary_index: 0,
            encrypted_content: Some("enc".to_string()),
        },
    }
}

fn image() -> CanonicalContent {
    CanonicalContent::Image {
        data: "aGk=".to_string(),
        mime_type: "image/png".to_string(),
    }
}

fn normalize(messages: Vec<CanonicalMessage>, target: (&str, ProviderApi)) -> ReplayTransform {
    normalize_history_for_target(messages, &target.1, target.0)
}

#[test]
fn matching_provenance_history_is_a_zero_count_pass_through() {
    let messages = vec![
        CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![
                anthropic_thinking("hmm"),
                CanonicalContent::text("hello"),
                CanonicalContent::tool_call("toolu_1", "lookup", json!({"q": 1})),
            ],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result("toolu_1", "lookup", "found", false),
        assistant(
            ANTHROPIC,
            vec![CanonicalContent::text("done")],
            CanonicalStopReason::EndTurn,
        ),
    ];
    let transformed = normalize(messages.clone(), ANTHROPIC);
    assert!(transformed.counts.is_noop(), "{:?}", transformed.counts);
    assert_eq!(transformed.messages, messages);
}

#[test]
fn foreign_thinking_converts_to_tagged_text_and_native_thinking_passes() {
    let messages = vec![
        CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![anthropic_thinking("foreign reasoning")],
            CanonicalStopReason::EndTurn,
        ),
        assistant(
            OPENAI,
            vec![openai_thinking("native reasoning")],
            CanonicalStopReason::EndTurn,
        ),
        CanonicalMessage::user_text("again"),
    ];
    let transformed = normalize(messages, OPENAI);
    assert_eq!(transformed.counts.thinking_converted, 1);
    assert_eq!(transformed.counts.thinking_dropped, 0);
    let CanonicalMessage::Assistant { content, .. } = &transformed.messages[1] else {
        panic!("expected assistant");
    };
    assert_eq!(
        content[0],
        CanonicalContent::Text {
            text: "<thinking>\nforeign reasoning\n</thinking>".to_string(),
            cache_control: None,
        }
    );
    let CanonicalMessage::Assistant { content, .. } = &transformed.messages[2] else {
        panic!("expected assistant");
    };
    assert_eq!(content[0], openai_thinking("native reasoning"));
}

#[test]
fn foreign_thinking_without_visible_text_is_dropped() {
    let messages = vec![
        CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![
                CanonicalContent::Thinking {
                    text: String::new(),
                    provider: ThinkingProvider::Anthropic,
                    metadata: ThinkingMetadata::AnthropicRedacted {
                        data: "opaque".to_string(),
                    },
                },
                CanonicalContent::text("visible answer"),
            ],
            CanonicalStopReason::EndTurn,
        ),
    ];
    let transformed = normalize(messages, OPENAI);
    assert_eq!(transformed.counts.thinking_dropped, 1);
    assert_eq!(transformed.counts.thinking_converted, 0);
}

#[test]
fn errored_assistants_drop_with_their_tool_results() {
    let messages = vec![
        CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![CanonicalContent::tool_call(
                "toolu_err",
                "lookup",
                json!({}),
            )],
            CanonicalStopReason::Error,
        ),
        CanonicalMessage::tool_result("toolu_err", "lookup", "late result", false),
        CanonicalMessage::user_text("retry"),
    ];
    let transformed = normalize(messages, ANTHROPIC);
    assert_eq!(transformed.counts.errored_assistants_dropped, 1);
    assert_eq!(transformed.counts.unpaired_tool_results_dropped, 1);
    assert_eq!(transformed.messages.len(), 2);
}

#[test]
fn dangling_tool_calls_and_unpaired_tool_results_are_dropped() {
    let messages = vec![
        CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![
                CanonicalContent::tool_call("toolu_paired", "lookup", json!({})),
                CanonicalContent::tool_call("toolu_dangling", "lookup", json!({})),
            ],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result("toolu_paired", "lookup", "ok", false),
        CanonicalMessage::tool_result("toolu_unknown", "lookup", "orphan", false),
    ];
    let transformed = normalize(messages, ANTHROPIC);
    assert_eq!(transformed.counts.dangling_tool_calls_dropped, 1);
    assert_eq!(transformed.counts.unpaired_tool_results_dropped, 1);
    let CanonicalMessage::Assistant { content, .. } = &transformed.messages[1] else {
        panic!("expected assistant");
    };
    assert_eq!(content.len(), 1);
    assert!(matches!(
        &content[0],
        CanonicalContent::ToolCall { id, .. } if id == "toolu_paired"
    ));
    assert_eq!(transformed.messages.len(), 3);

    let messages = vec![
        CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![
                CanonicalContent::tool_call("toolu_dup", "lookup", json!({"q": 1})),
                CanonicalContent::tool_call("toolu_dup", "lookup", json!({"q": 2})),
            ],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result("toolu_dup", "lookup", "ambiguous", false),
        CanonicalMessage::user_text("continue"),
    ];
    let transformed = normalize(messages, ANTHROPIC);
    assert_eq!(transformed.counts.dangling_tool_calls_dropped, 2);
    assert_eq!(transformed.counts.unpaired_tool_results_dropped, 1);
    assert_eq!(transformed.counts.empty_assistants_dropped, 1);
    assert_eq!(
        transformed
            .messages
            .iter()
            .filter(|message| matches!(message, CanonicalMessage::User { .. }))
            .count(),
        2
    );
}

#[test]
fn assistants_left_empty_by_drops_are_removed() {
    let messages = vec![
        CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![CanonicalContent::tool_call(
                "toolu_dangling",
                "lookup",
                json!({}),
            )],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::user_text("again"),
    ];
    let transformed = normalize(messages, ANTHROPIC);
    assert_eq!(transformed.counts.dangling_tool_calls_dropped, 1);
    assert_eq!(transformed.counts.empty_assistants_dropped, 1);
    assert_eq!(transformed.messages.len(), 2);
}

#[test]
fn historical_user_images_drop_for_non_image_targets_but_latest_user_keeps_them() {
    let messages = vec![
        CanonicalMessage::User {
            content: vec![CanonicalContent::text("look at this"), image()],
            timestamp_ms: 1,
        },
        assistant(
            OPENAI_COMPATIBLE,
            vec![CanonicalContent::text("seen")],
            CanonicalStopReason::EndTurn,
        ),
        CanonicalMessage::User {
            content: vec![CanonicalContent::text("and this"), image()],
            timestamp_ms: 2,
        },
    ];
    let transformed = normalize(messages, OPENAI_COMPATIBLE);
    assert_eq!(transformed.counts.images_dropped, 1);
    let CanonicalMessage::User { content, .. } = &transformed.messages[0] else {
        panic!("expected user");
    };
    assert_eq!(content.len(), 1);
    let CanonicalMessage::User { content, .. } = &transformed.messages[2] else {
        panic!("expected user");
    };
    assert_eq!(content.len(), 2, "latest user message keeps its image");

    let messages = vec![
        CanonicalMessage::User {
            content: vec![CanonicalContent::text("look at this"), image()],
            timestamp_ms: 1,
        },
        assistant(
            OPENAI_COMPATIBLE,
            vec![CanonicalContent::tool_call("call_1", "lookup", json!({}))],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::tool_result("call_1", "lookup", "found", false),
    ];
    let transformed = normalize(messages, OPENAI_COMPATIBLE);
    assert_eq!(transformed.counts.images_dropped, 1);
    assert_eq!(transformed.messages.len(), 3);
    let CanonicalMessage::User { content, .. } = &transformed.messages[0] else {
        panic!("expected user");
    };
    assert_eq!(content, &[CanonicalContent::text("look at this")]);
}

#[test]
fn cache_controls_strip_for_non_cache_targets_and_survive_for_anthropic() {
    let messages = vec![
        CanonicalMessage::User {
            content: vec![CanonicalContent::cached_text(
                "cached prompt",
                CacheControl::ephemeral(),
            )],
            timestamp_ms: 1,
        },
        assistant(
            ANTHROPIC,
            vec![CanonicalContent::tool_call("toolu_1", "lookup", json!({}))],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::ToolResult {
            tool_call_id: "toolu_1".to_string(),
            tool_name: "lookup".to_string(),
            content: vec![CanonicalContent::text("ok")],
            is_error: false,
            cache_control: Some(CacheControl::ephemeral()),
            timestamp_ms: 2,
        },
        CanonicalMessage::user_text("next"),
    ];
    let kept = normalize(messages.clone(), ANTHROPIC);
    assert!(kept.counts.is_noop());
    let stripped = normalize(messages, OPENAI);
    assert_eq!(stripped.counts.cache_controls_stripped, 2);
}

#[test]
fn normalized_anthropic_history_passes_openai_validation_and_builds_wire_bodies() {
    let messages = vec![
        CanonicalMessage::User {
            content: vec![
                CanonicalContent::cached_text("cached", CacheControl::ephemeral()),
                image(),
            ],
            timestamp_ms: 1,
        },
        assistant(
            ANTHROPIC,
            vec![
                anthropic_thinking("let me look"),
                CanonicalContent::tool_call("toolu_1", "lookup", json!({"q": 1})),
            ],
            CanonicalStopReason::ToolUse,
        ),
        CanonicalMessage::ToolResult {
            tool_call_id: "toolu_1".to_string(),
            tool_name: "lookup".to_string(),
            content: vec![CanonicalContent::text("found")],
            is_error: false,
            cache_control: Some(CacheControl::ephemeral()),
            timestamp_ms: 2,
        },
        assistant(
            ANTHROPIC,
            vec![CanonicalContent::text("answer")],
            CanonicalStopReason::EndTurn,
        ),
        CanonicalMessage::user_text("continue"),
    ];

    for (target, body_must_validate) in [(OPENAI, true), (OPENAI_COMPATIBLE, true)] {
        let transformed = normalize(messages.clone(), target.clone());
        let request = ProviderRequest {
            api: target.1.clone(),
            provider: target.0.to_string(),
            model: "test-model".to_string(),
            system: Vec::new(),
            messages: transformed.messages,
            tools: Vec::new(),
            max_tokens: 256,
            temperature: None,
            thinking: None,
        };
        let body = match target.1 {
            ProviderApi::OpenAIResponses => {
                OpenAIResponsesAdapter::default().build_request_body(&request)
            }
            ProviderApi::OpenAIChatCompletions => {
                OpenAIChatCompletionsAdapter.build_request_body(&request)
            }
            _ => unreachable!(),
        };
        assert_eq!(body.is_ok(), body_must_validate, "{target:?}: {body:?}");
    }
}

#[test]
fn unnormalized_anthropic_history_fails_openai_validation() {
    let request = ProviderRequest {
        api: ProviderApi::OpenAIResponses,
        provider: "openai".to_string(),
        model: "test-model".to_string(),
        system: Vec::new(),
        messages: vec![CanonicalMessage::User {
            content: vec![CanonicalContent::cached_text(
                "cached",
                CacheControl::ephemeral(),
            )],
            timestamp_ms: 1,
        }],
        tools: Vec::new(),
        max_tokens: 256,
        temperature: None,
        thinking: None,
    };
    assert!(
        OpenAIResponsesAdapter::default()
            .build_request_body(&request)
            .is_err()
    );
}

#[test]
fn usage_and_provenance_survive_the_rebuild() {
    let usage = CanonicalUsage {
        input_tokens: 7,
        ..CanonicalUsage::default()
    };
    let messages = vec![
        CanonicalMessage::user_text("hi"),
        CanonicalMessage::assistant_with_usage(
            "anthropic",
            ProviderApi::AnthropicMessages,
            "test-model",
            vec![anthropic_thinking("t"), CanonicalContent::text("a")],
            usage.clone(),
            CanonicalStopReason::EndTurn,
        ),
    ];
    let transformed = normalize(messages, OPENAI);
    let CanonicalMessage::Assistant {
        usage: kept_usage,
        provider,
        api,
        ..
    } = &transformed.messages[1]
    else {
        panic!("expected assistant");
    };
    assert_eq!(kept_usage, &usage);
    assert_eq!(provider, "anthropic");
    assert_eq!(api, &ProviderApi::AnthropicMessages);
}
