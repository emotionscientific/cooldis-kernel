pub mod provider_transform;

use async_trait::async_trait;
use base64::Engine;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;
use verlet_history::{
    CacheControl, CanonicalContent, CanonicalMessage, CanonicalStopReason, CanonicalUsage,
    ProviderApi, ThinkingMetadata, ThinkingProvider,
};

/// Providers that accept Zhipu-style chat-completions `thinking` parameters.
///
/// OpenAI-compatible chat-completions endpoints are commonly strict about
/// unknown request fields, so the extra `thinking` object is only sent to
/// provider ids verified to use the Zhipu convention. The generic
/// `openai_compatible` id is a fixture-only example for this public checkout.
const ZHIPU_CONVENTION_CHAT_PROVIDERS: &[&str] = &["openai_compatible", "zhipu", "glm"];
const BEDROCK_ANTHROPIC_VERSION: &str = "bedrock-2023-05-31";
const AWS_SIGV4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
type HmacSha256 = Hmac<Sha256>;

pub type ProviderResult<T> = Result<T, ProviderError>;

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("adapter {adapter} cannot handle request api {api:?}")]
    ApiMismatch {
        adapter: &'static str,
        api: ProviderApi,
    },
    #[error("wire payload decode failed: {0}")]
    Decode(String),
    #[error("provider HTTP request failed: {0}")]
    Http(String),
    #[error("provider HTTP status {status}: {body}")]
    HttpStatus { status: StatusCode, body: String },
    #[error("provider request cancelled")]
    Cancelled,
    #[error("provider {provider} ({api:?}) does not support {capability}: {detail}")]
    UnsupportedCapability {
        provider: String,
        api: ProviderApi,
        capability: &'static str,
        detail: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemBlock {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,
}

impl SystemBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            cache_control: None,
        }
    }

    pub fn cached(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            cache_control: Some(CacheControl::ephemeral()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,
}

impl ToolDefinition {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
            cache_control: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ThinkingConfig {
    Effort { effort: ThinkingEffort },
    Budget { budget_tokens: u32 },
    Disabled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingEffort {
    Low,
    Medium,
    High,
    XHigh,
    Max,
    Other(String),
}

impl ThinkingEffort {
    pub fn as_openai_wire(&self) -> &str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
            Self::Other(value) => value.as_str(),
        }
    }

    pub fn as_anthropic_wire(&self) -> &str {
        self.as_openai_wire()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderRequest {
    pub api: ProviderApi,
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub system: Vec<SystemBlock>,
    #[serde(default)]
    pub messages: Vec<CanonicalMessage>,
    #[serde(default)]
    pub tools: Vec<ToolDefinition>,
    pub max_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingConfig>,
}

impl ProviderRequest {
    pub fn new(api: ProviderApi, provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            api,
            provider: provider.into(),
            model: model.into(),
            system: Vec::new(),
            messages: Vec::new(),
            tools: Vec::new(),
            max_tokens: 1024,
            temperature: None,
            thinking: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderResponse {
    pub content: Vec<CanonicalContent>,
    pub usage: CanonicalUsage,
    pub stop_reason: CanonicalStopReason,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderStreamEvent {
    TextDelta {
        text: String,
    },
    ThinkingDelta {
        text: String,
    },
    ToolCallDelta {
        id: String,
        name: Option<String>,
        arguments_delta: String,
    },
    Content {
        content: CanonicalContent,
    },
    Usage {
        usage: CanonicalUsage,
    },
    Done {
        stop_reason: CanonicalStopReason,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderRequestMode {
    Complete,
    Stream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderAbiProjection {
    LlmTool,
    Text,
    ImageInput,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderToolResultConstraints {
    pub supports_error_flag: bool,
    pub requires_known_tool_call_id: bool,
    pub max_content_bytes: Option<usize>,
}

impl ProviderToolResultConstraints {
    pub fn open_tool_results() -> Self {
        Self {
            supports_error_flag: true,
            requires_known_tool_call_id: true,
            max_content_bytes: None,
        }
    }

    pub fn unsupported() -> Self {
        Self {
            supports_error_flag: false,
            requires_known_tool_call_id: false,
            max_content_bytes: Some(0),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderContextPolicy {
    pub max_messages: Option<usize>,
    pub max_text_bytes: Option<usize>,
}

impl ProviderContextPolicy {
    pub fn unbounded() -> Self {
        Self {
            max_messages: None,
            max_text_bytes: None,
        }
    }

    pub fn is_unbounded(&self) -> bool {
        self.max_messages.is_none() && self.max_text_bytes.is_none()
    }
}

impl Default for ProviderContextPolicy {
    fn default() -> Self {
        Self::unbounded()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderContextCompilation {
    pub messages: Vec<CanonicalMessage>,
    pub dropped_messages: usize,
    pub truncated_text_bytes: usize,
    pub retained_text_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderCapabilityRecord {
    pub api: ProviderApi,
    pub provider_family: String,
    pub supports_tools: bool,
    pub supports_streaming: bool,
    pub supports_reasoning: bool,
    pub supports_cache_control: bool,
    pub supports_images: bool,
    pub supports_attachments: bool,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub context_policy: ProviderContextPolicy,
    pub tool_result_constraints: ProviderToolResultConstraints,
    pub supported_abi_projections: BTreeSet<ProviderAbiProjection>,
}

impl ProviderCapabilityRecord {
    pub fn for_api(api: ProviderApi) -> Self {
        match api {
            ProviderApi::OpenAIResponses => Self {
                api: ProviderApi::OpenAIResponses,
                provider_family: "openai_responses".to_string(),
                supports_tools: true,
                supports_streaming: true,
                supports_reasoning: true,
                supports_cache_control: false,
                supports_images: true,
                supports_attachments: false,
                max_context_tokens: None,
                max_output_tokens: None,
                context_policy: ProviderContextPolicy::unbounded(),
                tool_result_constraints: ProviderToolResultConstraints::open_tool_results(),
                supported_abi_projections: BTreeSet::from([
                    ProviderAbiProjection::Text,
                    ProviderAbiProjection::ImageInput,
                    ProviderAbiProjection::LlmTool,
                ]),
            },
            ProviderApi::OpenAIChatCompletions => Self {
                api: ProviderApi::OpenAIChatCompletions,
                provider_family: "openai_chat_completions".to_string(),
                supports_tools: true,
                supports_streaming: true,
                supports_reasoning: true,
                supports_cache_control: false,
                supports_images: false,
                supports_attachments: false,
                max_context_tokens: None,
                max_output_tokens: None,
                context_policy: ProviderContextPolicy::unbounded(),
                tool_result_constraints: ProviderToolResultConstraints::open_tool_results(),
                supported_abi_projections: BTreeSet::from([
                    ProviderAbiProjection::Text,
                    ProviderAbiProjection::LlmTool,
                ]),
            },
            ProviderApi::AnthropicMessages => Self {
                api: ProviderApi::AnthropicMessages,
                provider_family: "anthropic_messages".to_string(),
                supports_tools: true,
                supports_streaming: true,
                supports_reasoning: true,
                supports_cache_control: true,
                supports_images: true,
                supports_attachments: false,
                max_context_tokens: None,
                max_output_tokens: None,
                context_policy: ProviderContextPolicy::unbounded(),
                tool_result_constraints: ProviderToolResultConstraints::open_tool_results(),
                supported_abi_projections: BTreeSet::from([
                    ProviderAbiProjection::Text,
                    ProviderAbiProjection::ImageInput,
                    ProviderAbiProjection::LlmTool,
                ]),
            },
            ProviderApi::Other(provider_family) => Self::local_offline(provider_family, "local"),
        }
    }

    pub fn local_offline(provider_family: impl Into<String>, model: impl Into<String>) -> Self {
        let provider_family = provider_family.into();
        let _model = model.into();
        Self {
            api: ProviderApi::Other(provider_family.clone()),
            provider_family,
            supports_tools: false,
            supports_streaming: false,
            supports_reasoning: false,
            supports_cache_control: false,
            supports_images: false,
            supports_attachments: false,
            max_context_tokens: None,
            max_output_tokens: Some(4096),
            context_policy: ProviderContextPolicy::unbounded(),
            tool_result_constraints: ProviderToolResultConstraints::unsupported(),
            supported_abi_projections: BTreeSet::from([ProviderAbiProjection::Text]),
        }
    }

    pub fn validate_request(
        &self,
        request: &ProviderRequest,
        mode: ProviderRequestMode,
    ) -> ProviderResult<()> {
        if request.api != self.api {
            return Err(ProviderError::ApiMismatch {
                adapter: "provider_capabilities",
                api: request.api.clone(),
            });
        }
        if mode == ProviderRequestMode::Stream && !self.supports_streaming {
            return Err(self.unsupported("streaming", "streaming requests are disabled"));
        }
        if !self.supports_tools && !request.tools.is_empty() {
            return Err(self.unsupported("tools", "tool definitions were provided"));
        }
        if !self.supports_reasoning
            && !matches!(request.thinking, None | Some(ThinkingConfig::Disabled))
        {
            return Err(self.unsupported("reasoning", "thinking/reasoning was requested"));
        }
        if !self.supports_cache_control && request_uses_cache_control(request) {
            return Err(self.unsupported("cache_control", "cache controls were provided"));
        }
        if !self.supports_images && request_uses_images(request) {
            return Err(self.unsupported("images", "image content was provided"));
        }
        if let Some(max_output_tokens) = self.max_output_tokens
            && request.max_tokens > max_output_tokens
        {
            return Err(self.unsupported(
                "max_output_tokens",
                format!(
                    "requested {}, limit is {max_output_tokens}",
                    request.max_tokens
                ),
            ));
        }
        if let Some(max_content_bytes) = self.tool_result_constraints.max_content_bytes {
            for message in &request.messages {
                if let CanonicalMessage::ToolResult { content, .. } = message {
                    let bytes = content_text_bytes(content);
                    if bytes > max_content_bytes {
                        return Err(self.unsupported(
                            "tool_result_constraints",
                            format!(
                                "tool result content is {bytes} bytes, limit is {max_content_bytes}"
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn unsupported(&self, capability: &'static str, detail: impl Into<String>) -> ProviderError {
        ProviderError::UnsupportedCapability {
            provider: self.provider_family.clone(),
            api: self.api.clone(),
            capability,
            detail: detail.into(),
        }
    }
}

pub fn compile_provider_context(
    messages: Vec<CanonicalMessage>,
    policy: &ProviderContextPolicy,
) -> ProviderContextCompilation {
    let original_len = messages.len();
    let mut messages = if let Some(max_messages) = policy.max_messages {
        recent_messages_preserving_tool_result_issuers(messages, max_messages)
    } else {
        messages
    };
    let dropped_messages = original_len.saturating_sub(messages.len());
    let original_text_bytes = messages_text_bytes(&messages);

    if let Some(max_text_bytes) = policy.max_text_bytes {
        truncate_messages_to_recent_text_bytes(&mut messages, max_text_bytes);
    }

    let retained_text_bytes = messages_text_bytes(&messages);
    ProviderContextCompilation {
        messages,
        dropped_messages,
        truncated_text_bytes: original_text_bytes.saturating_sub(retained_text_bytes),
        retained_text_bytes,
    }
}

fn recent_messages_preserving_tool_result_issuers(
    messages: Vec<CanonicalMessage>,
    max_messages: usize,
) -> Vec<CanonicalMessage> {
    if messages.len() <= max_messages {
        return messages;
    }

    let cutoff = messages.len().saturating_sub(max_messages);
    let mut included = (cutoff..messages.len()).collect::<BTreeSet<_>>();
    let mut issuer_by_call_id = HashMap::<&str, usize>::new();
    for (index, message) in messages.iter().enumerate() {
        if let CanonicalMessage::Assistant { content, .. } = message {
            for block in content {
                if let CanonicalContent::ToolCall { id, .. } = block {
                    issuer_by_call_id.insert(id.as_str(), index);
                }
            }
        }
    }

    for message in &messages[cutoff..] {
        if let CanonicalMessage::ToolResult { tool_call_id, .. } = message
            && let Some(&issuer) = issuer_by_call_id.get(tool_call_id.as_str())
        {
            included.insert(issuer);
        }
    }
    drop(issuer_by_call_id);

    messages
        .into_iter()
        .enumerate()
        .filter_map(|(index, message)| included.contains(&index).then_some(message))
        .collect()
}

pub fn compile_provider_request_context(
    mut request: ProviderRequest,
    policy: &ProviderContextPolicy,
) -> (ProviderRequest, ProviderContextCompilation) {
    let compilation = compile_provider_context(std::mem::take(&mut request.messages), policy);
    request.messages = compilation.messages.clone();
    (request, compilation)
}

fn request_uses_cache_control(request: &ProviderRequest) -> bool {
    request
        .system
        .iter()
        .any(|block| block.cache_control.is_some())
        || request
            .tools
            .iter()
            .any(|tool| tool.cache_control.is_some())
        || request.messages.iter().any(message_uses_cache_control)
}

fn message_uses_cache_control(message: &CanonicalMessage) -> bool {
    match message {
        CanonicalMessage::User { content, .. } | CanonicalMessage::Assistant { content, .. } => {
            content.iter().any(content_uses_cache_control)
        }
        CanonicalMessage::ToolResult {
            content,
            cache_control,
            ..
        } => cache_control.is_some() || content.iter().any(content_uses_cache_control),
    }
}

fn content_uses_cache_control(content: &CanonicalContent) -> bool {
    matches!(
        content,
        CanonicalContent::Text {
            cache_control: Some(_),
            ..
        }
    )
}

fn request_uses_images(request: &ProviderRequest) -> bool {
    request.messages.iter().any(|message| match message {
        CanonicalMessage::User { content, .. } | CanonicalMessage::Assistant { content, .. } => {
            content
                .iter()
                .any(|content| matches!(content, CanonicalContent::Image { .. }))
        }
        CanonicalMessage::ToolResult { content, .. } => content
            .iter()
            .any(|content| matches!(content, CanonicalContent::Image { .. })),
    })
}

fn messages_text_bytes(messages: &[CanonicalMessage]) -> usize {
    messages
        .iter()
        .map(|message| match message {
            CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => content_text_bytes(content),
        })
        .sum()
}

fn content_text_bytes(content: &[CanonicalContent]) -> usize {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.len()),
            CanonicalContent::Thinking { text, .. } => Some(text.len()),
            _ => None,
        })
        .sum()
}

fn truncate_messages_to_recent_text_bytes(messages: &mut [CanonicalMessage], max_bytes: usize) {
    let mut remaining = max_bytes;
    for message in messages.iter_mut().rev() {
        match message {
            CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => {
                truncate_content_to_recent_text_bytes(content, &mut remaining);
            }
        }
    }
}

fn truncate_content_to_recent_text_bytes(
    content: &mut Vec<CanonicalContent>,
    remaining: &mut usize,
) {
    for block in content.iter_mut().rev() {
        match block {
            CanonicalContent::Text { text, .. } | CanonicalContent::Thinking { text, .. } => {
                truncate_string_to_recent_bytes(text, remaining);
            }
            CanonicalContent::Image { .. } | CanonicalContent::ToolCall { .. } => {}
        }
    }
    content.retain(|block| match block {
        CanonicalContent::Text { text, .. } | CanonicalContent::Thinking { text, .. } => {
            !text.is_empty()
        }
        _ => true,
    });
}

fn truncate_string_to_recent_bytes(text: &mut String, remaining: &mut usize) {
    let bytes = text.len();
    if bytes <= *remaining {
        *remaining -= bytes;
        return;
    }
    if *remaining == 0 {
        text.clear();
        return;
    }
    let start = text
        .char_indices()
        .map(|(index, _)| index)
        .find(|index| bytes - *index <= *remaining)
        .unwrap_or(bytes);
    let suffix = text[start..].to_string();
    *remaining = (*remaining).saturating_sub(suffix.len());
    *text = suffix;
}

pub trait ProviderWireAdapter: Send + Sync {
    fn api(&self) -> ProviderApi;
    fn capabilities(&self) -> ProviderCapabilityRecord {
        ProviderCapabilityRecord::for_api(self.api())
    }
    fn build_request_body(&self, request: &ProviderRequest) -> ProviderResult<Value>;
    fn build_stream_request_body(&self, request: &ProviderRequest) -> ProviderResult<Value> {
        self.capabilities()
            .validate_request(request, ProviderRequestMode::Stream)?;
        let mut body = self.build_request_body(request)?;
        body["stream"] = json!(true);
        Ok(body)
    }
    fn stream_endpoint_url(&self, endpoint_url: &str) -> String {
        endpoint_url.to_string()
    }
    fn stream_request_headers(&self) -> Vec<(&'static str, &'static str)> {
        vec![("accept", "text/event-stream")]
    }
    fn decode_response_body(&self, body: &Value) -> ProviderResult<ProviderResponse>;
    fn decode_stream_events(&self, _sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        Err(ProviderError::Decode(format!(
            "adapter {:?} does not support streaming decode",
            self.api()
        )))
    }
    fn decode_stream_response(&self, body: &[u8]) -> ProviderResult<Vec<ProviderStreamEvent>> {
        let sse = std::str::from_utf8(body).map_err(|err| {
            ProviderError::Decode(format!("stream response was not UTF-8: {err}"))
        })?;
        self.decode_stream_events(sse)
    }
}

#[async_trait]
pub trait ProviderClient: Send + Sync {
    fn capabilities(&self) -> Option<ProviderCapabilityRecord> {
        None
    }

    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse>;

    async fn complete_cancellable(
        &self,
        request: &ProviderRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> ProviderResult<ProviderResponse> {
        tokio::select! {
            response = self.complete(request) => response,
            _ = cancellation.cancelled() => Err(ProviderError::Cancelled),
        }
    }

    async fn stream(&self, request: &ProviderRequest) -> ProviderResult<Vec<ProviderStreamEvent>> {
        if let Some(capabilities) = self.capabilities() {
            capabilities.validate_request(request, ProviderRequestMode::Stream)?;
        }
        let response = self.complete(request).await?;
        let mut events = response
            .content
            .into_iter()
            .map(|content| ProviderStreamEvent::Content { content })
            .collect::<Vec<_>>();
        if response.usage != CanonicalUsage::default() {
            events.push(ProviderStreamEvent::Usage {
                usage: response.usage,
            });
        }
        events.push(ProviderStreamEvent::Done {
            stop_reason: response.stop_reason,
        });
        Ok(events)
    }

    async fn stream_cancellable(
        &self,
        request: &ProviderRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> ProviderResult<Vec<ProviderStreamEvent>> {
        tokio::select! {
            response = self.stream(request) => response,
            _ = cancellation.cancelled() => Err(ProviderError::Cancelled),
        }
    }
}

#[derive(Clone)]
pub struct ProviderHttpClient {
    http: reqwest::Client,
    endpoint: ProviderEndpoint,
    adapter: std::sync::Arc<dyn ProviderWireAdapter>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderEndpoint {
    pub url: String,
    pub auth: ProviderAuth,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderAuth {
    Bearer {
        token: String,
    },
    AnthropicApiKey {
        key: String,
    },
    AwsSigV4 {
        access_key_id: String,
        secret_access_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token: Option<String>,
        region: String,
        service: String,
    },
    None,
}

impl ProviderEndpoint {
    pub fn openai_responses(base_url: impl AsRef<str>, token: impl Into<String>) -> Self {
        Self {
            url: provider_endpoint_url(base_url.as_ref(), "responses"),
            auth: ProviderAuth::Bearer {
                token: token.into(),
            },
            headers: Vec::new(),
        }
    }

    pub fn openai_chat_completions(base_url: impl AsRef<str>, token: impl Into<String>) -> Self {
        Self {
            url: provider_endpoint_url(base_url.as_ref(), "chat/completions"),
            auth: ProviderAuth::Bearer {
                token: token.into(),
            },
            headers: Vec::new(),
        }
    }

    pub fn anthropic_messages(base_url: impl AsRef<str>, key: impl Into<String>) -> Self {
        Self {
            url: provider_endpoint_url(base_url.as_ref(), "messages"),
            auth: ProviderAuth::AnthropicApiKey { key: key.into() },
            headers: vec![("anthropic-version".to_string(), "2023-06-01".to_string())],
        }
    }

    pub fn anthropic_bedrock(
        region: impl AsRef<str>,
        model_id: impl AsRef<str>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        let region = region.as_ref();
        let base_url = format!("https://bedrock-runtime.{region}.amazonaws.com");
        Self::anthropic_bedrock_with_base_url(
            base_url,
            region,
            model_id,
            access_key_id,
            secret_access_key,
            session_token,
        )
    }

    pub fn anthropic_bedrock_with_base_url(
        base_url: impl AsRef<str>,
        region: impl AsRef<str>,
        model_id: impl AsRef<str>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        let region = region.as_ref().to_string();
        let model_id = aws_uri_encode(model_id.as_ref());
        Self {
            url: format!(
                "{}/model/{model_id}/invoke",
                base_url.as_ref().trim_end_matches('/')
            ),
            auth: ProviderAuth::AwsSigV4 {
                access_key_id: access_key_id.into(),
                secret_access_key: secret_access_key.into(),
                session_token,
                region,
                service: "bedrock".to_string(),
            },
            headers: Vec::new(),
        }
    }
}

fn provider_endpoint_url(base_url: &str, endpoint: &str) -> String {
    let base_url = base_url.trim_end_matches('/');
    if base_url.ends_with("/v1") {
        format!("{base_url}/{endpoint}")
    } else {
        format!("{base_url}/v1/{endpoint}")
    }
}

impl ProviderHttpClient {
    pub fn new(
        endpoint: ProviderEndpoint,
        adapter: std::sync::Arc<dyn ProviderWireAdapter>,
    ) -> ProviderResult<Self> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|err| ProviderError::Http(err.to_string()))?;
        Ok(Self {
            http,
            endpoint,
            adapter,
        })
    }

    pub fn with_http(
        http: reqwest::Client,
        endpoint: ProviderEndpoint,
        adapter: std::sync::Arc<dyn ProviderWireAdapter>,
    ) -> Self {
        Self {
            http,
            endpoint,
            adapter,
        }
    }
}

fn provider_json_body(body: &Value) -> ProviderResult<Vec<u8>> {
    serde_json::to_vec(body)
        .map_err(|err| ProviderError::Decode(format!("failed to encode provider JSON: {err}")))
}

fn apply_endpoint_auth(
    mut builder: reqwest::RequestBuilder,
    endpoint: &ProviderEndpoint,
    request_url: &str,
    body: &[u8],
    extra_signed_headers: &[(String, String)],
) -> ProviderResult<reqwest::RequestBuilder> {
    builder = match &endpoint.auth {
        ProviderAuth::Bearer { token } => builder.bearer_auth(token),
        ProviderAuth::AnthropicApiKey { key } => builder.header("x-api-key", key),
        ProviderAuth::AwsSigV4 {
            access_key_id,
            secret_access_key,
            session_token,
            region,
            service,
        } => {
            for (name, value) in aws_sigv4_headers(AwsSigV4Request {
                method: "POST",
                url: request_url,
                body,
                access_key_id,
                secret_access_key,
                session_token: session_token.as_deref(),
                region,
                service,
                content_type: "application/json",
                extra_headers: extra_signed_headers,
                now: chrono::Utc::now(),
            })? {
                builder = builder.header(name, value);
            }
            builder
        }
        ProviderAuth::None => builder,
    };
    for (key, value) in &endpoint.headers {
        builder = builder.header(key, value);
    }
    Ok(builder)
}

struct AwsSigV4Request<'a> {
    method: &'a str,
    url: &'a str,
    body: &'a [u8],
    access_key_id: &'a str,
    secret_access_key: &'a str,
    session_token: Option<&'a str>,
    region: &'a str,
    service: &'a str,
    content_type: &'a str,
    extra_headers: &'a [(String, String)],
    now: chrono::DateTime<chrono::Utc>,
}

fn aws_sigv4_headers(request: AwsSigV4Request<'_>) -> ProviderResult<Vec<(String, String)>> {
    let url = reqwest::Url::parse(request.url)
        .map_err(|err| ProviderError::Http(format!("invalid AWS SigV4 endpoint URL: {err}")))?;
    let host = url_host_header(&url)?;
    let amz_date = request.now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = request.now.format("%Y%m%d").to_string();
    let payload_hash = hex::encode(Sha256::digest(request.body));

    let mut canonical_header_values = vec![
        ("content-type".to_string(), request.content_type.to_string()),
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    if let Some(session_token) = request.session_token {
        canonical_header_values.push((
            "x-amz-security-token".to_string(),
            session_token.to_string(),
        ));
    }
    for (name, value) in request.extra_headers {
        canonical_header_values.push((name.to_ascii_lowercase(), value.to_string()));
    }
    canonical_header_values.sort_by(|left, right| left.0.cmp(&right.0));
    let canonical_headers = canonical_header_values
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", normalize_header_value(value)))
        .collect::<String>();
    let signed_headers = canonical_header_values
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        canonical_uri(&url),
        canonical_query(&url),
        canonical_headers,
        signed_headers,
        payload_hash
    );
    let credential_scope = format!(
        "{date_stamp}/{}/{}/aws4_request",
        request.region, request.service
    );
    let string_to_sign = format!(
        "{AWS_SIGV4_ALGORITHM}\n{amz_date}\n{credential_scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let signing_key = aws_sigv4_signing_key(
        request.secret_access_key,
        &date_stamp,
        request.region,
        request.service,
    );
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let authorization = format!(
        "{AWS_SIGV4_ALGORITHM} Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
        request.access_key_id
    );

    let mut headers = vec![
        ("host".to_string(), host),
        ("x-amz-content-sha256".to_string(), payload_hash),
        ("x-amz-date".to_string(), amz_date),
        ("authorization".to_string(), authorization),
    ];
    if let Some(session_token) = request.session_token {
        headers.push((
            "x-amz-security-token".to_string(),
            session_token.to_string(),
        ));
    }
    Ok(headers)
}

fn aws_sigv4_signing_key(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Vec<u8> {
    let date_key = hmac_sha256(
        format!("AWS4{secret_access_key}").as_bytes(),
        date.as_bytes(),
    );
    let date_region_key = hmac_sha256(&date_key, region.as_bytes());
    let date_region_service_key = hmac_sha256(&date_region_key, service.as_bytes());
    hmac_sha256(&date_region_service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn url_host_header(url: &reqwest::Url) -> ProviderResult<String> {
    let host = url
        .host_str()
        .ok_or_else(|| ProviderError::Http("AWS SigV4 endpoint URL had no host".to_string()))?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

fn canonical_uri(url: &reqwest::Url) -> String {
    let path = url.path();
    if path.is_empty() {
        "/".to_string()
    } else {
        aws_canonical_uri_path(path)
    }
}

fn aws_canonical_uri_path(path: &str) -> String {
    let mut encoded = String::with_capacity(path.len());
    for byte in path.bytes() {
        if byte == b'/' {
            encoded.push('/');
        } else if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn canonical_query(url: &reqwest::Url) -> String {
    let mut pairs = url
        .query_pairs()
        .map(|(name, value)| format!("{}={}", aws_uri_encode(&name), aws_uri_encode(&value)))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs.join("&")
}

fn normalize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn aws_uri_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

#[async_trait]
impl ProviderClient for ProviderHttpClient {
    fn capabilities(&self) -> Option<ProviderCapabilityRecord> {
        Some(self.adapter.capabilities())
    }

    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        if self.adapter.api() != request.api {
            return Err(ProviderError::ApiMismatch {
                adapter: "provider_http_client",
                api: request.api.clone(),
            });
        }
        self.adapter
            .capabilities()
            .validate_request(request, ProviderRequestMode::Complete)?;
        let body = self.adapter.build_request_body(request)?;
        let body = provider_json_body(&body)?;
        let mut builder = self
            .http
            .post(&self.endpoint.url)
            .header("content-type", "application/json")
            .body(body.clone());
        builder = apply_endpoint_auth(builder, &self.endpoint, &self.endpoint.url, &body, &[])?;

        let response = builder
            .send()
            .await
            .map_err(|err| ProviderError::Http(err.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|err| ProviderError::Http(err.to_string()))?;
        if !status.is_success() {
            return Err(ProviderError::HttpStatus { status, body: text });
        }
        let value = serde_json::from_str(&text)
            .map_err(|err| ProviderError::Decode(format!("invalid provider JSON: {err}")))?;
        self.adapter.decode_response_body(&value)
    }

    async fn stream(&self, request: &ProviderRequest) -> ProviderResult<Vec<ProviderStreamEvent>> {
        if self.adapter.api() != request.api {
            return Err(ProviderError::ApiMismatch {
                adapter: "provider_http_client",
                api: request.api.clone(),
            });
        }
        self.adapter
            .capabilities()
            .validate_request(request, ProviderRequestMode::Stream)?;
        let body = self.adapter.build_stream_request_body(request)?;
        let body = provider_json_body(&body)?;
        let stream_url = self.adapter.stream_endpoint_url(&self.endpoint.url);
        let mut builder = self
            .http
            .post(&stream_url)
            .header("content-type", "application/json")
            .body(body.clone());
        let stream_headers = self.adapter.stream_request_headers();
        let extra_signed_headers = stream_headers
            .iter()
            .filter(|(name, _)| name.to_ascii_lowercase().starts_with("x-amz"))
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<Vec<_>>();
        for (name, value) in stream_headers {
            builder = builder.header(name, value);
        }
        builder = apply_endpoint_auth(
            builder,
            &self.endpoint,
            &stream_url,
            &body,
            &extra_signed_headers,
        )?;

        let response = builder
            .send()
            .await
            .map_err(|err| ProviderError::Http(err.to_string()))?;
        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let content_length = response.content_length();
        let bytes = response
            .bytes()
            .await
            .map_err(|err| ProviderError::Http(err.to_string()))?;
        if !status.is_success() {
            return Err(ProviderError::HttpStatus {
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        if bytes.is_empty() {
            return Err(ProviderError::Decode(format!(
                "provider stream response body was empty; content_type={}; content_length={}",
                content_type.unwrap_or_else(|| "unknown".to_string()),
                content_length
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            )));
        }
        self.adapter.decode_stream_response(&bytes)
    }
}

#[derive(Clone, Debug)]
pub struct LocalOfflineProviderClient {
    capabilities: ProviderCapabilityRecord,
}

impl LocalOfflineProviderClient {
    pub fn new(provider_family: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            capabilities: ProviderCapabilityRecord::local_offline(provider_family, model),
        }
    }
}

#[async_trait]
impl ProviderClient for LocalOfflineProviderClient {
    fn capabilities(&self) -> Option<ProviderCapabilityRecord> {
        Some(self.capabilities.clone())
    }

    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.capabilities
            .validate_request(request, ProviderRequestMode::Complete)?;
        let compilation =
            compile_provider_context(request.messages.clone(), &self.capabilities.context_policy);
        let last_user_text = compilation
            .messages
            .iter()
            .rev()
            .find_map(|message| match message {
                CanonicalMessage::User { content, .. } => {
                    let text = text_from_content(content, "\n");
                    (!text.is_empty()).then_some(text)
                }
                _ => None,
            })
            .unwrap_or_default();
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text(format!("local:{last_user_text}"))],
            usage: CanonicalUsage {
                input_tokens: compilation.retained_text_bytes as u64,
                output_tokens: last_user_text.len() as u64,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

#[derive(Clone, Debug)]
pub struct OpenAIResponsesAdapter {
    pub include_encrypted_reasoning: bool,
    pub reasoning_summary: OpenAIReasoningSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenAIReasoningSummary {
    Auto,
    Concise,
    Detailed,
    Other(String),
}

impl OpenAIReasoningSummary {
    fn as_wire(&self) -> &str {
        match self {
            Self::Auto => "auto",
            Self::Concise => "concise",
            Self::Detailed => "detailed",
            Self::Other(value) => value.as_str(),
        }
    }
}

impl Default for OpenAIResponsesAdapter {
    fn default() -> Self {
        Self {
            include_encrypted_reasoning: true,
            reasoning_summary: OpenAIReasoningSummary::Auto,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct OpenAIChatCompletionsAdapter;

#[derive(Clone, Debug, Default)]
pub struct AnthropicMessagesAdapter;

#[derive(Clone, Debug, Default)]
pub struct AnthropicBedrockMessagesAdapter;

impl OpenAIResponsesAdapter {
    pub fn decode_sse_events(&self, sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        decode_openai_responses_sse(sse)
    }
}

impl AnthropicMessagesAdapter {
    pub fn decode_sse_events(&self, sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        decode_anthropic_sse(sse)
    }
}

impl AnthropicBedrockMessagesAdapter {
    pub fn decode_sse_events(&self, sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        decode_anthropic_sse(sse)
    }
}

impl OpenAIChatCompletionsAdapter {
    pub fn decode_sse_events(&self, sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        decode_openai_chat_sse(sse)
    }
}

impl ProviderWireAdapter for OpenAIResponsesAdapter {
    fn api(&self) -> ProviderApi {
        ProviderApi::OpenAIResponses
    }

    fn build_request_body(&self, request: &ProviderRequest) -> ProviderResult<Value> {
        self.capabilities()
            .validate_request(request, ProviderRequestMode::Complete)?;
        ensure_api(
            "openai_responses",
            &request.api,
            ProviderApi::OpenAIResponses,
        )?;
        let mut body = json!({
            "model": request.model,
            "store": false,
            "stream": false,
            "input": build_openai_responses_input(&request.messages),
            "max_output_tokens": request.max_tokens,
        });
        if let Some(instructions) = joined_system(&request.system) {
            body["instructions"] = json!(instructions);
        }
        if let Some(temperature) = request.temperature {
            body["temperature"] = json!(temperature);
        }
        if let Some(reasoning) = openai_reasoning(
            &request.provider,
            &request.thinking,
            &self.reasoning_summary,
        )? {
            body["reasoning"] = reasoning;
        }
        if self.include_encrypted_reasoning {
            body["include"] = json!(["reasoning.encrypted_content"]);
        }
        let tools = build_openai_responses_tools(&request.tools);
        if !tools.is_empty() {
            body["tools"] = Value::Array(tools);
            body["tool_choice"] = json!("auto");
            body["parallel_tool_calls"] = json!(true);
        }
        Ok(body)
    }

    fn decode_response_body(&self, body: &Value) -> ProviderResult<ProviderResponse> {
        let mut content = Vec::new();
        if let Some(output) = body.get("output").and_then(Value::as_array) {
            for (output_index, item) in output.iter().enumerate() {
                decode_openai_responses_output_item(item, output_index, &mut content)?;
            }
        }
        if content.is_empty() {
            if let Some(text) = body.get("output_text").and_then(Value::as_str) {
                if !text.is_empty() {
                    content.push(CanonicalContent::text(text));
                }
            }
        }
        let has_tool_use = content
            .iter()
            .any(|content| matches!(content, CanonicalContent::ToolCall { .. }));
        Ok(ProviderResponse {
            content,
            usage: openai_responses_usage(body.get("usage")),
            stop_reason: if has_tool_use {
                CanonicalStopReason::ToolUse
            } else if body.get("status").and_then(Value::as_str) == Some("incomplete") {
                CanonicalStopReason::MaxTokens
            } else {
                CanonicalStopReason::EndTurn
            },
        })
    }

    fn decode_stream_events(&self, sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        self.decode_sse_events(sse)
    }
}

impl ProviderWireAdapter for OpenAIChatCompletionsAdapter {
    fn api(&self) -> ProviderApi {
        ProviderApi::OpenAIChatCompletions
    }

    fn build_request_body(&self, request: &ProviderRequest) -> ProviderResult<Value> {
        self.capabilities()
            .validate_request(request, ProviderRequestMode::Complete)?;
        ensure_api(
            "openai_chat_completions",
            &request.api,
            ProviderApi::OpenAIChatCompletions,
        )?;
        let mut body = json!({
            "model": request.model,
            "messages": build_chat_messages(&request.system, &request.messages),
            "max_tokens": request.max_tokens,
            "stream": false,
        });
        if let Some(temperature) = request.temperature {
            body["temperature"] = json!(temperature);
        }
        if let Some(thinking) = openai_chat_thinking(&request.provider, &request.thinking)? {
            for (key, value) in thinking {
                body[key] = value;
            }
        }
        let tools = build_chat_tools(&request.tools);
        if !tools.is_empty() {
            body["tools"] = Value::Array(tools);
            body["tool_choice"] = json!("auto");
        }
        Ok(body)
    }

    fn decode_response_body(&self, body: &Value) -> ProviderResult<ProviderResponse> {
        let choice = body
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .ok_or_else(|| ProviderError::Decode("chat response had no choices".to_string()))?;
        let message = choice
            .get("message")
            .ok_or_else(|| ProviderError::Decode("chat choice had no message".to_string()))?;
        let mut content = Vec::new();
        if let Some(text) = message.get("reasoning_content").and_then(Value::as_str) {
            if !text.is_empty() {
                content.push(CanonicalContent::Thinking {
                    text: text.to_string(),
                    provider: ThinkingProvider::OpenAICompatible,
                    metadata: ThinkingMetadata::None,
                });
            }
        }
        if let Some(text) = message.get("content").and_then(Value::as_str) {
            if !text.is_empty() {
                content.push(CanonicalContent::text(text));
            }
        }
        for tool_call in message
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let id = tool_call
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let function = tool_call.get("function").unwrap_or(&Value::Null);
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let arguments = function
                .get("arguments")
                .and_then(Value::as_str)
                .map(|raw| parse_tool_arguments(raw, "chat tool call arguments"))
                .transpose()?
                .unwrap_or_else(|| json!({}));
            content.push(CanonicalContent::tool_call(id, name, arguments));
        }
        let has_tool_use = content
            .iter()
            .any(|content| matches!(content, CanonicalContent::ToolCall { .. }));
        Ok(ProviderResponse {
            content,
            usage: chat_usage(body.get("usage")),
            stop_reason: chat_stop_reason(
                choice.get("finish_reason").and_then(Value::as_str),
                has_tool_use,
            ),
        })
    }

    fn decode_stream_events(&self, sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        decode_openai_chat_sse(sse)
    }
}

impl ProviderWireAdapter for AnthropicMessagesAdapter {
    fn api(&self) -> ProviderApi {
        ProviderApi::AnthropicMessages
    }

    fn build_request_body(&self, request: &ProviderRequest) -> ProviderResult<Value> {
        self.capabilities()
            .validate_request(request, ProviderRequestMode::Complete)?;
        ensure_api(
            "anthropic_messages",
            &request.api,
            ProviderApi::AnthropicMessages,
        )?;
        Ok(build_anthropic_messages_body(
            request,
            AnthropicRequestFlavor::Native,
        ))
    }

    fn decode_response_body(&self, body: &Value) -> ProviderResult<ProviderResponse> {
        let mut content = Vec::new();
        for block in body
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(content_block) = decode_anthropic_content(block) {
                content.push(content_block);
            }
        }
        Ok(ProviderResponse {
            content,
            usage: anthropic_usage(body.get("usage")),
            stop_reason: anthropic_stop_reason(body.get("stop_reason").and_then(Value::as_str)),
        })
    }

    fn decode_stream_events(&self, sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        self.decode_sse_events(sse)
    }
}

impl ProviderWireAdapter for AnthropicBedrockMessagesAdapter {
    fn api(&self) -> ProviderApi {
        ProviderApi::AnthropicMessages
    }

    fn capabilities(&self) -> ProviderCapabilityRecord {
        let mut capabilities = ProviderCapabilityRecord::for_api(ProviderApi::AnthropicMessages);
        capabilities.provider_family = "anthropic_bedrock_messages".to_string();
        capabilities
    }

    fn build_request_body(&self, request: &ProviderRequest) -> ProviderResult<Value> {
        self.capabilities()
            .validate_request(request, ProviderRequestMode::Complete)?;
        ensure_api(
            "anthropic_bedrock_messages",
            &request.api,
            ProviderApi::AnthropicMessages,
        )?;
        Ok(build_anthropic_messages_body(
            request,
            AnthropicRequestFlavor::Bedrock,
        ))
    }

    fn build_stream_request_body(&self, request: &ProviderRequest) -> ProviderResult<Value> {
        self.capabilities()
            .validate_request(request, ProviderRequestMode::Stream)?;
        ensure_api(
            "anthropic_bedrock_messages",
            &request.api,
            ProviderApi::AnthropicMessages,
        )?;
        Ok(build_anthropic_messages_body(
            request,
            AnthropicRequestFlavor::Bedrock,
        ))
    }

    fn stream_endpoint_url(&self, endpoint_url: &str) -> String {
        bedrock_response_stream_endpoint_url(endpoint_url)
    }

    fn stream_request_headers(&self) -> Vec<(&'static str, &'static str)> {
        vec![("x-amzn-bedrock-accept", "application/json")]
    }

    fn decode_response_body(&self, body: &Value) -> ProviderResult<ProviderResponse> {
        AnthropicMessagesAdapter.decode_response_body(body)
    }

    fn decode_stream_events(&self, sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
        self.decode_sse_events(sse)
    }

    fn decode_stream_response(&self, body: &[u8]) -> ProviderResult<Vec<ProviderStreamEvent>> {
        decode_bedrock_anthropic_eventstream(body)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AnthropicRequestFlavor {
    Native,
    Bedrock,
}

fn build_anthropic_messages_body(
    request: &ProviderRequest,
    flavor: AnthropicRequestFlavor,
) -> Value {
    let mut body = json!({
        "max_tokens": request.max_tokens,
        "messages": build_anthropic_messages(&request.messages),
    });
    match flavor {
        AnthropicRequestFlavor::Native => {
            body["model"] = json!(request.model);
        }
        AnthropicRequestFlavor::Bedrock => {
            body["anthropic_version"] = json!(BEDROCK_ANTHROPIC_VERSION);
        }
    }
    if !request.system.is_empty() {
        body["system"] = Value::Array(
            request
                .system
                .iter()
                .map(|block| {
                    let mut value = json!({"type": "text", "text": block.text});
                    if let Some(cache_control) = &block.cache_control {
                        value["cache_control"] = json!(cache_control);
                    }
                    value
                })
                .collect(),
        );
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    let mut value = json!({
                        "name": tool.name,
                        "description": tool.description,
                        "input_schema": tool.input_schema,
                    });
                    if let Some(cache_control) = &tool.cache_control {
                        value["cache_control"] = json!(cache_control);
                    }
                    value
                })
                .collect(),
        );
    }
    if let Some(thinking) = anthropic_thinking(&request.thinking) {
        if let Some(output_config) = thinking.get("output_config") {
            body["output_config"] = output_config.clone();
        }
        body["thinking"] = thinking["thinking"].clone();
    } else if let Some(temperature) = request.temperature {
        body["temperature"] = json!(temperature);
    }
    body
}

fn ensure_api(
    adapter: &'static str,
    actual: &ProviderApi,
    expected: ProviderApi,
) -> ProviderResult<()> {
    if *actual == expected {
        Ok(())
    } else {
        Err(ProviderError::ApiMismatch {
            adapter,
            api: actual.clone(),
        })
    }
}

fn joined_system(system: &[SystemBlock]) -> Option<String> {
    let joined = system
        .iter()
        .map(|block| block.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    (!joined.is_empty()).then_some(joined)
}

fn text_from_content(content: &[CanonicalContent], separator: &str) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(separator)
}

fn text_from_tool_result_content(content: &[CanonicalContent]) -> String {
    text_from_content(content, "\n")
}

fn build_openai_responses_tools(tools: &[ToolDefinition]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
                "strict": null,
            })
        })
        .collect()
}

fn build_openai_responses_input(messages: &[CanonicalMessage]) -> Vec<Value> {
    let mut input = Vec::new();
    for message in messages {
        match message {
            CanonicalMessage::User { content, .. } => {
                let text = text_from_content(content, "\n");
                if !text.is_empty() {
                    input.push(json!({"role": "user", "content": text}));
                }
                for image in content.iter().filter_map(image_data_url) {
                    input.push(json!({
                        "role": "user",
                        "content": [{"type": "input_image", "image_url": image}],
                    }));
                }
            }
            CanonicalMessage::Assistant { content, .. } => {
                let mut text = String::new();
                for block in content {
                    match block {
                        CanonicalContent::Text { text: chunk, .. } => text.push_str(chunk),
                        CanonicalContent::Thinking {
                            text: thinking,
                            provider: ThinkingProvider::OpenAIResponses,
                            metadata:
                                ThinkingMetadata::OpenAIResponses {
                                    item_id,
                                    encrypted_content,
                                    ..
                                },
                        } => {
                            flush_openai_responses_text(&mut input, "assistant", &mut text);
                            let mut item = json!({
                                "type": "reasoning",
                                "summary": [{"type": "summary_text", "text": thinking}],
                            });
                            if let Some(item_id) = item_id {
                                item["id"] = json!(item_id);
                            }
                            if let Some(encrypted_content) = encrypted_content {
                                item["encrypted_content"] = json!(encrypted_content);
                            }
                            input.push(item);
                        }
                        CanonicalContent::ToolCall {
                            id,
                            name,
                            arguments,
                        } => {
                            flush_openai_responses_text(&mut input, "assistant", &mut text);
                            let (call_id, item_id) = split_tool_call_id(id);
                            let mut item = json!({
                                "type": "function_call",
                                "call_id": call_id,
                                "name": name,
                                "arguments": arguments.to_string(),
                            });
                            if let Some(item_id) = item_id {
                                item["id"] = json!(item_id);
                            }
                            input.push(item);
                        }
                        CanonicalContent::Thinking { .. } | CanonicalContent::Image { .. } => {}
                    }
                }
                flush_openai_responses_text(&mut input, "assistant", &mut text);
            }
            CanonicalMessage::ToolResult {
                tool_call_id,
                content,
                is_error,
                ..
            } => {
                let output = text_from_tool_result_content(content);
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": split_tool_call_id(tool_call_id).0,
                    "output": if *is_error { format!("[error] {output}") } else { output },
                }));
            }
        }
    }
    input
}

fn flush_openai_responses_text(input: &mut Vec<Value>, role: &str, text: &mut String) {
    if !text.is_empty() {
        input.push(json!({"role": role, "content": std::mem::take(text)}));
    }
}

fn split_tool_call_id(id: &str) -> (&str, Option<&str>) {
    id.split_once('|').map_or((id, None), |(call_id, item_id)| {
        (call_id, (!item_id.is_empty()).then_some(item_id))
    })
}

fn combined_tool_call_id(call_id: &str, item_id: Option<&str>) -> String {
    match item_id {
        Some(item_id) if !item_id.is_empty() => format!("{call_id}|{item_id}"),
        _ => call_id.to_string(),
    }
}

fn openai_reasoning(
    provider: &str,
    thinking: &Option<ThinkingConfig>,
    summary: &OpenAIReasoningSummary,
) -> ProviderResult<Option<Value>> {
    match thinking {
        Some(ThinkingConfig::Disabled) | None => Ok(None),
        Some(ThinkingConfig::Budget { .. }) => Err(ProviderError::UnsupportedCapability {
            provider: provider.to_string(),
            api: ProviderApi::OpenAIResponses,
            capability: "thinking_budget",
            detail: "budget-based thinking does not map to OpenAI Responses reasoning; \
                     configure effort-based thinking for this provider"
                .to_string(),
        }),
        Some(ThinkingConfig::Effort { effort }) => Ok(Some(json!({
            "effort": effort.as_openai_wire(),
            "summary": summary.as_wire(),
        }))),
    }
}

fn openai_chat_thinking(
    provider: &str,
    thinking: &Option<ThinkingConfig>,
) -> ProviderResult<Option<Vec<(&'static str, Value)>>> {
    let zhipu_convention = ZHIPU_CONVENTION_CHAT_PROVIDERS.contains(&provider);
    match thinking {
        None => Ok(None),
        Some(ThinkingConfig::Disabled) => {
            if zhipu_convention {
                Ok(Some(vec![("thinking", json!({"type": "disabled"}))]))
            } else {
                Ok(None)
            }
        }
        Some(ThinkingConfig::Budget { .. }) => Err(ProviderError::UnsupportedCapability {
            provider: provider.to_string(),
            api: ProviderApi::OpenAIChatCompletions,
            capability: "thinking_budget",
            detail: "budget-based thinking does not map to OpenAI Chat Completions reasoning; \
                     configure effort-based thinking for this provider"
                .to_string(),
        }),
        Some(ThinkingConfig::Effort { effort }) => match effort {
            ThinkingEffort::Low | ThinkingEffort::Medium | ThinkingEffort::High => {
                let mut values = vec![("reasoning_effort", json!(effort.as_openai_wire()))];
                if zhipu_convention {
                    values.push(("thinking", json!({"type": "enabled"})));
                }
                Ok(Some(values))
            }
            ThinkingEffort::XHigh | ThinkingEffort::Max | ThinkingEffort::Other(_) => {
                Err(ProviderError::UnsupportedCapability {
                    provider: provider.to_string(),
                    api: ProviderApi::OpenAIChatCompletions,
                    capability: "thinking_effort",
                    detail: format!(
                        "OpenAI Chat Completions reasoning_effort supports low, medium, or high; got {}",
                        effort.as_openai_wire()
                    ),
                })
            }
        },
    }
}

fn decode_openai_responses_output_item(
    item: &Value,
    output_index: usize,
    content: &mut Vec<CanonicalContent>,
) -> ProviderResult<()> {
    match item.get("type").and_then(Value::as_str) {
        Some("message") => {
            for part in item
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(text) = part
                    .get("text")
                    .or_else(|| part.get("refusal"))
                    .and_then(Value::as_str)
                {
                    if !text.is_empty() {
                        content.push(CanonicalContent::text(text));
                    }
                }
            }
        }
        Some("function_call") => {
            let call_id = item
                .get("call_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let item_id = item.get("id").and_then(Value::as_str);
            let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
            let arguments = item
                .get("arguments")
                .and_then(Value::as_str)
                .map(|raw| parse_tool_arguments(raw, "OpenAI Responses function arguments"))
                .transpose()?
                .unwrap_or_else(|| json!({}));
            content.push(CanonicalContent::tool_call(
                combined_tool_call_id(call_id, item_id),
                name,
                arguments,
            ));
        }
        Some("reasoning") => {
            let item_id = item.get("id").and_then(Value::as_str).map(str::to_string);
            let encrypted_content = item
                .get("encrypted_content")
                .and_then(Value::as_str)
                .map(str::to_string);
            for (summary_index, part) in item
                .get("summary")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
            {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    content.push(CanonicalContent::Thinking {
                        text: text.to_string(),
                        provider: ThinkingProvider::OpenAIResponses,
                        metadata: ThinkingMetadata::OpenAIResponses {
                            item_id: item_id.clone(),
                            output_index: Some(output_index),
                            summary_index,
                            encrypted_content: encrypted_content.clone(),
                        },
                    });
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn parse_tool_arguments(raw: &str, context: &str) -> ProviderResult<Value> {
    serde_json::from_str(raw)
        .map_err(|err| ProviderError::Decode(format!("invalid {context}: {err}")))
}

fn openai_responses_usage(value: Option<&Value>) -> CanonicalUsage {
    let Some(value) = value else {
        return CanonicalUsage::default();
    };
    CanonicalUsage {
        input_tokens: value
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: value
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: value
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

#[derive(Debug)]
struct SseEvent {
    event: Option<String>,
    data: String,
}

fn parse_sse(sse: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut event_name = None;
    let mut data = Vec::new();

    for line in sse.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            if !data.is_empty() || event_name.is_some() {
                events.push(SseEvent {
                    event: event_name.take(),
                    data: data.join("\n"),
                });
                data.clear();
            }
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event_name = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data.push(rest.trim_start().to_string());
        }
    }

    if !data.is_empty() || event_name.is_some() {
        events.push(SseEvent {
            event: event_name,
            data: data.join("\n"),
        });
    }

    events
}

fn decode_openai_responses_sse(sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
    let mut out = Vec::new();
    for event in parse_sse(sse) {
        if event.data == "[DONE]" {
            if !out
                .iter()
                .any(|event| matches!(event, ProviderStreamEvent::Done { .. }))
            {
                out.push(ProviderStreamEvent::Done {
                    stop_reason: CanonicalStopReason::EndTurn,
                });
            }
            continue;
        }
        let value: Value = serde_json::from_str(&event.data)
            .map_err(|err| ProviderError::Decode(format!("invalid OpenAI SSE JSON: {err}")))?;
        let kind = event
            .event
            .as_deref()
            .or_else(|| value.get("type").and_then(Value::as_str))
            .unwrap_or_default();
        match kind {
            "response.output_text.delta" => {
                if let Some(delta) = value.get("delta").and_then(Value::as_str) {
                    out.push(ProviderStreamEvent::TextDelta {
                        text: delta.to_string(),
                    });
                }
            }
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                if let Some(delta) = value.get("delta").and_then(Value::as_str) {
                    out.push(ProviderStreamEvent::ThinkingDelta {
                        text: delta.to_string(),
                    });
                }
            }
            "response.function_call_arguments.delta" => {
                if let Some(delta) = value.get("delta").and_then(Value::as_str) {
                    let item_id = value.get("item_id").and_then(Value::as_str);
                    let call_id = value
                        .get("call_id")
                        .and_then(Value::as_str)
                        .or(item_id)
                        .unwrap_or_default();
                    out.push(ProviderStreamEvent::ToolCallDelta {
                        id: combined_tool_call_id(call_id, item_id.filter(|id| *id != call_id)),
                        name: None,
                        arguments_delta: delta.to_string(),
                    });
                }
            }
            "response.output_item.done" => {
                let item = value.get("item").unwrap_or(&value);
                let mut content = Vec::new();
                decode_openai_responses_output_item(item, 0, &mut content)?;
                out.extend(
                    content
                        .into_iter()
                        .map(|content| ProviderStreamEvent::Content { content }),
                );
            }
            "response.completed" | "response.incomplete" => {
                let response = value.get("response").unwrap_or(&value);
                let decoded = OpenAIResponsesAdapter::default().decode_response_body(response)?;
                if decoded.usage != CanonicalUsage::default() {
                    out.push(ProviderStreamEvent::Usage {
                        usage: decoded.usage,
                    });
                }
                out.push(ProviderStreamEvent::Done {
                    stop_reason: decoded.stop_reason,
                });
            }
            "response.failed" => out.push(ProviderStreamEvent::Error {
                message: value
                    .pointer("/response/error/message")
                    .or_else(|| value.pointer("/error/message"))
                    .and_then(Value::as_str)
                    .unwrap_or("OpenAI Responses stream failed")
                    .to_string(),
            }),
            _ => {}
        }
    }
    Ok(out)
}

fn build_chat_tools(tools: &[ToolDefinition]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                }
            })
        })
        .collect()
}

fn build_chat_messages(system: &[SystemBlock], messages: &[CanonicalMessage]) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(system) = joined_system(system) {
        out.push(json!({"role": "system", "content": system}));
    }
    for message in messages {
        match message {
            CanonicalMessage::User { content, .. } => {
                let text = text_from_content(content, "\n");
                if !text.is_empty() {
                    out.push(json!({"role": "user", "content": text}));
                }
            }
            CanonicalMessage::Assistant { content, .. } => {
                let mut text = String::new();
                let mut tool_calls = Vec::new();
                for block in content {
                    match block {
                        CanonicalContent::Text { text: chunk, .. } => text.push_str(chunk),
                        CanonicalContent::ToolCall {
                            id,
                            name,
                            arguments,
                        } => tool_calls.push(json!({
                            "id": id,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": arguments.to_string(),
                            }
                        })),
                        // Chat-completions reasoning_content is output-only provider state;
                        // provider docs exclude prior reasoning from later requests.
                        CanonicalContent::Image { .. } | CanonicalContent::Thinking { .. } => {}
                    }
                }
                if !text.is_empty() || !tool_calls.is_empty() {
                    let mut message = json!({"role": "assistant"});
                    if !text.is_empty() {
                        message["content"] = json!(text);
                    }
                    if !tool_calls.is_empty() {
                        message["tool_calls"] = Value::Array(tool_calls);
                    }
                    out.push(message);
                }
            }
            CanonicalMessage::ToolResult {
                tool_call_id,
                content,
                is_error,
                ..
            } => {
                let text = text_from_tool_result_content(content);
                out.push(json!({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": if *is_error { format!("[error] {text}") } else { text },
                }));
            }
        }
    }
    out
}

fn chat_usage(value: Option<&Value>) -> CanonicalUsage {
    let Some(value) = value else {
        return CanonicalUsage::default();
    };
    CanonicalUsage {
        input_tokens: value
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: value
            .get("completion_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    }
}

fn chat_stop_reason(finish_reason: Option<&str>, has_tool_use: bool) -> CanonicalStopReason {
    match finish_reason {
        Some("tool_calls") | Some("function_call") => CanonicalStopReason::ToolUse,
        Some("length") => CanonicalStopReason::MaxTokens,
        Some("stop_sequence") => CanonicalStopReason::StopSequence,
        _ if has_tool_use => CanonicalStopReason::ToolUse,
        _ => CanonicalStopReason::EndTurn,
    }
}

fn decode_openai_chat_sse(sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
    let mut out = Vec::new();
    let mut tool_call_ids = std::collections::BTreeMap::<u64, String>::new();
    for event in parse_sse(sse) {
        if event.data == "[DONE]" {
            if !out
                .iter()
                .any(|event| matches!(event, ProviderStreamEvent::Done { .. }))
            {
                out.push(ProviderStreamEvent::Done {
                    stop_reason: CanonicalStopReason::EndTurn,
                });
            }
            continue;
        }
        let value: Value = serde_json::from_str(&event.data)
            .map_err(|err| ProviderError::Decode(format!("invalid chat SSE JSON: {err}")))?;
        if let Some(usage) = value.get("usage") {
            let usage = chat_usage(Some(usage));
            if usage != CanonicalUsage::default() {
                out.push(ProviderStreamEvent::Usage { usage });
            }
        }
        for choice in value
            .get("choices")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(delta) = choice
                .pointer("/delta/reasoning_content")
                .and_then(Value::as_str)
            {
                if !delta.is_empty() {
                    out.push(ProviderStreamEvent::ThinkingDelta {
                        text: delta.to_string(),
                    });
                }
            }
            if let Some(delta) = choice.pointer("/delta/content").and_then(Value::as_str) {
                if !delta.is_empty() {
                    out.push(ProviderStreamEvent::TextDelta {
                        text: delta.to_string(),
                    });
                }
            }
            for tool_call in choice
                .pointer("/delta/tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let index = tool_call.get("index").and_then(Value::as_u64).unwrap_or(0);
                let id = tool_call
                    .get("id")
                    .and_then(Value::as_str)
                    .map(|id| {
                        tool_call_ids.insert(index, id.to_string());
                        id.to_string()
                    })
                    .or_else(|| tool_call_ids.get(&index).cloned())
                    .unwrap_or_else(|| format!("tool_call_index_{index}"));
                let name = tool_call
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let arguments_delta = tool_call
                    .pointer("/function/arguments")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if name.is_some() || !arguments_delta.is_empty() {
                    out.push(ProviderStreamEvent::ToolCallDelta {
                        id,
                        name,
                        arguments_delta,
                    });
                }
            }
            if let Some(finish_reason) = choice.get("finish_reason").and_then(Value::as_str) {
                out.push(ProviderStreamEvent::Done {
                    stop_reason: chat_stop_reason(Some(finish_reason), false),
                });
            }
        }
    }
    Ok(out)
}

fn build_anthropic_messages(messages: &[CanonicalMessage]) -> Vec<Value> {
    messages
        .iter()
        .filter_map(|message| {
            let (role, content) = match message {
                CanonicalMessage::User { content, .. } => (
                    "user",
                    content.iter().filter_map(anthropic_content).collect(),
                ),
                CanonicalMessage::Assistant { content, .. } => (
                    "assistant",
                    content.iter().filter_map(anthropic_content).collect(),
                ),
                CanonicalMessage::ToolResult {
                    tool_call_id,
                    content,
                    is_error,
                    cache_control,
                    ..
                } => {
                    let mut block = json!({
                        "type": "tool_result",
                        "tool_use_id": anthropic_tool_id(tool_call_id),
                        "content": text_from_tool_result_content(content),
                    });
                    if *is_error {
                        block["is_error"] = json!(true);
                    }
                    if let Some(cache_control) = cache_control {
                        block["cache_control"] = json!(cache_control);
                    }
                    ("user", vec![block])
                }
            };
            (!content.is_empty()).then(|| json!({"role": role, "content": content}))
        })
        .collect()
}

fn anthropic_content(content: &CanonicalContent) -> Option<Value> {
    match content {
        CanonicalContent::Text {
            text,
            cache_control,
        } => {
            let mut value = json!({"type": "text", "text": text});
            if let Some(cache_control) = cache_control {
                value["cache_control"] = json!(cache_control);
            }
            Some(value)
        }
        CanonicalContent::Image { data, mime_type } => Some(json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": mime_type,
                "data": data,
            }
        })),
        CanonicalContent::Thinking {
            text,
            provider: ThinkingProvider::Anthropic,
            metadata: ThinkingMetadata::Anthropic { signature },
        } => {
            let mut value = json!({"type": "thinking", "thinking": text});
            if let Some(signature) = signature {
                value["signature"] = json!(signature);
            }
            Some(value)
        }
        CanonicalContent::Thinking {
            provider: ThinkingProvider::Anthropic,
            metadata: ThinkingMetadata::AnthropicRedacted { data },
            ..
        } => Some(json!({"type": "redacted_thinking", "data": data})),
        CanonicalContent::ToolCall {
            id,
            name,
            arguments,
        } => Some(json!({
            "type": "tool_use",
            "id": anthropic_tool_id(id),
            "name": name,
            "input": arguments,
        })),
        CanonicalContent::Thinking { .. } => None,
    }
}

fn anthropic_tool_id(id: &str) -> String {
    let sanitized = id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    let sanitized = if sanitized.is_empty() {
        "toolu_empty".to_string()
    } else {
        sanitized
    };
    if sanitized.len() <= 64 {
        return sanitized;
    }

    let mut hash = 0xcbf29ce484222325_u64;
    for byte in id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("toolu_{hash:016x}")
}

fn anthropic_thinking(thinking: &Option<ThinkingConfig>) -> Option<Value> {
    match thinking {
        Some(ThinkingConfig::Disabled) | None => None,
        Some(ThinkingConfig::Budget { budget_tokens }) => Some(json!({
            "thinking": {
                "type": "enabled",
                "budget_tokens": budget_tokens,
            }
        })),
        Some(ThinkingConfig::Effort { effort }) => {
            let mut value = json!({
                "thinking": {
                    "type": "adaptive",
                    "display": "summarized",
                }
            });
            value["output_config"] = json!({"effort": effort.as_anthropic_wire()});
            Some(value)
        }
    }
}

fn decode_anthropic_content(block: &Value) -> Option<CanonicalContent> {
    match block.get("type").and_then(Value::as_str) {
        Some("text") => Some(CanonicalContent::text(
            block
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        )),
        Some("thinking") => Some(CanonicalContent::Thinking {
            text: block
                .get("thinking")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            provider: ThinkingProvider::Anthropic,
            metadata: ThinkingMetadata::Anthropic {
                signature: block
                    .get("signature")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            },
        }),
        Some("redacted_thinking") => Some(CanonicalContent::Thinking {
            text: String::new(),
            provider: ThinkingProvider::Anthropic,
            metadata: ThinkingMetadata::AnthropicRedacted {
                data: block
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            },
        }),
        Some("tool_use") => Some(CanonicalContent::tool_call(
            block.get("id").and_then(Value::as_str).unwrap_or_default(),
            block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            block.get("input").cloned().unwrap_or_else(|| json!({})),
        )),
        _ => None,
    }
}

fn anthropic_usage(value: Option<&Value>) -> CanonicalUsage {
    let Some(value) = value else {
        return CanonicalUsage::default();
    };
    CanonicalUsage {
        input_tokens: value
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: value
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_input_tokens: value
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_read_input_tokens: value
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

fn anthropic_stop_reason(reason: Option<&str>) -> CanonicalStopReason {
    match reason {
        Some("tool_use") => CanonicalStopReason::ToolUse,
        Some("max_tokens") => CanonicalStopReason::MaxTokens,
        Some("stop_sequence") => CanonicalStopReason::StopSequence,
        Some("pause_turn") => CanonicalStopReason::PauseTurn,
        _ => CanonicalStopReason::EndTurn,
    }
}

fn decode_anthropic_sse(sse: &str) -> ProviderResult<Vec<ProviderStreamEvent>> {
    let mut out = Vec::new();
    let mut pending_stop_reason = None;
    let mut tool_blocks = std::collections::BTreeMap::<u64, (String, Option<String>)>::new();

    for event in parse_sse(sse) {
        let value: Value = serde_json::from_str(&event.data)
            .map_err(|err| ProviderError::Decode(format!("invalid Anthropic SSE JSON: {err}")))?;
        let kind = event
            .event
            .as_deref()
            .or_else(|| value.get("type").and_then(Value::as_str))
            .unwrap_or_default();
        match kind {
            "message_start" => {
                let usage = anthropic_usage(value.pointer("/message/usage"));
                if usage != CanonicalUsage::default() {
                    out.push(ProviderStreamEvent::Usage { usage });
                }
            }
            "content_block_start" => {
                if value.pointer("/content_block/type").and_then(Value::as_str) == Some("tool_use")
                {
                    let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
                    let id = value
                        .pointer("/content_block/id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let name = value
                        .pointer("/content_block/name")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    tool_blocks.insert(index, (id.clone(), name.clone()));
                    out.push(ProviderStreamEvent::ToolCallDelta {
                        id,
                        name,
                        arguments_delta: String::new(),
                    });
                }
            }
            "content_block_delta" => match value.pointer("/delta/type").and_then(Value::as_str) {
                Some("text_delta") => {
                    if let Some(text) = value.pointer("/delta/text").and_then(Value::as_str) {
                        out.push(ProviderStreamEvent::TextDelta {
                            text: text.to_string(),
                        });
                    }
                }
                Some("thinking_delta") => {
                    if let Some(text) = value.pointer("/delta/thinking").and_then(Value::as_str) {
                        out.push(ProviderStreamEvent::ThinkingDelta {
                            text: text.to_string(),
                        });
                    }
                }
                Some("input_json_delta") => {
                    let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
                    let (id, name) = tool_blocks
                        .get(&index)
                        .cloned()
                        .unwrap_or_else(|| (format!("toolu_index_{index}"), None));
                    if let Some(partial_json) =
                        value.pointer("/delta/partial_json").and_then(Value::as_str)
                    {
                        out.push(ProviderStreamEvent::ToolCallDelta {
                            id,
                            name,
                            arguments_delta: partial_json.to_string(),
                        });
                    }
                }
                _ => {}
            },
            "message_delta" => {
                pending_stop_reason = Some(anthropic_stop_reason(
                    value.pointer("/delta/stop_reason").and_then(Value::as_str),
                ));
                let usage = anthropic_usage(value.get("usage"));
                if usage != CanonicalUsage::default() {
                    out.push(ProviderStreamEvent::Usage { usage });
                }
            }
            "message_stop" => out.push(ProviderStreamEvent::Done {
                stop_reason: pending_stop_reason.unwrap_or(CanonicalStopReason::EndTurn),
            }),
            "error" => out.push(ProviderStreamEvent::Error {
                message: value
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("Anthropic stream failed")
                    .to_string(),
            }),
            _ => {}
        }
    }

    Ok(out)
}

fn bedrock_response_stream_endpoint_url(endpoint_url: &str) -> String {
    endpoint_url
        .strip_suffix("/invoke")
        .map(|prefix| format!("{prefix}/invoke-with-response-stream"))
        .unwrap_or_else(|| endpoint_url.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AwsEventStreamFrame {
    headers: BTreeMap<String, AwsEventStreamHeaderValue>,
    payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AwsEventStreamHeaderValue {
    Bool(bool),
    Byte(i8),
    Short(i16),
    Integer(i32),
    Long(i64),
    Bytes(Vec<u8>),
    String(String),
    Timestamp(i64),
    Uuid([u8; 16]),
}

impl AwsEventStreamHeaderValue {
    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Default)]
struct AwsEventStreamDecoder {
    buffer: Vec<u8>,
}

impl AwsEventStreamDecoder {
    fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, chunk: &[u8]) -> ProviderResult<Vec<AwsEventStreamFrame>> {
        self.buffer.extend_from_slice(chunk);
        let mut frames = Vec::new();

        loop {
            if self.buffer.len() < 12 {
                break;
            }

            let total_length = read_be_u32(&self.buffer[0..4])? as usize;
            let headers_length = read_be_u32(&self.buffer[4..8])? as usize;
            if total_length < 16 {
                return Err(ProviderError::Decode(format!(
                    "AWS eventstream frame total length {total_length} is smaller than the 16-byte frame overhead"
                )));
            }
            if headers_length > total_length.saturating_sub(16) {
                return Err(ProviderError::Decode(format!(
                    "AWS eventstream headers length {headers_length} exceeds frame payload budget"
                )));
            }

            let expected_prelude_crc = read_be_u32(&self.buffer[8..12])?;
            let actual_prelude_crc = crc32(&self.buffer[0..8]);
            if expected_prelude_crc != actual_prelude_crc {
                return Err(ProviderError::Decode(format!(
                    "AWS eventstream prelude CRC mismatch: expected {expected_prelude_crc:#010x}, got {actual_prelude_crc:#010x}"
                )));
            }

            if self.buffer.len() < total_length {
                break;
            }

            let expected_message_crc = read_be_u32(&self.buffer[total_length - 4..total_length])?;
            let actual_message_crc = crc32(&self.buffer[0..total_length - 4]);
            if expected_message_crc != actual_message_crc {
                return Err(ProviderError::Decode(format!(
                    "AWS eventstream message CRC mismatch: expected {expected_message_crc:#010x}, got {actual_message_crc:#010x}"
                )));
            }

            let headers_start = 12;
            let headers_end = headers_start + headers_length;
            let payload_end = total_length - 4;
            let headers = decode_aws_eventstream_headers(&self.buffer[headers_start..headers_end])?;
            let payload = self.buffer[headers_end..payload_end].to_vec();
            frames.push(AwsEventStreamFrame { headers, payload });
            self.buffer.drain(..total_length);
        }

        Ok(frames)
    }

    fn finish(mut self) -> ProviderResult<Vec<AwsEventStreamFrame>> {
        let frames = self.push(&[])?;
        if self.buffer.is_empty() {
            return Ok(frames);
        }
        if self.buffer.len() < 12 {
            return Err(ProviderError::Decode(format!(
                "truncated AWS eventstream prelude: got {} bytes, need 12",
                self.buffer.len()
            )));
        }
        let total_length = read_be_u32(&self.buffer[0..4])? as usize;
        Err(ProviderError::Decode(format!(
            "truncated AWS eventstream frame: got {} bytes, need {total_length}",
            self.buffer.len()
        )))
    }
}

fn decode_aws_eventstream_frames(body: &[u8]) -> ProviderResult<Vec<AwsEventStreamFrame>> {
    let mut decoder = AwsEventStreamDecoder::new();
    let mut frames = decoder.push(body)?;
    frames.extend(decoder.finish()?);
    Ok(frames)
}

fn decode_aws_eventstream_headers(
    mut bytes: &[u8],
) -> ProviderResult<BTreeMap<String, AwsEventStreamHeaderValue>> {
    let mut headers = BTreeMap::new();
    while !bytes.is_empty() {
        let name_length = take_u8(&mut bytes)? as usize;
        if name_length == 0 {
            return Err(ProviderError::Decode(
                "AWS eventstream header name was empty".to_string(),
            ));
        }
        let name = take_bytes(&mut bytes, name_length)?;
        let name = std::str::from_utf8(name)
            .map_err(|err| {
                ProviderError::Decode(format!("AWS eventstream header name was not UTF-8: {err}"))
            })?
            .to_string();
        let value_type = take_u8(&mut bytes)?;
        let value = match value_type {
            0 => AwsEventStreamHeaderValue::Bool(true),
            1 => AwsEventStreamHeaderValue::Bool(false),
            2 => AwsEventStreamHeaderValue::Byte(take_u8(&mut bytes)? as i8),
            3 => AwsEventStreamHeaderValue::Short(i16::from_be_bytes(take_array(&mut bytes)?)),
            4 => AwsEventStreamHeaderValue::Integer(i32::from_be_bytes(take_array(&mut bytes)?)),
            5 => AwsEventStreamHeaderValue::Long(i64::from_be_bytes(take_array(&mut bytes)?)),
            6 => {
                let length = u16::from_be_bytes(take_array(&mut bytes)?) as usize;
                AwsEventStreamHeaderValue::Bytes(take_bytes(&mut bytes, length)?.to_vec())
            }
            7 => {
                let length = u16::from_be_bytes(take_array(&mut bytes)?) as usize;
                let value = take_bytes(&mut bytes, length)?;
                let value = std::str::from_utf8(value)
                    .map_err(|err| {
                        ProviderError::Decode(format!(
                            "AWS eventstream string header {name:?} was not UTF-8: {err}"
                        ))
                    })?
                    .to_string();
                AwsEventStreamHeaderValue::String(value)
            }
            8 => AwsEventStreamHeaderValue::Timestamp(i64::from_be_bytes(take_array(&mut bytes)?)),
            9 => AwsEventStreamHeaderValue::Uuid(take_array(&mut bytes)?),
            other => {
                return Err(ProviderError::Decode(format!(
                    "unknown AWS eventstream header value type {other}"
                )));
            }
        };
        headers.insert(name, value);
    }
    Ok(headers)
}

fn decode_bedrock_anthropic_eventstream(body: &[u8]) -> ProviderResult<Vec<ProviderStreamEvent>> {
    let frames = decode_aws_eventstream_frames(body)?;
    let mut events = Vec::new();
    let mut anthropic_sse = String::new();
    for frame in frames {
        let event_type = frame
            .headers
            .get(":event-type")
            .and_then(AwsEventStreamHeaderValue::as_str);
        let payload: Value = serde_json::from_slice(&frame.payload).map_err(|err| {
            ProviderError::Decode(format!("invalid Bedrock eventstream payload JSON: {err}"))
        })?;
        if let Some(bytes) = payload
            .pointer("/chunk/bytes")
            .or_else(|| payload.get("bytes"))
            .and_then(Value::as_str)
        {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(bytes)
                .map_err(|err| {
                    ProviderError::Decode(format!(
                        "invalid Bedrock chunk.bytes base64 payload: {err}"
                    ))
                })?;
            append_anthropic_sse_event(&mut anthropic_sse, &decoded)?;
            continue;
        }
        if event_type == Some("chunk") {
            append_anthropic_sse_event(&mut anthropic_sse, &frame.payload)?;
            continue;
        }
        if let Some(message) = bedrock_stream_error_message(&payload) {
            flush_anthropic_sse(&mut anthropic_sse, &mut events)?;
            events.push(ProviderStreamEvent::Error { message });
        } else if let Some(event_type) = event_type {
            flush_anthropic_sse(&mut anthropic_sse, &mut events)?;
            events.push(ProviderStreamEvent::Error {
                message: format!(
                    "{event_type}: {}",
                    payload
                        .get("message")
                        .or_else(|| payload.get("Message"))
                        .and_then(Value::as_str)
                        .unwrap_or("Bedrock response stream failed")
                ),
            });
        }
    }
    flush_anthropic_sse(&mut anthropic_sse, &mut events)?;
    Ok(events)
}

fn append_anthropic_sse_event(sse: &mut String, event_json: &[u8]) -> ProviderResult<()> {
    let event_text = std::str::from_utf8(event_json).map_err(|err| {
        ProviderError::Decode(format!(
            "Bedrock Anthropic chunk payload was not UTF-8: {err}"
        ))
    })?;
    let event: Value = serde_json::from_str(event_text).map_err(|err| {
        ProviderError::Decode(format!("invalid Bedrock Anthropic chunk JSON: {err}"))
    })?;
    let kind = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("message");
    sse.push_str("event: ");
    sse.push_str(kind);
    sse.push('\n');
    sse.push_str("data: ");
    sse.push_str(event_text);
    sse.push_str("\n\n");
    Ok(())
}

fn flush_anthropic_sse(
    sse: &mut String,
    events: &mut Vec<ProviderStreamEvent>,
) -> ProviderResult<()> {
    if sse.is_empty() {
        return Ok(());
    }
    events.extend(decode_anthropic_sse(sse)?);
    sse.clear();
    Ok(())
}

fn bedrock_stream_error_message(payload: &Value) -> Option<String> {
    for key in [
        "modelStreamErrorException",
        "modelTimeoutException",
        "internalServerException",
        "serviceUnavailableException",
        "throttlingException",
        "validationException",
        "modelErrorException",
        "modelNotReadyException",
        "serviceQuotaExceededException",
    ] {
        if let Some(error) = payload.get(key) {
            let message = error
                .get("message")
                .or_else(|| error.get("originalMessage"))
                .and_then(Value::as_str)
                .unwrap_or("Bedrock response stream failed");
            return Some(format!("{key}: {message}"));
        }
    }
    None
}

fn read_be_u32(bytes: &[u8]) -> ProviderResult<u32> {
    Ok(u32::from_be_bytes(bytes.try_into().map_err(|_| {
        ProviderError::Decode("internal AWS eventstream u32 slice length mismatch".to_string())
    })?))
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn take_u8<'a>(bytes: &mut &'a [u8]) -> ProviderResult<u8> {
    let Some((first, rest)) = bytes.split_first() else {
        return Err(ProviderError::Decode(
            "truncated AWS eventstream header".to_string(),
        ));
    };
    *bytes = rest;
    Ok(*first)
}

fn take_bytes<'a>(bytes: &mut &'a [u8], length: usize) -> ProviderResult<&'a [u8]> {
    if bytes.len() < length {
        return Err(ProviderError::Decode(format!(
            "truncated AWS eventstream header value: got {} bytes, need {length}",
            bytes.len()
        )));
    }
    let (head, rest) = bytes.split_at(length);
    *bytes = rest;
    Ok(head)
}

fn take_array<const N: usize>(bytes: &mut &[u8]) -> ProviderResult<[u8; N]> {
    let value = take_bytes(bytes, N)?;
    value.try_into().map_err(|_| {
        ProviderError::Decode("internal AWS eventstream array length mismatch".to_string())
    })
}

fn image_data_url(content: &CanonicalContent) -> Option<String> {
    match content {
        CanonicalContent::Image { data, mime_type } => {
            Some(format!("data:{mime_type};base64,{data}"))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
