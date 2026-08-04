use reqwest::StatusCode;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use verlet::{
    AgentLoopConfig, AgentLoopFactory, AnthropicBedrockMessagesAdapter, AnthropicMessagesAdapter,
    CanonicalContent, CanonicalMessage, OpenAIReasoningSummary, OpenAIResponsesAdapter,
    ProviderApi, ProviderClient, ProviderEndpoint, ProviderHttpClient, ProviderRequest,
    ProviderStreamEvent, ProviderWireAdapter, RuntimeHost, ThreadCoordinates, ThreadEvent,
    ThreadTopology,
};

const DEFAULT_ENV_FILE: &str = ".env";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env_flag("VERLET_BEDROCK_SMOKE_ONLY") {
        let config = BedrockSmokeConfig::load()?;
        let bedrock_stream = smoke_anthropic_bedrock_stream(&config).await?;
        println!(
            "bedrock stream ok model={} stop={:?} text={}",
            bedrock_stream.model,
            bedrock_stream.stop_reason,
            compact(&bedrock_stream.text)
        );
        return Ok(());
    }

    let config = SmokeConfig::load()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    let openai = smoke_openai(&client, &config).await?;
    println!(
        "openai ok model={} stop={:?} text={}",
        openai.model,
        openai.stop_reason,
        compact(&openai.text)
    );
    let openai_stream = smoke_openai_stream(&client, &config).await?;
    println!(
        "openai stream ok model={} stop={:?} text={}",
        openai_stream.model,
        openai_stream.stop_reason,
        compact(&openai_stream.text)
    );

    let anthropic = smoke_anthropic(&client, &config).await?;
    println!(
        "anthropic ok model={} stop={:?} text={}",
        anthropic.model,
        anthropic.stop_reason,
        compact(&anthropic.text)
    );
    let anthropic_stream = smoke_anthropic_stream(&client, &config).await?;
    println!(
        "anthropic stream ok model={} stop={:?} text={}",
        anthropic_stream.model,
        anthropic_stream.stop_reason,
        compact(&anthropic_stream.text)
    );

    let canonical_openai = smoke_canonical_openai_runtime(&config).await?;
    println!(
        "canonical runtime openai ok model={} text={}",
        canonical_openai.model,
        compact(&canonical_openai.text)
    );
    let canonical_anthropic = smoke_canonical_anthropic_runtime(&config).await?;
    println!(
        "canonical runtime anthropic ok model={} text={}",
        canonical_anthropic.model,
        compact(&canonical_anthropic.text)
    );

    Ok(())
}

struct SmokeConfig {
    openai: ProviderProtocolConfig,
    anthropic: ProviderProtocolConfig,
}

struct ProviderProtocolConfig {
    base_url: String,
    api_key: String,
    model: String,
}

struct SmokeResult {
    model: String,
    stop_reason: verlet::CanonicalStopReason,
    text: String,
}

struct BedrockSmokeConfig {
    region: String,
    base_url: Option<String>,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    model: String,
}

impl BedrockSmokeConfig {
    fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let env_file = verlet_runtime_contracts::env_compat::var("VERLET_BEDROCK_ENV_FILE")
            .or_else(|_| {
                verlet_runtime_contracts::env_compat::var("VERLET_ANTHROPIC_BEDROCK_ENV_FILE")
            })
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_ENV_FILE));
        let file_env = read_env_file_if_exists(&env_file)?;
        let region = env_or_file("AWS_BEDROCK_REGION", &file_env)
            .or_else(|| env_or_file("AWS_REGION", &file_env))
            .or_else(|| env_or_file("AWS_DEFAULT_REGION", &file_env))
            .unwrap_or_else(|| "us-east-1".to_string());
        let base_url = env_or_file("VERLET_BEDROCK_BASE_URL", &file_env)
            .or_else(|| env_or_file("ANTHROPIC_BEDROCK_BASE_URL", &file_env))
            .map(|value| value.trim_end_matches('/').to_string());
        let access_key_id = env_or_file("AWS_ACCESS_KEY_ID", &file_env)
            .ok_or("missing AWS_ACCESS_KEY_ID for VERLET_BEDROCK_SMOKE_ONLY")?;
        let secret_access_key = env_or_file("AWS_SECRET_ACCESS_KEY", &file_env)
            .ok_or("missing AWS_SECRET_ACCESS_KEY for VERLET_BEDROCK_SMOKE_ONLY")?;
        let session_token = env_or_file("AWS_SESSION_TOKEN", &file_env);
        let model = env_or_file("VERLET_ANTHROPIC_BEDROCK_MODEL", &file_env)
            .or_else(|| env_or_file("AWS_BEDROCK_MODEL", &file_env))
            .unwrap_or_else(|| "global.anthropic.claude-sonnet-4-5-20250929-v1:0".to_string());
        Ok(Self {
            region,
            base_url,
            access_key_id,
            secret_access_key,
            session_token,
            model,
        })
    }
}

impl SmokeConfig {
    fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let env_file = verlet_runtime_contracts::env_compat::var("VERLET_BIFROST_ENV_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_ENV_FILE));
        let file_env = read_env_file_if_exists(&env_file)?;

        let gateway_base_url = env_or_file("VERLET_BIFROST_URL", &file_env)
            .or_else(|| env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
            .or_else(|| env_or_file("LLM_PROXY_URL", &file_env))
            .map(|value| value.trim_end_matches('/').to_string());

        let gateway_api_key = env_or_file("VERLET_BIFROST_KEY", &file_env)
            .or_else(|| env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
            .or_else(|| env_or_file("BIFROST_SYSTEM_KEY", &file_env));

        let openai_api_key = env_or_file("VERLET_OPENAI_RESPONSES_KEY", &file_env)
            .or_else(|| env_or_file("OPENAI_API_KEY", &file_env))
            .or_else(|| gateway_api_key.clone())
            .ok_or("missing VERLET_OPENAI_RESPONSES_KEY, OPENAI_API_KEY, or VERLET_BIFROST_KEY")?;
        let openai_base_url = env_or_file("VERLET_OPENAI_RESPONSES_BASE_URL", &file_env)
            .or_else(|| env_or_file("VERLET_OPENAI_BASE_URL", &file_env))
            .or_else(|| {
                env_or_file("OPENAI_API_KEY", &file_env).map(|_| "https://api.openai.com".into())
            })
            .or_else(|| gateway_base_url.clone())
            .ok_or(
                "missing VERLET_OPENAI_RESPONSES_BASE_URL, OPENAI_API_KEY, or VERLET_BIFROST_URL",
            )?
            .trim_end_matches('/')
            .to_string();

        let anthropic_api_key = env_or_file("VERLET_ANTHROPIC_MESSAGES_KEY", &file_env)
            .or_else(|| env_or_file("ANTHROPIC_API_KEY", &file_env))
            .or_else(|| gateway_api_key)
            .ok_or(
                "missing VERLET_ANTHROPIC_MESSAGES_KEY, ANTHROPIC_API_KEY, or VERLET_BIFROST_KEY",
            )?;
        let anthropic_base_url = env_or_file("VERLET_ANTHROPIC_MESSAGES_BASE_URL", &file_env)
            .or_else(|| env_or_file("VERLET_ANTHROPIC_BASE_URL", &file_env))
            .or_else(|| {
                env_or_file("ANTHROPIC_API_KEY", &file_env)
                    .map(|_| "https://api.anthropic.com".into())
            })
            .or_else(|| gateway_base_url.map(|base_url| format!("{base_url}/anthropic")))
            .ok_or(
                "missing VERLET_ANTHROPIC_MESSAGES_BASE_URL, ANTHROPIC_API_KEY, or VERLET_BIFROST_URL",
            )?
            .trim_end_matches('/')
            .to_string();

        Ok(Self {
            openai: ProviderProtocolConfig {
                base_url: openai_base_url.clone(),
                api_key: openai_api_key,
                model: env_or_file("VERLET_OPENAI_RESPONSES_MODEL", &file_env)
                    .or_else(|| env_or_file("VERLET_BIFROST_OPENAI_MODEL", &file_env))
                    .or_else(|| env_or_file("OPENAI_MODEL", &file_env))
                    .unwrap_or_else(|| {
                        if openai_base_url.contains("api.openai.com") {
                            "gpt-4.1-mini".to_string()
                        } else {
                            "openai/gpt-5.5".to_string()
                        }
                    }),
            },
            anthropic: ProviderProtocolConfig {
                base_url: anthropic_base_url.clone(),
                api_key: anthropic_api_key,
                model: env_or_file("VERLET_ANTHROPIC_MESSAGES_MODEL", &file_env)
                    .or_else(|| env_or_file("VERLET_BIFROST_ANTHROPIC_MODEL", &file_env))
                    .or_else(|| env_or_file("ANTHROPIC_MODEL", &file_env))
                    .unwrap_or_else(|| {
                        if anthropic_base_url.contains("api.anthropic.com") {
                            "claude-sonnet-4-5-20250929".to_string()
                        } else {
                            "bedrock/us.anthropic.claude-sonnet-4-6".to_string()
                        }
                    }),
            },
        })
    }
}

async fn smoke_openai(
    client: &reqwest::Client,
    config: &SmokeConfig,
) -> Result<SmokeResult, Box<dyn std::error::Error>> {
    let adapter = OpenAIResponsesAdapter {
        include_encrypted_reasoning: false,
        reasoning_summary: OpenAIReasoningSummary::Auto,
    };
    let mut request = ProviderRequest::new(
        ProviderApi::OpenAIResponses,
        "openai",
        config.openai.model.clone(),
    );
    request.max_tokens = 32;
    request.messages = vec![CanonicalMessage::user_text(
        "Reply with exactly COOL_OPENAI_OK and no other text.",
    )];
    let body = adapter.build_request_body(&request)?;
    let url = endpoint_url(&config.openai.base_url, "responses");
    let response = client
        .post(url)
        .bearer_auth(&config.openai.api_key)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await?;
    let value = read_success_json(response).await?;
    let decoded = adapter.decode_response_body(&value)?;
    let text = content_text(&decoded.content);
    ensure_marker(&text, "COOL_OPENAI_OK")?;
    Ok(SmokeResult {
        model: config.openai.model.clone(),
        stop_reason: decoded.stop_reason,
        text,
    })
}

async fn smoke_openai_stream(
    client: &reqwest::Client,
    config: &SmokeConfig,
) -> Result<SmokeResult, Box<dyn std::error::Error>> {
    let adapter = OpenAIResponsesAdapter {
        include_encrypted_reasoning: false,
        reasoning_summary: OpenAIReasoningSummary::Auto,
    };
    let mut request = ProviderRequest::new(
        ProviderApi::OpenAIResponses,
        "openai",
        config.openai.model.clone(),
    );
    request.max_tokens = 32;
    request.messages = vec![CanonicalMessage::user_text(
        "Reply with exactly COOL_OPENAI_STREAM_OK and no other text.",
    )];
    let body = adapter.build_stream_request_body(&request)?;
    let url = endpoint_url(&config.openai.base_url, "responses");
    let response = client
        .post(url)
        .bearer_auth(&config.openai.api_key)
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .json(&body)
        .send()
        .await?;
    let sse = read_success_text(response).await?;
    let events = adapter.decode_sse_events(&sse)?;
    let text = stream_text(&events);
    ensure_marker(&text, "COOL_OPENAI_STREAM_OK")?;
    Ok(SmokeResult {
        model: config.openai.model.clone(),
        stop_reason: stream_stop_reason(&events),
        text,
    })
}

async fn smoke_anthropic(
    client: &reqwest::Client,
    config: &SmokeConfig,
) -> Result<SmokeResult, Box<dyn std::error::Error>> {
    let adapter = AnthropicMessagesAdapter;
    let mut request = ProviderRequest::new(
        ProviderApi::AnthropicMessages,
        "anthropic",
        config.anthropic.model.clone(),
    );
    request.max_tokens = 32;
    request.messages = vec![CanonicalMessage::user_text(
        "Reply with exactly COOL_ANTHROPIC_OK and no other text.",
    )];
    let body = adapter.build_request_body(&request)?;
    let url = endpoint_url(&config.anthropic.base_url, "messages");
    let response = client
        .post(url)
        .header("x-api-key", &config.anthropic.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await?;
    let value = read_success_json(response).await?;
    let decoded = adapter.decode_response_body(&value)?;
    let text = content_text(&decoded.content);
    ensure_marker(&text, "COOL_ANTHROPIC_OK")?;
    Ok(SmokeResult {
        model: config.anthropic.model.clone(),
        stop_reason: decoded.stop_reason,
        text,
    })
}

async fn smoke_anthropic_stream(
    client: &reqwest::Client,
    config: &SmokeConfig,
) -> Result<SmokeResult, Box<dyn std::error::Error>> {
    let adapter = AnthropicMessagesAdapter;
    let mut request = ProviderRequest::new(
        ProviderApi::AnthropicMessages,
        "anthropic",
        config.anthropic.model.clone(),
    );
    request.max_tokens = 32;
    request.messages = vec![CanonicalMessage::user_text(
        "Reply with exactly COOL_ANTHROPIC_STREAM_OK and no other text.",
    )];
    let body = adapter.build_stream_request_body(&request)?;
    let url = endpoint_url(&config.anthropic.base_url, "messages");
    let response = client
        .post(url)
        .header("x-api-key", &config.anthropic.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .header("accept", "text/event-stream")
        .json(&body)
        .send()
        .await?;
    let sse = read_success_text(response).await?;
    let events = adapter.decode_sse_events(&sse)?;
    let text = stream_text(&events);
    ensure_marker(&text, "COOL_ANTHROPIC_STREAM_OK")?;
    Ok(SmokeResult {
        model: config.anthropic.model.clone(),
        stop_reason: stream_stop_reason(&events),
        text,
    })
}

async fn smoke_anthropic_bedrock_stream(
    config: &BedrockSmokeConfig,
) -> Result<SmokeResult, Box<dyn std::error::Error>> {
    let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(AnthropicBedrockMessagesAdapter);
    let endpoint = if let Some(base_url) = &config.base_url {
        ProviderEndpoint::anthropic_bedrock_with_base_url(
            base_url,
            &config.region,
            &config.model,
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            config.session_token.clone(),
        )
    } else {
        ProviderEndpoint::anthropic_bedrock(
            &config.region,
            &config.model,
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            config.session_token.clone(),
        )
    };
    let client = ProviderHttpClient::new(endpoint, adapter)?;
    let mut request = ProviderRequest::new(
        ProviderApi::AnthropicMessages,
        "anthropic_bedrock",
        config.model.clone(),
    );
    request.max_tokens = 32;
    request.messages = vec![CanonicalMessage::user_text(
        "Reply with exactly COOL_BEDROCK_STREAM_OK and no other text.",
    )];

    let events = client.stream(&request).await?;
    if let Some(error) = stream_error(&events) {
        return Err(format!("Bedrock stream returned provider error: {error}").into());
    }
    let text = stream_text(&events);
    ensure_marker_with_context(
        &text,
        "COOL_BEDROCK_STREAM_OK",
        &stream_event_summary(&events),
    )?;
    Ok(SmokeResult {
        model: config.model.clone(),
        stop_reason: stream_stop_reason(&events),
        text,
    })
}

async fn smoke_canonical_openai_runtime(
    config: &SmokeConfig,
) -> Result<SmokeResult, Box<dyn std::error::Error>> {
    let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(OpenAIResponsesAdapter {
        include_encrypted_reasoning: false,
        reasoning_summary: OpenAIReasoningSummary::Auto,
    });
    let client = Arc::new(ProviderHttpClient::new(
        ProviderEndpoint::openai_responses(&config.openai.base_url, config.openai.api_key.clone()),
        adapter,
    )?);
    let mut runtime_config = AgentLoopConfig::new(
        ProviderApi::OpenAIResponses,
        "openai",
        config.openai.model.clone(),
    );
    runtime_config.max_tokens = 32;
    runtime_config.stream = true;
    let host = RuntimeHost::new(Arc::new(AgentLoopFactory::new(runtime_config, client)));
    run_canonical_runtime_smoke(
        host,
        "canonical_openai",
        "Reply with exactly COOL_CANONICAL_OPENAI_STREAM_OK and no other text.",
        config.openai.model.clone(),
    )
    .await
}

async fn smoke_canonical_anthropic_runtime(
    config: &SmokeConfig,
) -> Result<SmokeResult, Box<dyn std::error::Error>> {
    let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(AnthropicMessagesAdapter);
    let client = Arc::new(ProviderHttpClient::new(
        ProviderEndpoint::anthropic_messages(
            &config.anthropic.base_url,
            config.anthropic.api_key.clone(),
        ),
        adapter,
    )?);
    let mut runtime_config = AgentLoopConfig::new(
        ProviderApi::AnthropicMessages,
        "anthropic",
        config.anthropic.model.clone(),
    );
    runtime_config.max_tokens = 32;
    runtime_config.stream = true;
    let host = RuntimeHost::new(Arc::new(AgentLoopFactory::new(runtime_config, client)));
    run_canonical_runtime_smoke(
        host,
        "canonical_anthropic",
        "Reply with exactly COOL_CANONICAL_ANTHROPIC_STREAM_OK and no other text.",
        config.anthropic.model.clone(),
    )
    .await
}

async fn run_canonical_runtime_smoke(
    host: RuntimeHost,
    session_id: &str,
    prompt: &str,
    model: String,
) -> Result<SmokeResult, Box<dyn std::error::Error>> {
    let thread = host
        .start_thread(
            ThreadCoordinates::new("smoke_tenant", "smoke_user", session_id),
            ThreadTopology::root(),
        )
        .await?;
    let mut events = thread.subscribe_events();
    host.submit(thread.context().coordinates.thread_id, "turn_1", prompt)
        .await?;
    let text = next_output(&mut events).await?;
    let marker = if session_id.contains("openai") {
        "COOL_CANONICAL_OPENAI_STREAM_OK"
    } else {
        "COOL_CANONICAL_ANTHROPIC_STREAM_OK"
    };
    ensure_marker(&text, marker)?;
    let session = thread.session_context().await?;
    if !session
        .entries
        .iter()
        .all(|entry| matches!(entry.kind, verlet::SessionEntryKind::Message { .. }))
    {
        return Err("canonical runtime stored a non-message provider record".into());
    }
    Ok(SmokeResult {
        model,
        stop_reason: verlet::CanonicalStopReason::EndTurn,
        text,
    })
}

async fn next_output(
    events: &mut tokio::sync::broadcast::Receiver<ThreadEvent>,
) -> Result<String, Box<dyn std::error::Error>> {
    loop {
        let event = timeout(Duration::from_secs(60), events.recv()).await??;
        match event {
            ThreadEvent::Output { text, .. } => return Ok(text),
            ThreadEvent::Failed { message, .. } => return Err(message.into()),
            _ => {}
        }
    }
}

async fn read_success_json(
    response: reqwest::Response,
) -> Result<Value, Box<dyn std::error::Error>> {
    let text = read_success_text(response).await?;
    Ok(serde_json::from_str(&text)?)
}

async fn read_success_text(
    response: reqwest::Response,
) -> Result<String, Box<dyn std::error::Error>> {
    let status = response.status();
    let text = response.text().await?;
    if status != StatusCode::OK {
        return Err(format!("HTTP {status}: {}", compact(&text)).into());
    }
    Ok(text)
}

fn endpoint_url(base_url: &str, endpoint: &str) -> String {
    let base_url = base_url.trim_end_matches('/');
    if base_url.ends_with("/v1") {
        format!("{base_url}/{endpoint}")
    } else {
        format!("{base_url}/v1/{endpoint}")
    }
}

fn read_env_file_if_exists(
    path: &PathBuf,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text = std::fs::read_to_string(path)?;
    let mut env = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        env.insert(key.trim().to_string(), unquote(value.trim()));
    }
    Ok(env)
}

fn env_or_file(key: &str, file_env: &HashMap<String, String>) -> Option<String> {
    verlet_runtime_contracts::env_compat::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| file_env.get(key).cloned())
        .filter(|value| !value.trim().is_empty())
}

fn env_flag(key: &str) -> bool {
    matches!(
        verlet_runtime_contracts::env_compat::var(key)
            .ok()
            .as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn unquote(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
        .to_string()
}

fn content_text(content: &[CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn stream_text(events: &[ProviderStreamEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            ProviderStreamEvent::TextDelta { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn stream_error(events: &[ProviderStreamEvent]) -> Option<&str> {
    events.iter().find_map(|event| match event {
        ProviderStreamEvent::Error { message } => Some(message.as_str()),
        _ => None,
    })
}

fn stream_event_summary(events: &[ProviderStreamEvent]) -> String {
    if events.is_empty() {
        return "no_events".to_string();
    }
    events
        .iter()
        .map(|event| match event {
            ProviderStreamEvent::TextDelta { text } => format!("text_delta(len={})", text.len()),
            ProviderStreamEvent::ThinkingDelta { text } => {
                format!("thinking_delta(len={})", text.len())
            }
            ProviderStreamEvent::ToolCallDelta {
                arguments_delta, ..
            } => {
                format!("tool_call_delta(args_len={})", arguments_delta.len())
            }
            ProviderStreamEvent::Content { content } => match content {
                CanonicalContent::Text { text, .. } => format!("content_text(len={})", text.len()),
                CanonicalContent::Thinking { text, .. } => {
                    format!("content_thinking(len={})", text.len())
                }
                CanonicalContent::Image { .. } => "content_image".to_string(),
                CanonicalContent::ToolCall { .. } => "content_tool_call".to_string(),
            },
            ProviderStreamEvent::Usage { usage } => format!(
                "usage(in={},out={},cache_create={},cache_read={})",
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens,
                usage.cache_read_input_tokens
            ),
            ProviderStreamEvent::Done { stop_reason } => format!("done({stop_reason:?})"),
            ProviderStreamEvent::Error { message } => format!("error(len={})", message.len()),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn stream_stop_reason(events: &[ProviderStreamEvent]) -> verlet::CanonicalStopReason {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            ProviderStreamEvent::Done { stop_reason } => Some(*stop_reason),
            _ => None,
        })
        .unwrap_or(verlet::CanonicalStopReason::EndTurn)
}

fn ensure_marker(text: &str, marker: &str) -> Result<(), Box<dyn std::error::Error>> {
    ensure_marker_with_context(text, marker, "")
}

fn ensure_marker_with_context(
    text: &str,
    marker: &str,
    context: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if text.contains(marker) {
        Ok(())
    } else {
        let context = if context.is_empty() {
            String::new()
        } else {
            format!(" events=[{context}]")
        };
        let message = format!(
            "provider response did not contain expected marker {marker}: {}",
            compact(text),
        ) + &context;
        Err(message.into())
    }
}

fn compact(text: &str) -> String {
    let one_line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if one_line.len() <= 240 {
        one_line
    } else {
        format!("{}...", &one_line[..240])
    }
}
