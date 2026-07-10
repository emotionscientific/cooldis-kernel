use super::kernel_test::{
    CanonicalContent, CanonicalProviderRuntimeConfig, CanonicalProviderRuntimeFactory,
    CanonicalStopReason, CanonicalUsage, ProviderApi, ProviderCapabilityRecord, ProviderClient,
    ProviderError, ProviderRequest, ProviderResponse, ProviderResult, ProviderStreamEvent,
};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum ScriptedProviderStep {
    Response(ProviderResponse),
    Error(String),
    Pending,
}

#[derive(Default)]
pub struct ScriptedProviderClient {
    requests: Mutex<Vec<ProviderRequest>>,
    stream_requests: Mutex<Vec<ProviderRequest>>,
    responses: Mutex<VecDeque<ScriptedProviderStep>>,
    stream_events: Mutex<VecDeque<Vec<ProviderStreamEvent>>>,
    capabilities: Option<ProviderCapabilityRecord>,
}

impl ScriptedProviderClient {
    pub fn with_responses(responses: Vec<ProviderResponse>) -> Self {
        Self {
            responses: Mutex::new(
                responses
                    .into_iter()
                    .map(ScriptedProviderStep::Response)
                    .collect(),
            ),
            ..Self::default()
        }
    }

    pub fn with_steps(steps: Vec<ScriptedProviderStep>) -> Self {
        Self {
            responses: Mutex::new(steps.into()),
            ..Self::default()
        }
    }

    pub fn with_stream_events(events: Vec<Vec<ProviderStreamEvent>>) -> Self {
        Self {
            stream_events: Mutex::new(events.into()),
            ..Self::default()
        }
    }

    pub fn with_capabilities(mut self, capabilities: ProviderCapabilityRecord) -> Self {
        self.capabilities = Some(capabilities);
        self
    }

    pub fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn stream_requests(&self) -> Vec<ProviderRequest> {
        self.stream_requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl ProviderClient for ScriptedProviderClient {
    fn capabilities(&self) -> Option<ProviderCapabilityRecord> {
        self.capabilities.clone()
    }

    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let step = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ProviderError::Decode("no test response queued".to_string()))?;
        match step {
            ScriptedProviderStep::Response(response) => Ok(response),
            ScriptedProviderStep::Error(message) => Err(ProviderError::Decode(message)),
            ScriptedProviderStep::Pending => std::future::pending().await,
        }
    }

    async fn stream(&self, request: &ProviderRequest) -> ProviderResult<Vec<ProviderStreamEvent>> {
        self.stream_requests.lock().unwrap().push(request.clone());
        self.stream_events
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ProviderError::Decode("no test stream queued".to_string()))
    }
}

pub fn response_text(text: &str) -> ProviderResponse {
    ProviderResponse {
        content: vec![CanonicalContent::text(text)],
        usage: CanonicalUsage {
            input_tokens: 1,
            output_tokens: 2,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        },
        stop_reason: CanonicalStopReason::EndTurn,
    }
}

pub fn response_tool_call(name: &str, arguments: Value) -> ProviderResponse {
    response_tool_call_with_id("call_1|fc_1", name, arguments)
}

pub fn response_tool_call_with_id(call_id: &str, name: &str, arguments: Value) -> ProviderResponse {
    ProviderResponse {
        content: vec![CanonicalContent::tool_call(call_id, name, arguments)],
        usage: CanonicalUsage::default(),
        stop_reason: CanonicalStopReason::ToolUse,
    }
}

pub fn provider_factory(client: Arc<dyn ProviderClient>) -> Arc<CanonicalProviderRuntimeFactory> {
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    Arc::new(CanonicalProviderRuntimeFactory::new(config, client))
}

pub fn streaming_provider_factory(
    client: Arc<dyn ProviderClient>,
) -> Arc<CanonicalProviderRuntimeFactory> {
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    config.stream = true;
    Arc::new(CanonicalProviderRuntimeFactory::new(config, client))
}
