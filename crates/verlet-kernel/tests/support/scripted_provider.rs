#[derive(Debug)]
pub enum ScriptedProviderStep {
    Response(verlet::ProviderResponse),
    Error(String),
    Pending,
}

#[derive(Default)]
pub struct ScriptedProviderClient {
    requests: std::sync::Mutex<Vec<verlet::ProviderRequest>>,
    stream_requests: std::sync::Mutex<Vec<verlet::ProviderRequest>>,
    responses: std::sync::Mutex<std::collections::VecDeque<ScriptedProviderStep>>,
    stream_events: std::sync::Mutex<std::collections::VecDeque<Vec<verlet::ProviderStreamEvent>>>,
    capabilities: Option<verlet::ProviderCapabilityRecord>,
}

impl ScriptedProviderClient {
    pub fn with_responses(responses: Vec<verlet::ProviderResponse>) -> Self {
        Self {
            responses: std::sync::Mutex::new(
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
            responses: std::sync::Mutex::new(steps.into()),
            ..Self::default()
        }
    }

    pub fn with_stream_events(events: Vec<Vec<verlet::ProviderStreamEvent>>) -> Self {
        Self {
            stream_events: std::sync::Mutex::new(events.into()),
            ..Self::default()
        }
    }

    pub fn with_capabilities(mut self, capabilities: verlet::ProviderCapabilityRecord) -> Self {
        self.capabilities = Some(capabilities);
        self
    }

    pub fn requests(&self) -> Vec<verlet::ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn stream_requests(&self) -> Vec<verlet::ProviderRequest> {
        self.stream_requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl verlet::ProviderClient for ScriptedProviderClient {
    fn capabilities(&self) -> Option<verlet::ProviderCapabilityRecord> {
        self.capabilities.clone()
    }

    async fn complete(
        &self,
        request: &verlet::ProviderRequest,
    ) -> verlet::ProviderResult<verlet::ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let step =
            self.responses.lock().unwrap().pop_front().ok_or_else(|| {
                verlet::ProviderError::Decode("no test response queued".to_string())
            })?;
        match step {
            ScriptedProviderStep::Response(response) => Ok(response),
            ScriptedProviderStep::Error(message) => Err(verlet::ProviderError::Decode(message)),
            ScriptedProviderStep::Pending => std::future::pending().await,
        }
    }

    async fn stream(
        &self,
        request: &verlet::ProviderRequest,
    ) -> verlet::ProviderResult<Vec<verlet::ProviderStreamEvent>> {
        self.stream_requests.lock().unwrap().push(request.clone());
        self.stream_events
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| verlet::ProviderError::Decode("no test stream queued".to_string()))
    }
}

pub fn response_text(text: &str) -> verlet::ProviderResponse {
    verlet::ProviderResponse {
        content: vec![verlet::CanonicalContent::text(text)],
        usage: verlet::CanonicalUsage {
            input_tokens: 1,
            output_tokens: 2,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        },
        stop_reason: verlet::CanonicalStopReason::EndTurn,
    }
}

pub fn response_tool_call(name: &str, arguments: serde_json::Value) -> verlet::ProviderResponse {
    response_tool_call_with_id("call_1|fc_1", name, arguments)
}

pub fn response_tool_call_with_id(
    call_id: &str,
    name: &str,
    arguments: serde_json::Value,
) -> verlet::ProviderResponse {
    verlet::ProviderResponse {
        content: vec![verlet::CanonicalContent::tool_call(
            call_id, name, arguments,
        )],
        usage: verlet::CanonicalUsage::default(),
        stop_reason: verlet::CanonicalStopReason::ToolUse,
    }
}

pub fn provider_factory(
    client: std::sync::Arc<dyn verlet::ProviderClient>,
) -> std::sync::Arc<verlet::AgentLoopFactory> {
    let mut config =
        verlet::AgentLoopConfig::new(verlet::ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    std::sync::Arc::new(verlet::AgentLoopFactory::new(config, client))
}

pub fn streaming_provider_factory(
    client: std::sync::Arc<dyn verlet::ProviderClient>,
) -> std::sync::Arc<verlet::AgentLoopFactory> {
    let mut config =
        verlet::AgentLoopConfig::new(verlet::ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    config.stream = true;
    std::sync::Arc::new(verlet::AgentLoopFactory::new(config, client))
}
