#[derive(Debug)]
pub enum ScriptedProviderStep {
    Response(verlet_provider::ProviderResponse),
    Error(String),
    Pending,
}

#[derive(Default)]
pub struct ScriptedProviderClient {
    requests: std::sync::Mutex<Vec<verlet_provider::ProviderRequest>>,
    stream_requests: std::sync::Mutex<Vec<verlet_provider::ProviderRequest>>,
    responses: std::sync::Mutex<std::collections::VecDeque<ScriptedProviderStep>>,
    stream_events:
        std::sync::Mutex<std::collections::VecDeque<Vec<verlet_provider::ProviderStreamEvent>>>,
    capabilities: Option<verlet_provider::ProviderCapabilityRecord>,
}

impl ScriptedProviderClient {
    pub fn with_responses(responses: Vec<verlet_provider::ProviderResponse>) -> Self {
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

    pub fn with_stream_events(events: Vec<Vec<verlet_provider::ProviderStreamEvent>>) -> Self {
        Self {
            stream_events: std::sync::Mutex::new(events.into()),
            ..Self::default()
        }
    }

    pub fn with_capabilities(
        mut self,
        capabilities: verlet_provider::ProviderCapabilityRecord,
    ) -> Self {
        self.capabilities = Some(capabilities);
        self
    }

    pub fn requests(&self) -> Vec<verlet_provider::ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn stream_requests(&self) -> Vec<verlet_provider::ProviderRequest> {
        self.stream_requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for ScriptedProviderClient {
    fn capabilities(&self) -> Option<verlet_provider::ProviderCapabilityRecord> {
        self.capabilities.clone()
    }

    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let step = self.responses.lock().unwrap().pop_front().ok_or_else(|| {
            verlet_provider::ProviderError::Decode("no test response queued".to_string())
        })?;
        match step {
            ScriptedProviderStep::Response(response) => Ok(response),
            ScriptedProviderStep::Error(message) => {
                Err(verlet_provider::ProviderError::Decode(message))
            }
            ScriptedProviderStep::Pending => std::future::pending().await,
        }
    }

    async fn stream(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<Vec<verlet_provider::ProviderStreamEvent>> {
        self.stream_requests.lock().unwrap().push(request.clone());
        self.stream_events
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| {
                verlet_provider::ProviderError::Decode("no test stream queued".to_string())
            })
    }
}

pub fn response_text(text: &str) -> verlet_provider::ProviderResponse {
    verlet_provider::ProviderResponse {
        content: vec![verlet_history::CanonicalContent::text(text)],
        usage: verlet_history::CanonicalUsage {
            input_tokens: 1,
            output_tokens: 2,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        },
        stop_reason: verlet_history::CanonicalStopReason::EndTurn,
    }
}

pub fn response_tool_call(
    name: &str,
    arguments: serde_json::Value,
) -> verlet_provider::ProviderResponse {
    response_tool_call_with_id("call_1|fc_1", name, arguments)
}

pub fn response_tool_call_with_id(
    call_id: &str,
    name: &str,
    arguments: serde_json::Value,
) -> verlet_provider::ProviderResponse {
    verlet_provider::ProviderResponse {
        content: vec![verlet_history::CanonicalContent::tool_call(
            call_id, name, arguments,
        )],
        usage: verlet_history::CanonicalUsage::default(),
        stop_reason: verlet_history::CanonicalStopReason::ToolUse,
    }
}

pub fn provider_factory(
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
) -> std::sync::Arc<verlet::adapters::agent_loop::AgentLoopFactory> {
    let mut config = verlet::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    std::sync::Arc::new(verlet::adapters::agent_loop::AgentLoopFactory::new(
        config, client,
    ))
}

pub fn streaming_provider_factory(
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
) -> std::sync::Arc<verlet::adapters::agent_loop::AgentLoopFactory> {
    let mut config = verlet::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIResponses,
        "openai",
        "gpt-test",
    );
    config.max_tokens = 128;
    config.stream = true;
    std::sync::Arc::new(verlet::adapters::agent_loop::AgentLoopFactory::new(
        config, client,
    ))
}
