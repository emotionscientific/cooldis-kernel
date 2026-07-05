use crate::{
    CanonicalMessage, CooldisError, CooldisResult, OPERATION_METADATA_RUNTIME_KIND,
    OperationProjection, OperationRegistry, ToolDefinition, TurnContext, TurnContextSnapshot,
    WasmOperationValueKind,
};
use async_trait::async_trait;
use cooldis_process::{CooldisProcessBackend, CooldisProcessId};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone)]
pub struct AgentToolRouter {
    operation_registry: Arc<OperationRegistry>,
    kernel_tool_providers: Vec<Arc<dyn AgentKernelToolProvider>>,
    capability_grants: BTreeSet<String>,
    tool_aliases: BTreeMap<String, OperationToolAlias>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedAgentToolCall {
    pub call_id: String,
    pub tool_name: String,
    pub registered_name: String,
    pub operation_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentKernelToolCall {
    pub call_id: String,
    pub tool_name: String,
    pub arguments: Value,
    pub turn_context: Option<TurnContextSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentKernelPendingToolCall {
    pub process_id: CooldisProcessId,
    pub backend: CooldisProcessBackend,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AgentKernelToolOutcome {
    Completed(Option<CanonicalMessage>),
    Pending(AgentKernelPendingToolCall),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationToolAlias {
    pub tool_name: String,
    pub registered_name: String,
    pub operation_name: String,
}

#[async_trait]
pub trait AgentKernelToolProvider: Send + Sync + 'static {
    async fn tool_definitions(&self) -> Vec<ToolDefinition>;

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>>;

    async fn invoke_tool_call_outcome(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<AgentKernelToolOutcome> {
        self.invoke_tool_call(call)
            .await
            .map(AgentKernelToolOutcome::Completed)
    }
}

impl AgentToolRouter {
    pub fn new(operation_registry: Arc<OperationRegistry>) -> Self {
        Self {
            operation_registry,
            kernel_tool_providers: Vec::new(),
            capability_grants: BTreeSet::new(),
            tool_aliases: BTreeMap::new(),
        }
    }

    pub fn with_kernel_tool_provider(
        mut self,
        kernel_tool_provider: Arc<dyn AgentKernelToolProvider>,
    ) -> Self {
        self.kernel_tool_providers.push(kernel_tool_provider);
        self
    }

    pub fn with_capability_grant(mut self, grant: impl Into<String>) -> Self {
        self.capability_grants.insert(grant.into());
        self
    }

    pub fn with_capability_grants(mut self, grants: impl IntoIterator<Item = String>) -> Self {
        self.capability_grants.extend(grants);
        self
    }

    pub fn with_tool_aliases(
        mut self,
        aliases: impl IntoIterator<Item = OperationToolAlias>,
    ) -> Self {
        for alias in aliases {
            self.tool_aliases.insert(alias.tool_name.clone(), alias);
        }
        self
    }

    pub fn operation_registry(&self) -> Arc<OperationRegistry> {
        Arc::clone(&self.operation_registry)
    }

    pub async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        let mut definitions = Vec::new();
        let mut names = BTreeSet::new();
        for alias in self.tool_aliases.values() {
            if names.insert(alias.tool_name.clone())
                && let Some(projection) = self
                    .projection_for_registered_operation(
                        &alias.registered_name,
                        &alias.operation_name,
                    )
                    .await
            {
                definitions.push(ToolDefinition::new(
                    alias.tool_name.clone(),
                    format!(
                        "Run Cooldis operation {}/{}.",
                        projection.registered_name, projection.operation_name
                    ),
                    input_schema_for_value_kind(&projection.input),
                ));
            }
        }
        for record in self.operation_registry.list().await {
            if is_kernel_record(&record) {
                continue;
            }
            for projection in record.projections().operations {
                if names.insert(projection.llm_tool.name.clone()) {
                    definitions.push(ToolDefinition::new(
                        projection.llm_tool.name,
                        format!(
                            "Run Cooldis operation {}/{}.",
                            projection.registered_name, projection.operation_name
                        ),
                        input_schema_for_value_kind(&projection.input),
                    ));
                }
            }
        }
        for kernel_tool_provider in &self.kernel_tool_providers {
            for tool in kernel_tool_provider.tool_definitions().await {
                if names.insert(tool.name.clone()) {
                    definitions.push(tool);
                }
            }
        }
        definitions
    }

    pub async fn invoke_tool_call(
        &self,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Value,
    ) -> CanonicalMessage {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        match self
            .invoke_tool_call_message(None, &call_id, &tool_name, arguments)
            .await
        {
            Ok(message) => message,
            Err(err) => CanonicalMessage::tool_result(call_id, tool_name, err.to_string(), true),
        }
    }

    pub async fn invoke_tool_call_for_turn(
        &self,
        turn_context: &TurnContext,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Value,
    ) -> CanonicalMessage {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        match self
            .invoke_tool_call_message(Some(turn_context), &call_id, &tool_name, arguments)
            .await
        {
            Ok(message) => message,
            Err(err) => CanonicalMessage::tool_result(call_id, tool_name, err.to_string(), true),
        }
    }

    pub async fn invoke_tool_call_outcome(
        &self,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Value,
    ) -> CooldisResult<AgentKernelToolOutcome> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        self.invoke_tool_call_outcome_message(None, &call_id, &tool_name, arguments)
            .await
    }

    pub async fn invoke_tool_call_outcome_for_turn(
        &self,
        turn_context: &TurnContext,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Value,
    ) -> CooldisResult<AgentKernelToolOutcome> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        self.invoke_tool_call_outcome_message(Some(turn_context), &call_id, &tool_name, arguments)
            .await
    }

    pub async fn route_tool_call(
        &self,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
    ) -> CooldisResult<RoutedAgentToolCall> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        let projection = self
            .projection_for_tool_name(&tool_name)
            .await?
            .ok_or_else(|| CooldisError::RuntimeExecution(format!("unknown tool {tool_name:?}")))?;
        Ok(RoutedAgentToolCall {
            call_id,
            tool_name,
            registered_name: projection.registered_name,
            operation_name: projection.operation_name,
        })
    }

    async fn invoke_tool_call_message(
        &self,
        turn_context: Option<&TurnContext>,
        call_id: &str,
        tool_name: &str,
        arguments: Value,
    ) -> CooldisResult<CanonicalMessage> {
        match self
            .invoke_tool_call_outcome_message(turn_context, call_id, tool_name, arguments)
            .await?
        {
            AgentKernelToolOutcome::Completed(Some(message)) => Ok(message),
            AgentKernelToolOutcome::Completed(None) => Err(CooldisError::RuntimeExecution(
                format!("unknown tool {tool_name:?}"),
            )),
            AgentKernelToolOutcome::Pending(pending) => {
                Err(CooldisError::RuntimeExecution(format!(
                    "tool {tool_name:?} returned pending process {} from {:?}, but this caller expects a completed tool result",
                    pending.process_id, pending.backend
                )))
            }
        }
    }

    async fn invoke_tool_call_outcome_message(
        &self,
        turn_context: Option<&TurnContext>,
        call_id: &str,
        tool_name: &str,
        arguments: Value,
    ) -> CooldisResult<AgentKernelToolOutcome> {
        if let Some(projection) = self.projection_for_tool_name(tool_name).await? {
            if projection.abi.has_hidden_durable_sink() {
                return Err(CooldisError::RuntimeExecution(format!(
                    "tool {tool_name:?} has a hidden durable sink"
                )));
            }
            self.validate_capability_grants(tool_name, &projection)?;
            let input = encode_tool_input(call_id, tool_name, &projection, arguments)?;
            let process = self
                .operation_registry
                .invoke_process_with_kernel_metadata(
                    &projection.registered_name,
                    &projection.operation_name,
                    input,
                    BTreeMap::from([
                        (
                            "cooldis.tool_call_id".to_string(),
                            Value::String(call_id.to_string()),
                        ),
                        (
                            "cooldis.tool_name".to_string(),
                            Value::String(tool_name.to_string()),
                        ),
                    ]),
                )
                .await?;
            let output = process.output();
            return Ok(AgentKernelToolOutcome::Completed(Some(
                CanonicalMessage::tool_result(
                    call_id,
                    tool_name,
                    decode_tool_output(&projection.output, &output.stdout),
                    false,
                ),
            )));
        }

        if let Some(kernel_tool_provider) = self.kernel_tool_provider_for_name(tool_name).await {
            let outcome = kernel_tool_provider
                .invoke_tool_call_outcome(AgentKernelToolCall {
                    call_id: call_id.to_string(),
                    tool_name: tool_name.to_string(),
                    arguments,
                    turn_context: turn_context.map(TurnContext::snapshot),
                })
                .await?;
            match outcome {
                AgentKernelToolOutcome::Completed(Some(_)) | AgentKernelToolOutcome::Pending(_) => {
                    return Ok(outcome);
                }
                AgentKernelToolOutcome::Completed(None) => {}
            }
        }

        Err(CooldisError::RuntimeExecution(format!(
            "unknown tool {tool_name:?}"
        )))
    }

    fn validate_capability_grants(
        &self,
        tool_name: &str,
        projection: &OperationProjection,
    ) -> CooldisResult<()> {
        let missing = projection
            .abi
            .required_capabilities
            .iter()
            .filter(|capability| !self.capability_grants.contains(capability.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(CooldisError::RuntimeExecution(format!(
                "tool {tool_name:?} missing capability grants: {}",
                missing.join(", ")
            )))
        }
    }

    async fn kernel_tool_provider_for_name(
        &self,
        tool_name: &str,
    ) -> Option<Arc<dyn AgentKernelToolProvider>> {
        for kernel_tool_provider in &self.kernel_tool_providers {
            if kernel_tool_provider
                .tool_definitions()
                .await
                .into_iter()
                .any(|tool| tool.name == tool_name)
            {
                return Some(Arc::clone(kernel_tool_provider));
            }
        }
        None
    }

    async fn projection_for_tool_name(
        &self,
        tool_name: &str,
    ) -> CooldisResult<Option<OperationProjection>> {
        if let Some(alias) = self.tool_aliases.get(tool_name) {
            return Ok(self
                .projection_for_registered_operation(&alias.registered_name, &alias.operation_name)
                .await);
        }
        for record in self.operation_registry.list().await {
            if is_kernel_record(&record) {
                continue;
            }
            for projection in record.projections().operations {
                if projection.llm_tool.name == tool_name {
                    return Ok(Some(projection));
                }
            }
        }
        Ok(None)
    }

    async fn projection_for_registered_operation(
        &self,
        registered_name: &str,
        operation_name: &str,
    ) -> Option<OperationProjection> {
        for record in self.operation_registry.list().await {
            if record.name != registered_name {
                continue;
            }
            for projection in record.projections().operations {
                if projection.operation_name == operation_name {
                    return Some(projection);
                }
            }
        }
        None
    }
}

fn is_kernel_record(record: &crate::RegisteredOperation) -> bool {
    record
        .metadata
        .get(OPERATION_METADATA_RUNTIME_KIND)
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == crate::KERNEL_RUNTIME_KIND)
}

fn input_schema_for_value_kind(kind: &WasmOperationValueKind) -> Value {
    match kind {
        WasmOperationValueKind::Json => json!({
            "type": "object",
            "additionalProperties": true
        }),
        WasmOperationValueKind::Text => text_input_schema("UTF-8 text input for the operation."),
        WasmOperationValueKind::Bytes => {
            text_input_schema("UTF-8 text that Cooldis passes as operation input bytes.")
        }
    }
}

fn text_input_schema(description: &str) -> Value {
    json!({
        "type": "object",
        "properties": {
            "input": {
                "type": "string",
                "description": description
            }
        },
        "required": ["input"],
        "additionalProperties": false
    })
}

fn encode_tool_input(
    call_id: &str,
    tool_name: &str,
    projection: &OperationProjection,
    arguments: Value,
) -> CooldisResult<Vec<u8>> {
    match projection.input {
        WasmOperationValueKind::Json => {
            if !arguments.is_object() {
                return Err(CooldisError::RuntimeExecution(format!(
                    "tool {tool_name:?} call {call_id} requires object arguments"
                )));
            }
            serde_json::to_vec(&arguments).map_err(|err| {
                CooldisError::RuntimeExecution(format!(
                    "tool {tool_name:?} call {call_id} has invalid JSON input: {err}"
                ))
            })
        }
        WasmOperationValueKind::Text | WasmOperationValueKind::Bytes => {
            let input = match arguments {
                Value::String(value) => value,
                Value::Object(mut object) => object
                    .remove("input")
                    .and_then(|value| value.as_str().map(ToString::to_string))
                    .ok_or_else(|| {
                        CooldisError::RuntimeExecution(format!(
                            "tool {tool_name:?} call {call_id} requires a string input field"
                        ))
                    })?,
                _ => {
                    return Err(CooldisError::RuntimeExecution(format!(
                        "tool {tool_name:?} call {call_id} requires object arguments"
                    )));
                }
            };
            Ok(input.into_bytes())
        }
    }
}

fn decode_tool_output(kind: &WasmOperationValueKind, bytes: &[u8]) -> String {
    match kind {
        WasmOperationValueKind::Json => serde_json::from_slice::<Value>(bytes)
            .ok()
            .and_then(|value| serde_json::to_string(&value).ok())
            .unwrap_or_else(|| String::from_utf8_lossy(bytes).to_string()),
        WasmOperationValueKind::Text | WasmOperationValueKind::Bytes => {
            String::from_utf8_lossy(bytes).to_string()
        }
    }
}

#[cfg(test)]
mod tests;
