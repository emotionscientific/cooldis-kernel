#[derive(Clone)]
pub struct AgentToolRouter {
    operation_registry: std::sync::Arc<verlet_operations::operation_registry::OperationRegistry>,
    kernel_dispatch_overlay: verlet_operations::operation_registry::KernelDispatchOverlay,
    kernel_tool_providers: Vec<std::sync::Arc<dyn AgentKernelToolProvider>>,
    tool_aliases: std::collections::BTreeMap<String, OperationToolAlias>,
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
    pub arguments: serde_json::Value,
    pub turn_context: Option<crate::kernel::runtime_host::turn::TurnContextSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentKernelPendingToolCall {
    pub process_id: verlet_process::process::VerletProcessId,
    pub backend: verlet_process::process::VerletProcessBackend,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AgentKernelToolOutcome {
    Completed(Option<verlet_history::CanonicalMessage>),
    Pending(AgentKernelPendingToolCall),
}

/// Grace applied when the manifest binds no `runtime.cancellation_grace_ms`.
pub const DEFAULT_TOOL_CANCELLATION_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// Cancellation contract handed to every tool invocation.
///
/// `token` is a child of the turn cancellation token (the same family the
/// provider request race uses); an interrupt fires it. `grace` is the bound
/// manifest cancellation grace: it starts counting when the token fires, not
/// when the call starts. An invocation that has not settled within grace of
/// the token firing is abandoned via the spawn-shield discipline — the
/// detached invocation settles its own terminal record.
#[derive(Clone, Debug)]
pub struct ToolInvocationCancellation {
    token: tokio_util::sync::CancellationToken,
    grace: std::time::Duration,
}

impl ToolInvocationCancellation {
    pub fn new(token: tokio_util::sync::CancellationToken, grace: std::time::Duration) -> Self {
        Self { token, grace }
    }

    /// A contract whose token never fires, for paths that must run a call to
    /// settlement regardless of turn state (e.g. resumed witnessed calls).
    pub fn never() -> Self {
        Self::new(
            tokio_util::sync::CancellationToken::new(),
            DEFAULT_TOOL_CANCELLATION_GRACE,
        )
    }

    pub fn token(&self) -> &tokio_util::sync::CancellationToken {
        &self.token
    }

    pub fn grace(&self) -> std::time::Duration {
        self.grace
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Resolves with the witnessed outcome the executor must record if the
    /// invocation itself never observes the token: waits for the token, then
    /// for grace. Callers select this against the running invocation.
    pub async fn cancelled_then_grace_elapsed(&self) {
        self.token.cancelled().await;
        tokio::time::sleep(self.grace).await;
    }
}

/// The model-facing surface of a direct tool row whose operation declares an
/// envelope split (lexicon: bound parameter). Resolved where the alias is
/// built, from the published interface contract plus the binding's attach
/// configuration; the router consumes it without touching the record store.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolCallSurface {
    /// Derived via `ToolOperationInterface::model_input_schema`; what the
    /// model sees and what its arguments are validated against.
    pub model_input_schema: serde_json::Value,
    /// Envelope field the validated model arguments mount into.
    pub args_field: String,
    /// Bound parameter values pinned at attach; assembled into the envelope
    /// host-side. A model-supplied value for one of these keys is rejected.
    pub bound_values: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OperationToolAlias {
    pub tool_name: String,
    pub registered_name: String,
    pub operation_name: String,
    pub attach_event_id: Option<verlet_history::EventRecordId>,
    pub surface: Option<ToolCallSurface>,
}

#[async_trait::async_trait]
pub trait AgentKernelToolProvider: Send + Sync + 'static {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition>;

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>>;

    async fn invoke_tool_call_outcome(
        &self,
        call: AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<AgentKernelToolOutcome> {
        self.invoke_tool_call(call)
            .await
            .map(AgentKernelToolOutcome::Completed)
    }

    /// Cancellation-aware invocation. The default delegates to
    /// `invoke_tool_call_outcome` and never observes the token: the executor
    /// enforces grace and abandonment outside this call, so a provider that
    /// cannot stop early stays correct — it is merely abandoned at grace
    /// instead of acknowledging. Providers that can stop work early (process-
    /// backed tools) override this, kill whatever they spawned when the token
    /// fires, and return promptly with the partial result.
    async fn invoke_tool_call_cancellable(
        &self,
        call: AgentKernelToolCall,
        cancellation: ToolInvocationCancellation,
    ) -> crate::kernel::runtime_host::VerletResult<AgentKernelToolOutcome> {
        let _ = cancellation;
        self.invoke_tool_call_outcome(call).await
    }
}

impl AgentToolRouter {
    pub fn new(
        operation_registry: std::sync::Arc<
            verlet_operations::operation_registry::OperationRegistry,
        >,
    ) -> Self {
        Self {
            operation_registry,
            kernel_dispatch_overlay:
                verlet_operations::operation_registry::KernelDispatchOverlay::new(),
            kernel_tool_providers: Vec::new(),
            tool_aliases: std::collections::BTreeMap::new(),
        }
    }

    pub fn with_kernel_tool_provider(
        mut self,
        kernel_tool_provider: std::sync::Arc<dyn AgentKernelToolProvider>,
    ) -> Self {
        self.kernel_tool_providers.push(kernel_tool_provider);
        self
    }

    pub fn with_kernel_dispatch_overlay(
        mut self,
        overlay: verlet_operations::operation_registry::KernelDispatchOverlay,
    ) -> Self {
        self.kernel_dispatch_overlay = overlay;
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

    pub fn operation_registry(
        &self,
    ) -> std::sync::Arc<verlet_operations::operation_registry::OperationRegistry> {
        std::sync::Arc::clone(&self.operation_registry)
    }

    pub fn attach_event_id_for_tool_name(
        &self,
        tool_name: &str,
    ) -> Option<verlet_history::EventRecordId> {
        self.tool_aliases
            .get(tool_name)
            .and_then(|alias| alias.attach_event_id)
    }

    pub async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        let mut definitions = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        for alias in self.tool_aliases.values() {
            if names.insert(alias.tool_name.clone())
                && let Some(projection) = self
                    .projection_for_registered_operation(
                        &alias.registered_name,
                        &alias.operation_name,
                    )
                    .await
            {
                let input_schema = alias
                    .surface
                    .as_ref()
                    .map(|surface| surface.model_input_schema.clone())
                    .unwrap_or_else(|| input_schema_for_value_kind(&projection.input));
                definitions.push(verlet_provider::ToolDefinition::new(
                    alias.tool_name.clone(),
                    format!(
                        "Run Verlet operation {}/{}.",
                        projection.registered_name, projection.operation_name
                    ),
                    input_schema,
                ));
            }
        }
        for record in self.operation_registry.list().await {
            if is_kernel_record(&record) {
                continue;
            }
            for projection in record.projections().operations {
                if names.insert(projection.llm_tool.name.clone()) {
                    definitions.push(verlet_provider::ToolDefinition::new(
                        projection.llm_tool.name,
                        format!(
                            "Run Verlet operation {}/{}.",
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
        arguments: serde_json::Value,
    ) -> verlet_history::CanonicalMessage {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        match self
            .invoke_tool_call_message(None, &call_id, &tool_name, arguments)
            .await
        {
            Ok(message) => message,
            Err(err) => verlet_history::CanonicalMessage::tool_result(
                call_id,
                tool_name,
                err.to_string(),
                true,
            ),
        }
    }

    pub async fn invoke_tool_call_for_turn(
        &self,
        turn_context: &crate::kernel::runtime_host::turn::TurnContext,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: serde_json::Value,
    ) -> verlet_history::CanonicalMessage {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        match self
            .invoke_tool_call_message(Some(turn_context), &call_id, &tool_name, arguments)
            .await
        {
            Ok(message) => message,
            Err(err) => verlet_history::CanonicalMessage::tool_result(
                call_id,
                tool_name,
                err.to_string(),
                true,
            ),
        }
    }

    pub async fn invoke_tool_call_cancellable_for_turn(
        &self,
        turn_context: &crate::kernel::runtime_host::turn::TurnContext,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: serde_json::Value,
        cancellation: ToolInvocationCancellation,
    ) -> verlet_history::CanonicalMessage {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        match self
            .invoke_tool_call_outcome_message(
                Some(turn_context),
                &call_id,
                &tool_name,
                arguments,
                Some(cancellation),
            )
            .await
        {
            Ok(AgentKernelToolOutcome::Completed(Some(message))) => message,
            Ok(AgentKernelToolOutcome::Completed(None)) => {
                verlet_history::CanonicalMessage::tool_result(
                    call_id,
                    tool_name.clone(),
                    format!("unknown tool {tool_name:?}"),
                    true,
                )
            }
            Ok(AgentKernelToolOutcome::Pending(pending)) => {
                verlet_history::CanonicalMessage::tool_result(
                    call_id,
                    tool_name.clone(),
                    format!(
                        "tool {tool_name:?} returned pending process {} from {:?}, but this caller expects a completed tool result",
                        pending.process_id, pending.backend
                    ),
                    true,
                )
            }
            Err(err) => verlet_history::CanonicalMessage::tool_result(
                call_id,
                tool_name,
                err.to_string(),
                true,
            ),
        }
    }

    pub async fn invoke_tool_call_outcome(
        &self,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<AgentKernelToolOutcome> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        self.invoke_tool_call_outcome_message(None, &call_id, &tool_name, arguments, None)
            .await
    }

    pub async fn invoke_tool_call_outcome_for_turn(
        &self,
        turn_context: &crate::kernel::runtime_host::turn::TurnContext,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<AgentKernelToolOutcome> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        self.invoke_tool_call_outcome_message(
            Some(turn_context),
            &call_id,
            &tool_name,
            arguments,
            None,
        )
        .await
    }

    pub async fn route_tool_call(
        &self,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
    ) -> crate::kernel::runtime_host::VerletResult<RoutedAgentToolCall> {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        let projection = self
            .projection_for_tool_name(&tool_name)
            .await?
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                    "unknown tool {tool_name:?}"
                ))
            })?;
        Ok(RoutedAgentToolCall {
            call_id,
            tool_name,
            registered_name: projection.registered_name,
            operation_name: projection.operation_name,
        })
    }

    async fn invoke_tool_call_message(
        &self,
        turn_context: Option<&crate::kernel::runtime_host::turn::TurnContext>,
        call_id: &str,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::CanonicalMessage> {
        match self
            .invoke_tool_call_outcome_message(turn_context, call_id, tool_name, arguments, None)
            .await?
        {
            AgentKernelToolOutcome::Completed(Some(message)) => Ok(message),
            AgentKernelToolOutcome::Completed(None) => {
                Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    format!("unknown tool {tool_name:?}"),
                ))
            }
            AgentKernelToolOutcome::Pending(pending) => Err(
                crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                    "tool {tool_name:?} returned pending process {} from {:?}, but this caller expects a completed tool result",
                    pending.process_id, pending.backend
                )),
            ),
        }
    }

    async fn invoke_tool_call_outcome_message(
        &self,
        turn_context: Option<&crate::kernel::runtime_host::turn::TurnContext>,
        call_id: &str,
        tool_name: &str,
        arguments: serde_json::Value,
        cancellation: Option<ToolInvocationCancellation>,
    ) -> crate::kernel::runtime_host::VerletResult<AgentKernelToolOutcome> {
        if let Some(projection) = self.projection_for_tool_name(tool_name).await? {
            if projection.abi.has_hidden_durable_sink() {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    format!("tool {tool_name:?} has a hidden durable sink"),
                ));
            }
            let arguments = match self
                .tool_aliases
                .get(tool_name)
                .and_then(|alias| alias.surface.as_ref())
            {
                Some(surface) => assemble_surface_envelope(call_id, tool_name, surface, arguments)?,
                None => arguments,
            };
            let input = encode_tool_input(call_id, tool_name, &projection, arguments)?;
            let operation_registry =
                verlet_operations::operation_registry::ScopedOperationRegistry::new(
                    std::sync::Arc::clone(&self.operation_registry),
                    self.kernel_dispatch_overlay.clone(),
                );
            let process = operation_registry
                .invoke_process_with_kernel_metadata(
                    &projection.registered_name,
                    &projection.operation_name,
                    input,
                    std::collections::BTreeMap::from([
                        (
                            "cooldis.tool_call_id".to_string(),
                            serde_json::Value::String(call_id.to_string()),
                        ),
                        (
                            "cooldis.tool_name".to_string(),
                            serde_json::Value::String(tool_name.to_string()),
                        ),
                    ]),
                )
                .await?;
            let output = process.output();
            return Ok(AgentKernelToolOutcome::Completed(Some(
                verlet_history::CanonicalMessage::tool_result(
                    call_id,
                    tool_name,
                    decode_tool_output(&projection.output, &output.stdout),
                    false,
                ),
            )));
        }

        if let Some(kernel_tool_provider) = self.kernel_tool_provider_for_name(tool_name).await {
            let call = AgentKernelToolCall {
                call_id: call_id.to_string(),
                tool_name: tool_name.to_string(),
                arguments,
                turn_context: turn_context
                    .map(crate::kernel::runtime_host::turn::TurnContext::snapshot),
            };
            let outcome = match cancellation {
                Some(cancellation) => {
                    kernel_tool_provider
                        .invoke_tool_call_cancellable(call, cancellation)
                        .await?
                }
                None => kernel_tool_provider.invoke_tool_call_outcome(call).await?,
            };
            match outcome {
                AgentKernelToolOutcome::Completed(Some(_)) | AgentKernelToolOutcome::Pending(_) => {
                    return Ok(outcome);
                }
                AgentKernelToolOutcome::Completed(None) => {}
            }
        }

        Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!("unknown tool {tool_name:?}"),
        ))
    }

    async fn kernel_tool_provider_for_name(
        &self,
        tool_name: &str,
    ) -> Option<std::sync::Arc<dyn AgentKernelToolProvider>> {
        for kernel_tool_provider in &self.kernel_tool_providers {
            if kernel_tool_provider
                .tool_definitions()
                .await
                .into_iter()
                .any(|tool| tool.name == tool_name)
            {
                return Some(std::sync::Arc::clone(kernel_tool_provider));
            }
        }
        None
    }

    async fn projection_for_tool_name(
        &self,
        tool_name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_operations::OperationProjection>>
    {
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
    ) -> Option<verlet_operations::OperationProjection> {
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

fn is_kernel_record(record: &verlet_operations::RegisteredOperation) -> bool {
    record
        .metadata
        .get(crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND)
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind == crate::operations::kernel_packages::KERNEL_RUNTIME_KIND)
}

fn input_schema_for_value_kind(kind: &verlet_abi::WasmOperationValueKind) -> serde_json::Value {
    match kind {
        verlet_abi::WasmOperationValueKind::Json => serde_json::json!({
            "type": "object",
            "additionalProperties": true
        }),
        verlet_abi::WasmOperationValueKind::Text => {
            text_input_schema("UTF-8 text input for the operation.")
        }
        verlet_abi::WasmOperationValueKind::Bytes => {
            text_input_schema("UTF-8 text that Verlet passes as operation input bytes.")
        }
    }
}

fn text_input_schema(description: &str) -> serde_json::Value {
    serde_json::json!({
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

/// Assembles the host-side envelope for a surface-declared operation:
/// rejects model-supplied values for bound parameters, validates the model
/// arguments against `surface.model_input_schema`, mounts them at
/// `surface.args_field`, and merges `surface.bound_values` at the top level.
/// Validation failures return the schema error as tool-error text so the
/// model can correct itself; a bound-parameter collision is always an error.
fn assemble_surface_envelope(
    call_id: &str,
    tool_name: &str,
    surface: &ToolCallSurface,
    arguments: serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
    let _ = (call_id, tool_name, surface, &arguments);
    todo!("EMO-615: validate model args, mount at args_field, merge bound values")
}

fn encode_tool_input(
    call_id: &str,
    tool_name: &str,
    projection: &verlet_operations::OperationProjection,
    arguments: serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<Vec<u8>> {
    match projection.input {
        verlet_abi::WasmOperationValueKind::Json => {
            if !arguments.is_object() {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    format!("tool {tool_name:?} call {call_id} requires object arguments"),
                ));
            }
            serde_json::to_vec(&arguments).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                    "tool {tool_name:?} call {call_id} has invalid JSON input: {err}"
                ))
            })
        }
        verlet_abi::WasmOperationValueKind::Text | verlet_abi::WasmOperationValueKind::Bytes => {
            let input = match arguments {
                serde_json::Value::String(value) => value,
                serde_json::Value::Object(mut object) => object
                    .remove("input")
                    .and_then(|value| value.as_str().map(ToString::to_string))
                    .ok_or_else(|| {
                        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                            "tool {tool_name:?} call {call_id} requires a string input field"
                        ))
                    })?,
                _ => {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        format!("tool {tool_name:?} call {call_id} requires object arguments"),
                    ));
                }
            };
            Ok(input.into_bytes())
        }
    }
}

fn decode_tool_output(kind: &verlet_abi::WasmOperationValueKind, bytes: &[u8]) -> String {
    match kind {
        verlet_abi::WasmOperationValueKind::Json => {
            serde_json::from_slice::<serde_json::Value>(bytes)
                .ok()
                .and_then(|value| serde_json::to_string(&value).ok())
                .unwrap_or_else(|| String::from_utf8_lossy(bytes).to_string())
        }
        verlet_abi::WasmOperationValueKind::Text | verlet_abi::WasmOperationValueKind::Bytes => {
            String::from_utf8_lossy(bytes).to_string()
        }
    }
}

#[cfg(test)]
mod tests;
