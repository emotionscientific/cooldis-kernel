#[derive(Clone)]
pub struct ToolExecutionInterceptor {
    tool_router: std::sync::Arc<crate::agent::agent_tool_router::AgentToolRouter>,
    hook_pipeline: Option<std::sync::Arc<crate::agent::hooks::HookPipeline>>,
    permission_gate: std::sync::Arc<dyn ToolPermissionGate>,
}

#[derive(Clone, Debug)]
pub struct ToolExecutionRequest<'a> {
    pub turn_context: &'a crate::kernel::runtime_host::turn::TurnContext,
    pub call_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolExecutionOutcome {
    pub result: verlet_history::CanonicalMessage,
    pub hook_records: Vec<crate::agent::hooks::HookRunRecord>,
    pub pre_model_contexts: Vec<String>,
    pub post_model_contexts: Vec<String>,
    pub permission_decision: Option<ToolPermissionDecision>,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolPermissionRequest {
    pub turn_context: crate::kernel::runtime_host::turn::TurnContextSnapshot,
    pub call_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ToolPermissionDecision {
    Allow,
    Deny { reason: String },
}

#[async_trait::async_trait]
pub trait ToolPermissionGate: Send + Sync + 'static {
    async fn check(&self, request: ToolPermissionRequest) -> ToolPermissionDecision;
}

#[derive(Clone, Default)]
pub struct AllowAllToolPermissionGate;

#[async_trait::async_trait]
impl ToolPermissionGate for AllowAllToolPermissionGate {
    async fn check(&self, _request: ToolPermissionRequest) -> ToolPermissionDecision {
        ToolPermissionDecision::Allow
    }
}

impl ToolExecutionInterceptor {
    pub fn new(
        tool_router: std::sync::Arc<crate::agent::agent_tool_router::AgentToolRouter>,
    ) -> Self {
        Self {
            tool_router,
            hook_pipeline: None,
            permission_gate: std::sync::Arc::new(AllowAllToolPermissionGate),
        }
    }

    pub fn with_hook_pipeline(
        mut self,
        hook_pipeline: Option<std::sync::Arc<crate::agent::hooks::HookPipeline>>,
    ) -> Self {
        self.hook_pipeline = hook_pipeline;
        self
    }

    pub fn with_permission_gate(
        mut self,
        permission_gate: std::sync::Arc<dyn ToolPermissionGate>,
    ) -> Self {
        self.permission_gate = permission_gate;
        self
    }

    pub async fn execute(
        &self,
        request: ToolExecutionRequest<'_>,
        on_hook_started: impl FnMut(&crate::agent::hooks::HookHandlerSpec),
    ) -> crate::kernel::runtime_host::VerletResult<ToolExecutionOutcome> {
        self.execute_with_witnessing(request, on_hook_started, |_| std::future::ready(Ok(())))
            .await
    }

    pub async fn execute_with_witnessing<W, Fut>(
        &self,
        request: ToolExecutionRequest<'_>,
        on_hook_started: impl FnMut(&crate::agent::hooks::HookHandlerSpec),
        witness_hook_mutations: W,
    ) -> crate::kernel::runtime_host::VerletResult<ToolExecutionOutcome>
    where
        W: FnMut(Vec<crate::agent::hooks::HookMutationWitness>) -> Fut,
        Fut: std::future::Future<Output = crate::kernel::runtime_host::VerletResult<()>>,
    {
        self.execute_with_witnessing_cancellable(
            request,
            crate::agent::agent_tool_router::ToolInvocationCancellation::never(),
            on_hook_started,
            witness_hook_mutations,
        )
        .await
    }

    pub async fn execute_with_witnessing_cancellable<W, Fut>(
        &self,
        request: ToolExecutionRequest<'_>,
        cancellation: crate::agent::agent_tool_router::ToolInvocationCancellation,
        mut on_hook_started: impl FnMut(&crate::agent::hooks::HookHandlerSpec),
        mut witness_hook_mutations: W,
    ) -> crate::kernel::runtime_host::VerletResult<ToolExecutionOutcome>
    where
        W: FnMut(Vec<crate::agent::hooks::HookMutationWitness>) -> Fut,
        Fut: std::future::Future<Output = crate::kernel::runtime_host::VerletResult<()>>,
    {
        let started_at = std::time::Instant::now();
        let mut hook_records = Vec::new();
        let mut pre_model_contexts = Vec::new();
        let mut post_model_contexts = Vec::new();
        let mut permission_decision = None;
        let mut arguments = request.arguments;

        if let Some(hook_pipeline) = &self.hook_pipeline {
            let outcome = hook_pipeline
                .run_pre_tool_use(
                    crate::agent::hooks::PreToolUseHookRequest {
                        turn_context: request.turn_context.snapshot(),
                        call_id: request.call_id.clone(),
                        tool_name: request.tool_name.clone(),
                        arguments: arguments.clone(),
                    },
                    &mut on_hook_started,
                )
                .await;
            hook_records.extend(outcome.records);
            if !outcome.mutation_witnesses.is_empty() {
                witness_hook_mutations(outcome.mutation_witnesses).await?;
            }
            pre_model_contexts.extend(outcome.additional_contexts);
            if outcome.should_block {
                let reason = outcome
                    .block_reason
                    .unwrap_or_else(|| "PreToolUse hook blocked tool execution".to_string());
                return Ok(ToolExecutionOutcome {
                    result: verlet_history::CanonicalMessage::tool_result(
                        request.call_id,
                        request.tool_name,
                        reason,
                        true,
                    ),
                    hook_records,
                    pre_model_contexts,
                    post_model_contexts,
                    permission_decision,
                    duration_ms: elapsed_ms(started_at),
                });
            }
            if let Some(updated_input) = outcome.updated_input {
                arguments = updated_input;
            }
        }

        match self
            .permission_gate
            .check(ToolPermissionRequest {
                turn_context: request.turn_context.snapshot(),
                call_id: request.call_id.clone(),
                tool_name: request.tool_name.clone(),
                arguments: arguments.clone(),
            })
            .await
        {
            ToolPermissionDecision::Allow => {
                permission_decision = Some(ToolPermissionDecision::Allow);
            }
            ToolPermissionDecision::Deny { reason } => {
                permission_decision = Some(ToolPermissionDecision::Deny {
                    reason: reason.clone(),
                });
                return Ok(ToolExecutionOutcome {
                    result: verlet_history::CanonicalMessage::tool_result(
                        request.call_id,
                        request.tool_name,
                        reason,
                        true,
                    ),
                    hook_records,
                    pre_model_contexts,
                    post_model_contexts,
                    permission_decision,
                    duration_ms: elapsed_ms(started_at),
                });
            }
        }

        let result = self
            .tool_router
            .invoke_tool_call_cancellable_for_turn(
                request.turn_context,
                request.call_id.clone(),
                request.tool_name.clone(),
                arguments.clone(),
                cancellation,
            )
            .await;
        let success = tool_result_success(&result);
        let output = text_from_message(&result);
        let mut result = result;

        if success && let Some(hook_pipeline) = &self.hook_pipeline {
            let outcome = hook_pipeline
                .run_post_tool_use(
                    crate::agent::hooks::PostToolUseHookRequest {
                        turn_context: request.turn_context.snapshot(),
                        call_id: request.call_id.clone(),
                        tool_name: request.tool_name.clone(),
                        arguments,
                        output,
                        success,
                    },
                    &mut on_hook_started,
                )
                .await;
            hook_records.extend(outcome.records);
            if !outcome.mutation_witnesses.is_empty() {
                witness_hook_mutations(outcome.mutation_witnesses).await?;
            }
            post_model_contexts.extend(outcome.additional_contexts);
            if let Some(feedback) = outcome.feedback {
                post_model_contexts.push(feedback);
            }
            if let Some(replacement_output) = outcome.replacement_output {
                result = replace_tool_result_output(result, replacement_output);
            }
        }

        Ok(ToolExecutionOutcome {
            result,
            hook_records,
            pre_model_contexts,
            post_model_contexts,
            permission_decision,
            duration_ms: elapsed_ms(started_at),
        })
    }
}

fn elapsed_ms(started_at: std::time::Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn tool_result_success(message: &verlet_history::CanonicalMessage) -> bool {
    match message {
        verlet_history::CanonicalMessage::ToolResult { is_error, .. } => !is_error,
        _ => false,
    }
}

fn replace_tool_result_output(
    message: verlet_history::CanonicalMessage,
    replacement_output: String,
) -> verlet_history::CanonicalMessage {
    match message {
        verlet_history::CanonicalMessage::ToolResult {
            tool_call_id,
            tool_name,
            is_error,
            cache_control,
            timestamp_ms,
            ..
        } => verlet_history::CanonicalMessage::ToolResult {
            tool_call_id,
            tool_name,
            content: vec![verlet_history::CanonicalContent::text(replacement_output)],
            is_error,
            cache_control,
            timestamp_ms,
        },
        other => other,
    }
}

fn text_from_message(message: &verlet_history::CanonicalMessage) -> String {
    match message {
        verlet_history::CanonicalMessage::Assistant { content, .. }
        | verlet_history::CanonicalMessage::ToolResult { content, .. }
        | verlet_history::CanonicalMessage::User { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
    }
}

#[cfg(test)]
mod tests;
