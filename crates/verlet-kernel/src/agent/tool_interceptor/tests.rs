struct StaticHookHandler {
    spec: crate::HookHandlerSpec,
    output: crate::HookHandlerOutput,
}

#[async_trait::async_trait]
impl crate::HookHandler for StaticHookHandler {
    fn spec(&self) -> crate::HookHandlerSpec {
        self.spec.clone()
    }

    async fn run(
        &self,
        _request: crate::HookRequest,
    ) -> crate::VerletResult<crate::HookHandlerOutput> {
        Ok(self.output.clone())
    }
}

struct EchoKernelToolProvider {
    seen_arguments: std::sync::Mutex<Vec<serde_json::Value>>,
}

struct DenyGate;

#[async_trait::async_trait]
impl crate::agent::tool_interceptor::ToolPermissionGate for DenyGate {
    async fn check(
        &self,
        _request: crate::agent::tool_interceptor::ToolPermissionRequest,
    ) -> crate::agent::tool_interceptor::ToolPermissionDecision {
        crate::agent::tool_interceptor::ToolPermissionDecision::Deny {
            reason: "denied by gate".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl crate::AgentKernelToolProvider for EchoKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<crate::ToolDefinition> {
        vec![crate::ToolDefinition::new(
            "echo",
            "Echo input",
            serde_json::json!({"type":"object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::AgentKernelToolCall,
    ) -> crate::VerletResult<Option<crate::CanonicalMessage>> {
        self.seen_arguments.lock().unwrap().push(call.arguments);
        Ok(Some(crate::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "original",
            false,
        )))
    }
}

#[tokio::test]
async fn interceptor_applies_pre_rewrite_and_post_replacement() {
    let provider = std::sync::Arc::new(EchoKernelToolProvider {
        seen_arguments: std::sync::Mutex::new(Vec::new()),
    });
    let kernel_provider: std::sync::Arc<dyn crate::AgentKernelToolProvider> = provider.clone();
    let router = std::sync::Arc::new(
        crate::AgentToolRouter::new(std::sync::Arc::new(crate::OperationRegistry::new()))
            .with_kernel_tool_provider(kernel_provider),
    );
    let pre_hook = std::sync::Arc::new(StaticHookHandler {
        spec: crate::HookHandlerSpec {
            id: "pre".to_string(),
            event_name: crate::HookEventName::PreToolUse,
            matcher: Some("echo".to_string()),
        },
        output: crate::HookHandlerOutput {
            updated_input: Some(serde_json::json!({"input":"rewritten"})),
            additional_context: Some("pre context".to_string()),
            ..crate::HookHandlerOutput::default()
        },
    });
    let post_hook = std::sync::Arc::new(StaticHookHandler {
        spec: crate::HookHandlerSpec {
            id: "post".to_string(),
            event_name: crate::HookEventName::PostToolUse,
            matcher: Some("echo".to_string()),
        },
        output: crate::HookHandlerOutput {
            replacement_output: Some("replacement".to_string()),
            feedback: Some("feedback".to_string()),
            ..crate::HookHandlerOutput::default()
        },
    });
    let hook_pipeline = std::sync::Arc::new(
        crate::HookPipeline::new()
            .with_handler(pre_hook)
            .with_handler(post_hook),
    );
    let turn_context = test_turn_context();
    let outcome = crate::agent::tool_interceptor::ToolExecutionInterceptor::new(router)
        .with_hook_pipeline(Some(hook_pipeline))
        .execute(
            crate::agent::tool_interceptor::ToolExecutionRequest {
                turn_context: &turn_context,
                call_id: "call_1".to_string(),
                tool_name: "echo".to_string(),
                arguments: serde_json::json!({"input":"original"}),
            },
            |_| {},
        )
        .await
        .unwrap();

    assert_eq!(
        provider.seen_arguments.lock().unwrap().as_slice(),
        &[serde_json::json!({"input":"rewritten"})]
    );
    assert_eq!(
        crate::agent::tool_interceptor::text_from_message(&outcome.result),
        "replacement"
    );
    assert_eq!(outcome.pre_model_contexts, vec!["pre context"]);
    assert_eq!(outcome.post_model_contexts, vec!["feedback"]);
    assert_eq!(outcome.hook_records.len(), 2);
    assert_eq!(
        outcome.permission_decision,
        Some(crate::agent::tool_interceptor::ToolPermissionDecision::Allow)
    );
}

#[tokio::test]
async fn interceptor_permission_gate_can_deny_before_router_invocation() {
    let provider = std::sync::Arc::new(EchoKernelToolProvider {
        seen_arguments: std::sync::Mutex::new(Vec::new()),
    });
    let kernel_provider: std::sync::Arc<dyn crate::AgentKernelToolProvider> = provider.clone();
    let router = std::sync::Arc::new(
        crate::AgentToolRouter::new(std::sync::Arc::new(crate::OperationRegistry::new()))
            .with_kernel_tool_provider(kernel_provider),
    );
    let turn_context = test_turn_context();
    let outcome = crate::agent::tool_interceptor::ToolExecutionInterceptor::new(router)
        .with_permission_gate(std::sync::Arc::new(DenyGate))
        .execute(
            crate::agent::tool_interceptor::ToolExecutionRequest {
                turn_context: &turn_context,
                call_id: "call_1".to_string(),
                tool_name: "echo".to_string(),
                arguments: serde_json::json!({"input":"original"}),
            },
            |_| {},
        )
        .await
        .unwrap();

    assert!(provider.seen_arguments.lock().unwrap().is_empty());
    assert!(matches!(
        &outcome.result,
        crate::CanonicalMessage::ToolResult { is_error: true, .. }
    ));
    assert_eq!(
        crate::agent::tool_interceptor::text_from_message(&outcome.result),
        "denied by gate"
    );
    assert_eq!(
        outcome.permission_decision,
        Some(
            crate::agent::tool_interceptor::ToolPermissionDecision::Deny {
                reason: "denied by gate".to_string()
            }
        )
    );
}

fn test_turn_context() -> crate::TurnContext {
    crate::TurnContext::new(
        crate::ThreadContext::root(crate::ThreadCoordinates::new(
            "tenant_a",
            "user_1",
            "session_1",
        )),
        "turn-1",
        &crate::TurnInput::text("hello"),
        tokio_util::sync::CancellationToken::new(),
    )
}
