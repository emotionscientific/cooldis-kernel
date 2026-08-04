use super::*;
use crate::{
    HookEventName, HookHandler, HookHandlerOutput, HookHandlerSpec, HookRequest, OperationRegistry,
    ThreadContext, ThreadCoordinates, ToolDefinition, TurnInput,
};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

struct StaticHookHandler {
    spec: HookHandlerSpec,
    output: HookHandlerOutput,
}

#[async_trait]
impl HookHandler for StaticHookHandler {
    fn spec(&self) -> HookHandlerSpec {
        self.spec.clone()
    }

    async fn run(&self, _request: HookRequest) -> VerletResult<HookHandlerOutput> {
        Ok(self.output.clone())
    }
}

struct EchoKernelToolProvider {
    seen_arguments: Mutex<Vec<Value>>,
}

struct DenyGate;

#[async_trait]
impl ToolPermissionGate for DenyGate {
    async fn check(&self, _request: ToolPermissionRequest) -> ToolPermissionDecision {
        ToolPermissionDecision::Deny {
            reason: "denied by gate".to_string(),
        }
    }
}

#[async_trait]
impl crate::AgentKernelToolProvider for EchoKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "echo",
            "Echo input",
            serde_json::json!({"type":"object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::AgentKernelToolCall,
    ) -> VerletResult<Option<CanonicalMessage>> {
        self.seen_arguments.lock().unwrap().push(call.arguments);
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "original",
            false,
        )))
    }
}

#[tokio::test]
async fn interceptor_applies_pre_rewrite_and_post_replacement() {
    let provider = Arc::new(EchoKernelToolProvider {
        seen_arguments: Mutex::new(Vec::new()),
    });
    let kernel_provider: Arc<dyn crate::AgentKernelToolProvider> = provider.clone();
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(kernel_provider),
    );
    let pre_hook = Arc::new(StaticHookHandler {
        spec: HookHandlerSpec {
            id: "pre".to_string(),
            event_name: HookEventName::PreToolUse,
            matcher: Some("echo".to_string()),
        },
        output: HookHandlerOutput {
            updated_input: Some(serde_json::json!({"input":"rewritten"})),
            additional_context: Some("pre context".to_string()),
            ..HookHandlerOutput::default()
        },
    });
    let post_hook = Arc::new(StaticHookHandler {
        spec: HookHandlerSpec {
            id: "post".to_string(),
            event_name: HookEventName::PostToolUse,
            matcher: Some("echo".to_string()),
        },
        output: HookHandlerOutput {
            replacement_output: Some("replacement".to_string()),
            feedback: Some("feedback".to_string()),
            ..HookHandlerOutput::default()
        },
    });
    let hook_pipeline = Arc::new(
        HookPipeline::new()
            .with_handler(pre_hook)
            .with_handler(post_hook),
    );
    let turn_context = test_turn_context();
    let outcome = ToolExecutionInterceptor::new(router)
        .with_hook_pipeline(Some(hook_pipeline))
        .execute(
            ToolExecutionRequest {
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
    assert_eq!(text_from_message(&outcome.result), "replacement");
    assert_eq!(outcome.pre_model_contexts, vec!["pre context"]);
    assert_eq!(outcome.post_model_contexts, vec!["feedback"]);
    assert_eq!(outcome.hook_records.len(), 2);
    assert_eq!(
        outcome.permission_decision,
        Some(ToolPermissionDecision::Allow)
    );
}

#[tokio::test]
async fn interceptor_permission_gate_can_deny_before_router_invocation() {
    let provider = Arc::new(EchoKernelToolProvider {
        seen_arguments: Mutex::new(Vec::new()),
    });
    let kernel_provider: Arc<dyn crate::AgentKernelToolProvider> = provider.clone();
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(kernel_provider),
    );
    let turn_context = test_turn_context();
    let outcome = ToolExecutionInterceptor::new(router)
        .with_permission_gate(Arc::new(DenyGate))
        .execute(
            ToolExecutionRequest {
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
        CanonicalMessage::ToolResult { is_error: true, .. }
    ));
    assert_eq!(text_from_message(&outcome.result), "denied by gate");
    assert_eq!(
        outcome.permission_decision,
        Some(ToolPermissionDecision::Deny {
            reason: "denied by gate".to_string()
        })
    );
}

fn test_turn_context() -> crate::TurnContext {
    crate::TurnContext::new(
        ThreadContext::root(ThreadCoordinates::new("tenant_a", "user_1", "session_1")),
        "turn-1",
        &TurnInput::text("hello"),
        CancellationToken::new(),
    )
}
