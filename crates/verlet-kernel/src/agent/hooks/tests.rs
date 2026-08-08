#[tokio::test]
async fn command_hook_handler_reads_json_stdin_and_returns_pre_tool_output() {
    let hook = crate::agent::hooks::CommandHookHandler::new(
            "rewrite",
            crate::agent::hooks::HookEventName::PreToolUse,
            r#"cat >/dev/null; printf '%s' '{"updated_input":{"input":"rewritten"},"additional_context":"ctx"}'"#,
        )
        .with_matcher("echo_search");
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let turn_context = crate::kernel::runtime_host::turn::TurnContext::new(
        verlet_runtime_contracts::ThreadContext::root(coordinates),
        "turn-1",
        &crate::kernel::runtime_host::turn::TurnInput::text("hello"),
        tokio_util::sync::CancellationToken::new(),
    );
    let request = crate::agent::hooks::PreToolUseHookRequest {
        turn_context: turn_context.snapshot(),
        call_id: "call_1".to_string(),
        tool_name: "echo_search".to_string(),
        arguments: serde_json::json!({"input":"original"}),
    };
    let outcome = crate::agent::hooks::HookPipeline::new()
        .with_command_handler(hook)
        .run_pre_tool_use(request, |_| {})
        .await;

    assert_eq!(outcome.records.len(), 1);
    assert_eq!(
        outcome.records[0].status,
        crate::agent::hooks::HookRunStatus::Completed
    );
    assert_eq!(
        outcome.updated_input,
        Some(serde_json::json!({"input":"rewritten"}))
    );
    assert_eq!(outcome.additional_contexts, vec!["ctx"]);
}

#[test]
fn hook_request_serializes_stable_shape() {
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let request = crate::agent::hooks::HookRequest::SessionStart(
        crate::agent::hooks::SessionStartHookRequest {
            coordinates: coordinates.clone(),
            parent_thread_id: None,
            source: "startup".to_string(),
            cwd: None,
            provider: "openai".to_string(),
            model: "gpt-test".to_string(),
            permission_profile: None,
        },
    );

    assert_eq!(
        serde_json::to_value(request).unwrap(),
        serde_json::json!({
            "hook_event_name": "session_start",
            "coordinates": coordinates,
            "parent_thread_id": null,
            "source": "startup",
            "provider": "openai",
            "model": "gpt-test"
        })
    );
}
