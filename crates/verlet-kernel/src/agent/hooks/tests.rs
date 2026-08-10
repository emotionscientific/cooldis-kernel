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

#[cfg(unix)]
#[tokio::test]
async fn command_hook_handler_prefers_injected_shell() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = std::env::temp_dir().join(format!(
        "verlet-injected-hook-shell-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let shell = root.join("injected-shell");
    std::fs::write(
        &shell,
        "#!/bin/sh\nprintf '%s' '{\"additional_context\":\"injected shell\"}'\n",
    )
    .unwrap();
    std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o700)).unwrap();

    // A freshly written script can fail exec with ETXTBSY on Linux when a
    // concurrent test's child briefly inherits the writing descriptor across
    // its fork/exec window. Spin a discarded invocation until the file is
    // executable so the pipeline run below cannot hit the race.
    for attempt in 0..500u32 {
        match std::process::Command::new(&shell)
            .stdout(std::process::Stdio::null())
            .status()
        {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::ExecutableFileBusy => {
                assert!(attempt < 499, "injected shell stayed busy: {error}");
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(error) => panic!("injected shell failed to exec: {error}"),
        }
    }

    let hook = crate::agent::hooks::CommandHookHandler::new(
        "injected-shell",
        crate::agent::hooks::HookEventName::SessionStart,
        "exit 99",
    );
    let request = crate::agent::hooks::SessionStartHookRequest {
        coordinates: verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
        parent_thread_id: None,
        source: "test".to_string(),
        cwd: None,
        provider: "test".to_string(),
        model: "test".to_string(),
        permission_profile: None,
    };

    let outcome = crate::agent::hooks::HookPipeline::new()
        .with_shell(Some(shell.to_string_lossy().into_owned()))
        .with_handler(std::sync::Arc::new(hook))
        .run_session_start(request, |_| {})
        .await;

    assert_eq!(
        outcome.additional_contexts,
        ["injected shell"],
        "hook records: {:?}",
        outcome.records
    );
    let _ = std::fs::remove_dir_all(root);
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
