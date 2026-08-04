mod support;

use serde_json::json;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use verlet::{
    AgentKernelToolCall, AgentKernelToolOutcome, AgentKernelToolProvider, AgentLoopConfig,
    AgentLoopFactory, AgentToolRouter, BashExecutionPolicy, CanonicalMessage, CanonicalStopReason,
    CanonicalUsage, CompactionTrigger, EventKind, EventStore, EventStreamId, HookEventName,
    HookHandler, HookHandlerOutput, HookRequest, HookRunStatus, InMemorySessionStore,
    KernelOperationRegistration, OperationRegistry, OperationToolAlias, ProviderApi,
    ProviderClient, ProviderRequest, ProviderResponse, ProviderResult, ProviderStreamEvent,
    RuntimeEventKind, RuntimeHost, RuntimeModelRequestMode, RuntimeModelRequestPurpose,
    RuntimePermissionDecision, RuntimeToolLogLevel, SessionContext, SqliteSessionStore,
    THREAD_SPAWN_OPERATION, ThreadCoordinates, ThreadEvent, ThreadSignal, ThreadSignalKind,
    ThreadStatus, ThreadTopology, ToolCallCancellation, ToolCallCompletedPayload, ToolDefinition,
    ToolInvocationCancellation, TurnSubmissionMode, VerletResult, VirtualBashRuntimeConfig,
    VirtualBashRuntimeFactory, verlet_threads_kernel_package,
};

use support::{
    DenyGate, FaultingProviderClient, FaultingRuntimeStore, RootProviderChildEchoFactory,
    ScriptedProviderClient, ScriptedProviderStep, StaticHookHandler, assert_event_order,
    collect_until_cancelled, collect_until_compaction, collect_until_failed, collect_until_output,
    echo_router, hook_pipeline, response_text, response_tool_call, response_tool_call_with_id,
    streaming_provider_factory, text_from_message,
};

#[tokio::test]
async fn provider_loop_exposes_bash_tool_with_structured_output() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("bash", json!({"command": "printf 'VERLET\\n'"})),
        response_text("bash done"),
    ]));
    let factory =
        provider_factory(Arc::clone(&client)).with_bash_tool(VirtualBashRuntimeConfig::default());
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-bash",
        "use bash",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "bash done").await;

    let output = bash_tool_result_json(trace.runtime_events(), "call_1|fc_1");
    assert_eq!(output["stdout"], "VERLET\n");
    assert_eq!(output["stderr"], "");
    assert_eq!(output["exit_code"], 0);
    assert_eq!(output["stdout_truncated"], false);
    assert_eq!(output["stderr_truncated"], false);
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolLog {
            call_id,
            tool_name,
            metadata,
            ..
        } if call_id == "call_1|fc_1"
            && tool_name == "bash"
            && metadata.get("success").map(String::as_str) == Some("true")
    )));
}

#[tokio::test]
async fn provider_loop_bash_tool_keeps_virtual_filesystem_state_across_calls() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call_with_id(
            "call_write",
            "bash",
            json!({"command": "echo persisted > /workspace/state.txt"}),
        ),
        response_tool_call_with_id(
            "call_read",
            "bash",
            json!({"command": "cat /workspace/state.txt"}),
        ),
        response_text("state done"),
    ]));
    let factory =
        provider_factory(Arc::clone(&client)).with_bash_tool(VirtualBashRuntimeConfig::default());
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-bash-state",
        "use bash twice",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "state done").await;

    let write_output = bash_tool_result_json(trace.runtime_events(), "call_write");
    assert_eq!(write_output["exit_code"], 0);
    let read_output = bash_tool_result_json(trace.runtime_events(), "call_read");
    assert_eq!(read_output["stdout"], "persisted\n");
    assert_eq!(read_output["exit_code"], 0);
}

#[tokio::test]
async fn provider_loop_bash_tool_can_author_host_file_when_host_route_is_configured() {
    let host_root = std::env::temp_dir().join(format!(
        "verlet-provider-host-author-{}",
        uuid::Uuid::now_v7()
    ));
    tokio::fs::create_dir_all(&host_root).await.unwrap();
    let command = r#"cat > glm_authored.py <<'PY'
print("hello from model host author")
PY
"#;
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call_with_id("call_author", "bash", json!({"command": command})),
        response_text("host author done"),
    ]));
    let bash_config = VirtualBashRuntimeConfig::default()
        .with_execution_policy(BashExecutionPolicy::host_always())
        .with_host_bash_executor(&host_root);
    let factory = provider_factory(Arc::clone(&client)).with_bash_tool(bash_config);
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-host-author",
        "author a host file with bash",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "host author done").await;

    let output = bash_tool_result_json(trace.runtime_events(), "call_author");
    assert_eq!(output["exit_code"], 0);
    assert_eq!(
        tokio::fs::read_to_string(host_root.join("glm_authored.py"))
            .await
            .unwrap(),
        "print(\"hello from model host author\")\n"
    );
    tokio::fs::remove_dir_all(host_root).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn interrupting_host_bash_acknowledges_the_call_and_kills_its_process_tree() {
    let host_root = std::env::temp_dir().join(format!(
        "verlet-provider-host-cancel-{}",
        uuid::Uuid::now_v7()
    ));
    tokio::fs::create_dir_all(&host_root).await.unwrap();
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call(
            "bash",
            json!({
                "command": "(trap '' TERM; while :; do sleep 1; done) & echo $! > child.pid; wait"
            }),
        ),
    ]));
    let bash_config = VirtualBashRuntimeConfig::default()
        .with_execution_policy(BashExecutionPolicy::host_always())
        .with_host_bash_executor(&host_root);
    let host = RuntimeHost::new(Arc::new(
        provider_factory(Arc::clone(&client)).with_bash_tool(bash_config),
    ));
    let thread = start_thread(&host).await;
    let coordinates = thread.context().coordinates.clone();
    let mut events = thread.subscribe_events();

    host.submit(
        coordinates.thread_id,
        "turn-host-cancel",
        "run until interrupted",
    )
    .await
    .unwrap();
    let child_pid = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(pid) = tokio::fs::read_to_string(host_root.join("child.pid")).await
                && let Ok(pid) = pid.trim().parse::<libc::pid_t>()
            {
                break pid;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("host bash did not expose its child pid");

    host.cancel(coordinates.thread_id, "stop host process")
        .await
        .unwrap();
    collect_until_cancelled(&mut events, "stop host process").await;
    let completion = host
        .runtime_store()
        .read_events(&EventStreamId::for_thread(&coordinates), None)
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.kind == EventKind::ToolCallCompleted)
        .expect("interrupted host bash did not settle a completion");
    let completion =
        serde_json::from_value::<ToolCallCompletedPayload>(completion.payload).unwrap();
    assert_eq!(
        completion.cancellation,
        Some(ToolCallCancellation::CancelledAcknowledged)
    );
    assert!(!completion.success);
    assert!(
        thread
            .session_context()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|message| {
                matches!(message, CanonicalMessage::ToolResult { is_error: true, .. })
                    && text_from_message(message).contains("host bash exec cancelled")
            }),
        "the canonical mirror must retain the partial cancelled process result"
    );

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if unsafe { libc::kill(child_pid, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("interrupt left a host bash descendant alive");
    host.shutdown_all().await.unwrap();
    tokio::fs::remove_dir_all(host_root).await.unwrap();
}

#[tokio::test]
async fn tool_hook_permission_scenario_records_ordered_events_and_history() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("echo_search", json!({"input": "original"})),
        response_text("final reply"),
    ]));
    let pre_hook = StaticHookHandler::pre_tool(
        "pre-echo",
        "echo_search",
        HookHandlerOutput {
            updated_input: Some(json!({"input": "rewritten"})),
            additional_context: Some("pre context".to_string()),
            ..HookHandlerOutput::default()
        },
    );
    let post_hook = StaticHookHandler::post_tool(
        "post-echo",
        "echo_search",
        HookHandlerOutput {
            replacement_output: Some("hook replacement".to_string()),
            additional_context: Some("post context".to_string()),
            feedback: Some("feedback context".to_string()),
            ..HookHandlerOutput::default()
        },
    );
    let pre_handler: Arc<dyn HookHandler> = pre_hook.clone();
    let post_handler: Arc<dyn HookHandler> = post_hook.clone();
    let factory = provider_factory(Arc::clone(&client))
        .with_tool_router(echo_router("echo_search"))
        .with_hook_pipeline(hook_pipeline(vec![pre_handler, post_handler]));
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-hook",
        "use echo",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "final reply").await;

    assert_event_order(
        trace.runtime_events(),
        "tool call started",
        |event| matches!(event, RuntimeEventKind::ToolCallStarted { call_id, .. } if call_id == "call_1|fc_1"),
        "permission decision",
        |event| matches!(event, RuntimeEventKind::PermissionDecision { call_id, .. } if call_id == "call_1|fc_1"),
    );
    assert_event_order(
        trace.runtime_events(),
        "permission decision",
        |event| matches!(event, RuntimeEventKind::PermissionDecision { call_id, .. } if call_id == "call_1|fc_1"),
        "tool log",
        |event| matches!(event, RuntimeEventKind::ToolLog { call_id, .. } if call_id == "call_1|fc_1"),
    );
    assert_event_order(
        trace.runtime_events(),
        "tool log",
        |event| matches!(event, RuntimeEventKind::ToolLog { call_id, .. } if call_id == "call_1|fc_1"),
        "tool result",
        |event| matches!(event, RuntimeEventKind::ToolCallResult { call_id, .. } if call_id == "call_1|fc_1"),
    );
    assert_eq!(pre_hook.requests().len(), 1);
    assert_eq!(post_hook.requests().len(), 1);
    assert!(matches!(
        &pre_hook.requests()[0],
        HookRequest::PreToolUse(request)
            if request.arguments == json!({"input": "original"})
    ));
    assert!(matches!(
        &post_hook.requests()[0],
        HookRequest::PostToolUse(request)
            if request.arguments == json!({"input": "rewritten"})
                && request.output == "echo:rewritten"
    ));
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::PermissionDecision {
            decision: RuntimePermissionDecision::Allow,
            reason: None,
            ..
        }
    )));
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolLog {
            level: RuntimeToolLogLevel::Info,
            metadata,
            ..
        } if metadata.get("success").map(String::as_str) == Some("true")
            && metadata.contains_key("duration_ms")
    )));
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolCallResult {
            output,
            success: true,
            duration_ms: Some(_),
            ..
        } if output == "hook replacement"
    )));
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec![
            "use echo",
            "",
            "pre context",
            "hook replacement",
            "post context",
            "feedback context",
            "final reply"
        ]
    );
}

#[tokio::test]
async fn pre_tool_block_scenario_records_failed_tool_result_and_continues() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("echo_search", json!({"input": "original"})),
        response_text("handled block"),
    ]));
    let block_hook = StaticHookHandler::pre_tool(
        "block-echo",
        "echo_search",
        HookHandlerOutput {
            should_block: true,
            block_reason: Some("blocked by scenario".to_string()),
            additional_context: Some("block context".to_string()),
            ..HookHandlerOutput::default()
        },
    );
    let block_handler: Arc<dyn HookHandler> = block_hook.clone();
    let factory = provider_factory(Arc::clone(&client))
        .with_tool_router(echo_router("echo_search"))
        .with_hook_pipeline(hook_pipeline(vec![block_handler]));
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-block",
        "use echo",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "handled block").await;

    assert_eq!(block_hook.requests().len(), 1);
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::HookCompleted {
            hook_id,
            event_name: HookEventName::PreToolUse,
            status: HookRunStatus::Blocked,
            message: Some(message),
            ..
        } if hook_id == "block-echo" && message == "blocked by scenario"
    )));
    assert!(
        !trace
            .runtime_events()
            .iter()
            .any(|event| matches!(event, RuntimeEventKind::PermissionDecision { .. }))
    );
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolCallResult {
            output,
            success: false,
            ..
        } if output == "blocked by scenario"
    )));
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec![
            "use echo",
            "",
            "block context",
            "blocked by scenario",
            "handled block"
        ]
    );
}

#[tokio::test]
async fn permission_deny_scenario_records_failed_tool_result_and_continues() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("echo_search", json!({"input": "secret"})),
        response_text("handled deny"),
    ]));
    let factory = provider_factory(Arc::clone(&client))
        .with_tool_router(echo_router("echo_search"))
        .with_tool_permission_gate(Arc::new(DenyGate::new("denied by scenario")));
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-deny",
        "use echo",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "handled deny").await;

    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::PermissionDecision {
            decision: RuntimePermissionDecision::Deny,
            reason: Some(reason),
            ..
        } if reason == "denied by scenario"
    )));
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolLog {
            level: RuntimeToolLogLevel::Error,
            metadata,
            ..
        } if metadata.get("success").map(String::as_str) == Some("false")
    )));
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolCallResult {
            output,
            success: false,
            ..
        } if output == "denied by scenario"
    )));
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec!["use echo", "", "denied by scenario", "handled deny"]
    );
}

#[tokio::test]
async fn unknown_tool_scenario_appends_error_result_and_continues() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("missing_tool", json!({})),
        response_text("handled missing"),
    ]));
    let factory = provider_factory(Arc::clone(&client)).with_tool_router(echo_router("known_echo"));
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-missing",
        "use missing",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "handled missing").await;

    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolCallResult {
            output,
            success: false,
            ..
        } if output.contains("unknown tool \"missing_tool\"")
    )));
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec![
            "use missing",
            "",
            "runtime execution failed: unknown tool \"missing_tool\"",
            "handled missing"
        ]
    );
}

#[tokio::test]
async fn provider_failure_scenario_emits_model_failed_without_assistant_history() {
    let client = Arc::new(ScriptedProviderClient::with_steps(vec![
        ScriptedProviderStep::Error("provider down".to_string()),
    ]));
    let host = RuntimeHost::new(Arc::new(provider_factory(Arc::clone(&client))));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-fail", "hello")
        .await
        .unwrap();
    let trace = collect_until_failed(&mut events, "provider down").await;

    assert_event_order(
        trace.runtime_events(),
        "model request started",
        |event| matches!(event, RuntimeEventKind::ModelRequestStarted { turn_id, .. } if turn_id == "turn-fail"),
        "model request failed",
        |event| matches!(event, RuntimeEventKind::ModelRequestFailed { turn_id, error, .. } if turn_id == "turn-fail" && error.contains("provider down")),
    );
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::Failed {
            code,
            message
        } if code == "runtime_execution" && message.contains("provider down")
    )));
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec!["hello"]
    );
}

#[tokio::test]
async fn streaming_scenario_emits_deltas_usage_and_model_completion() {
    let client = Arc::new(ScriptedProviderClient::with_stream_events(vec![vec![
        ProviderStreamEvent::TextDelta {
            text: "VER".to_string(),
        },
        ProviderStreamEvent::TextDelta {
            text: "LET".to_string(),
        },
        ProviderStreamEvent::Usage {
            usage: CanonicalUsage {
                input_tokens: 5,
                output_tokens: 6,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 1,
            },
        },
        ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::EndTurn,
        },
    ]]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(streaming_provider_factory(provider_client));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-stream",
        "stream",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "VERLET").await;

    assert_event_order(
        trace.runtime_events(),
        "stream model start",
        |event| {
            matches!(
                event,
                RuntimeEventKind::ModelRequestStarted {
                    mode: RuntimeModelRequestMode::Stream,
                    ..
                }
            )
        },
        "first text delta",
        |event| matches!(event, RuntimeEventKind::TextDelta { text } if text == "VER"),
    );
    assert_event_order(
        trace.runtime_events(),
        "first text delta",
        |event| matches!(event, RuntimeEventKind::TextDelta { text } if text == "VER"),
        "usage",
        |event| matches!(event, RuntimeEventKind::Usage { usage } if usage.input_tokens == 5 && usage.cache_read_input_tokens == 1),
    );
    assert_event_order(
        trace.runtime_events(),
        "usage",
        |event| matches!(event, RuntimeEventKind::Usage { usage } if usage.input_tokens == 5),
        "stream model complete",
        |event| {
            matches!(
                event,
                RuntimeEventKind::ModelRequestCompleted {
                    mode: RuntimeModelRequestMode::Stream,
                    stop_reason: CanonicalStopReason::EndTurn,
                    ..
                }
            )
        },
    );
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec!["stream", "VERLET"]
    );
}

#[tokio::test]
async fn cancellation_during_model_request_keeps_thread_reusable() {
    let client = Arc::new(ScriptedProviderClient::with_steps(vec![
        ScriptedProviderStep::Pending,
        ScriptedProviderStep::Response(response_text("after reply")),
    ]));
    let host = RuntimeHost::new(Arc::new(provider_factory(Arc::clone(&client))));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-slow", "slow")
        .await
        .unwrap();
    wait_for_requests(&client, 1).await;
    host.cancel(thread.context().coordinates.thread_id, "stop slow")
        .await
        .unwrap();
    let cancelled = collect_until_cancelled(&mut events, "stop slow").await;
    assert!(cancelled.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::Cancelled { reason } if reason == "stop slow"
    )));

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-after",
        "after",
    )
    .await
    .unwrap();
    collect_until_output(&mut events, "after reply").await;
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec!["slow", "after", "after reply"]
    );
}

#[tokio::test]
async fn queue_mode_waits_for_active_turn_and_runs_after_cancel() {
    let client = Arc::new(ScriptedProviderClient::with_steps(vec![
        ScriptedProviderStep::Pending,
        ScriptedProviderStep::Response(response_text("queued reply")),
    ]));
    let host = RuntimeHost::new(Arc::new(provider_factory(Arc::clone(&client))));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-slow", "slow")
        .await
        .unwrap();
    wait_for_requests(&client, 1).await;
    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-queued",
        "queued",
        TurnSubmissionMode::Queue,
    )
    .await
    .unwrap();

    host.cancel(thread.context().coordinates.thread_id, "release slow")
        .await
        .unwrap();
    let cancelled = collect_until_cancelled(&mut events, "release slow").await;
    assert!(
        cancelled
            .signals
            .iter()
            .any(|signal| signal.kind == ThreadSignalKind::UserQueue)
    );

    collect_until_output(&mut events, "queued reply").await;
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec!["slow", "queued", "queued reply"]
    );
}

#[tokio::test]
async fn interrupt_mode_cancels_active_turn_and_front_queues_replacement() {
    let client = Arc::new(ScriptedProviderClient::with_steps(vec![
        ScriptedProviderStep::Pending,
        ScriptedProviderStep::Response(response_text("replacement reply")),
        ScriptedProviderStep::Response(response_text("queued reply")),
    ]));
    let host = RuntimeHost::new(Arc::new(provider_factory(Arc::clone(&client))));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-slow", "slow")
        .await
        .unwrap();
    wait_for_requests(&client, 1).await;
    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-queued",
        "queued",
        TurnSubmissionMode::Queue,
    )
    .await
    .unwrap();
    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-replacement",
        "replacement",
        TurnSubmissionMode::Interrupt,
    )
    .await
    .unwrap();

    let cancelled =
        collect_until_cancelled(&mut events, "interrupted by turn turn-replacement").await;
    assert!(
        cancelled
            .signals
            .iter()
            .any(|signal| signal.kind == ThreadSignalKind::UserInterrupt)
    );
    assert!(
        cancelled
            .signals
            .iter()
            .any(|signal| signal.kind == ThreadSignalKind::UserQueue)
    );
    collect_until_output(&mut events, "replacement reply").await;
    collect_until_output(&mut events, "queued reply").await;
    assert_eq!(
        session_texts(&thread.session_context().await.unwrap()),
        vec![
            "slow",
            "replacement",
            "replacement reply",
            "queued",
            "queued reply"
        ]
    );
}

#[tokio::test]
async fn steer_mode_is_model_visible_inside_active_provider_turn() {
    let client = Arc::new(GatedProviderClient::new(vec![
        response_tool_call_with_id("call_first", "echo_search", json!({"input": "original"})),
        response_tool_call_with_id("call_second", "echo_search", json!({"input": "follow-up"})),
        response_text("steered reply"),
    ]));
    let factory =
        provider_factory(Arc::clone(&client)).with_tool_router(echo_router("echo_search"));
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-root", "root")
        .await
        .unwrap();
    wait_for_gated_requests(&client, 1).await;
    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-steer",
        "please revise with the tool result",
        TurnSubmissionMode::Steer,
    )
    .await
    .unwrap();
    let steer_signal = collect_until_signal(&mut events, ThreadSignalKind::UserSteer).await;
    assert_eq!(
        steer_signal
            .metadata
            .get("active_turn_id")
            .map(String::as_str),
        Some("turn-root")
    );
    client.release_first();

    collect_until_output(&mut events, "steered reply").await;
    wait_for_gated_requests(&client, 3).await;
    let requests = client.requests();
    assert_eq!(
        steering_injection_count(
            &requests[1],
            "turn-steer",
            "please revise with the tool result"
        ),
        1,
        "the first request after the boundary must deliver the steer: {:?}",
        request_texts(&requests[1]),
    );
    assert_eq!(
        steering_injection_count(
            &requests[2],
            "turn-steer",
            "please revise with the tool result"
        ),
        0,
        "a later tool round must not inject the same steer again: {:?}",
        request_texts(&requests[2]),
    );
}

#[tokio::test]
async fn steer_settling_during_tool_execution_is_folded_at_that_boundary_once() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("boundary_echo", json!({"input": "original"})),
        response_text("boundary reply"),
    ]));
    let tool = Arc::new(GatedToolProvider::new("boundary_echo"));
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool.clone()),
    );
    let factory = provider_factory(Arc::clone(&client)).with_tool_router(router);
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-root", "root")
        .await
        .unwrap();
    tool.wait_until_started().await;
    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-boundary-steer",
        "arrived while tool results were completing",
        TurnSubmissionMode::Steer,
    )
    .await
    .unwrap();
    tool.release();

    collect_until_signal(&mut events, ThreadSignalKind::UserSteer).await;
    assert!(
        !tool.cancellation_observed.load(Ordering::SeqCst),
        "a steer must not fire an in-flight tool token"
    );
    collect_until_output(&mut events, "boundary reply").await;
    wait_for_requests(&client, 2).await;
    let requests = client.requests();
    assert_eq!(
        steering_injection_count(
            &requests[1],
            "turn-boundary-steer",
            "arrived while tool results were completing"
        ),
        1,
        "the boundary request did not fold the concurrently settling steer: {:?}",
        request_texts(&requests[1]),
    );
    assert!(
        thread
            .session_context()
            .await
            .unwrap()
            .messages
            .iter()
            .any(|message| matches!(
                message,
                CanonicalMessage::ToolResult { tool_call_id, .. }
                    if tool_call_id == "call_1|fc_1"
            )),
        "the steer boundary must retain the completed tool result"
    );
    assert_eq!(
        request_text_occurrence_count(&requests[1], "arrived while tool results were completing"),
        1,
        "the boundary request must present the steer exactly once across history and hook context: {:?}",
        request_texts(&requests[1]),
    );
}

#[tokio::test]
async fn cancel_during_tool_execution_cancels_in_flight_batch_and_keeps_thread_reusable() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("cancel_boundary_echo", json!({"input": "original"})),
        response_text("reply after boundary cancel"),
    ]));
    let tool = Arc::new(GatedToolProvider::new("cancel_boundary_echo"));
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool.clone()),
    );
    let factory = provider_factory(Arc::clone(&client)).with_tool_router(router);
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-root", "root")
        .await
        .unwrap();
    tool.wait_until_started().await;
    let cancel_host = host.clone();
    let thread_id = thread.context().coordinates.thread_id;
    let cancel =
        tokio::spawn(async move { cancel_host.cancel(thread_id, "cancel at boundary").await });

    tokio::time::timeout(Duration::from_secs(30), cancel)
        .await
        .expect("turn cancellation should not wait for the in-flight tool")
        .unwrap()
        .unwrap();
    collect_until_cancelled(&mut events, "cancel at boundary").await;
    assert!(tool.cancellation_observed.load(Ordering::SeqCst));
    assert_eq!(thread.status(), ThreadStatus::Idle);

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-after-cancel",
        "continue",
    )
    .await
    .unwrap();
    collect_until_output(&mut events, "reply after boundary cancel").await;
}

#[tokio::test]
async fn steer_append_failure_at_tool_boundary_fails_the_agent_loop() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("failed_boundary_echo", json!({"input": "original"})),
    ]));
    let tool = Arc::new(GatedToolProvider::new("failed_boundary_echo"));
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool.clone()),
    );
    let store = Arc::new(
        FaultingRuntimeStore::new(Arc::new(InMemorySessionStore::new())).fail_nth(
            "append_turn_input",
            2,
            "boundary steer append failed",
        ),
    );
    let factory = provider_factory(Arc::clone(&client)).with_tool_router(router);
    let host = RuntimeHost::with_session_store(Arc::new(factory), store);
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-root", "root")
        .await
        .unwrap();
    tool.wait_until_started().await;
    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-failed-steer",
        "cannot persist",
        TurnSubmissionMode::Steer,
    )
    .await
    .unwrap();
    tool.release();

    collect_until_failed(&mut events, "boundary steer append failed").await;
    assert_eq!(thread.status(), ThreadStatus::Failed);
    assert_eq!(client.requests().len(), 1);
}

#[tokio::test]
async fn steer_after_last_tool_boundary_is_delivered_by_the_next_turn() {
    let client = Arc::new(GatedProviderClient::gating_request(
        2,
        vec![
            response_tool_call("echo_search", json!({"input": "original"})),
            response_text("root reply"),
            response_text("next reply"),
        ],
    ));
    let factory =
        provider_factory(Arc::clone(&client)).with_tool_router(echo_router("echo_search"));
    let host = RuntimeHost::new(Arc::new(factory));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-root", "root")
        .await
        .unwrap();
    wait_for_gated_requests(&client, 2).await;
    let boundary_request_texts = request_texts(&client.requests()[1]);
    assert!(
        !boundary_request_texts
            .iter()
            .any(|text| text.contains("missed the last boundary")),
        "the final boundary request was assembled before the late steer: {boundary_request_texts:?}",
    );
    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-late-steer",
        "missed the last boundary",
        TurnSubmissionMode::Steer,
    )
    .await
    .unwrap();
    collect_until_signal(&mut events, ThreadSignalKind::UserSteer).await;
    client.release_gate();
    collect_until_output(&mut events, "root reply").await;

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-next",
        "continue",
    )
    .await
    .unwrap();
    collect_until_output(&mut events, "next reply").await;
    wait_for_gated_requests(&client, 3).await;
    let next_texts = request_texts(&client.requests()[2]);
    assert_eq!(
        next_texts
            .iter()
            .filter(|text| text.as_str() == "missed the last boundary")
            .count(),
        1,
        "the next turn must contain the persisted late steer exactly once: {next_texts:?}",
    );
    assert!(
        !next_texts
            .iter()
            .any(|text| text.contains("Additional user steering for active turn turn-late-steer:")),
        "next-turn fallback must preserve the existing history assembly path: {next_texts:?}",
    );
}

#[tokio::test]
async fn replay_after_boundary_request_before_result_persistence_keeps_one_steer() {
    let path = temp_db_path("verlet-intra-turn-steer-replay");
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_steer_replay");
    let first_client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call("crash_echo", json!({"input": "original"})),
        response_text("lost before persistence"),
    ]));
    let faulting_client = Arc::new(
        FaultingProviderClient::new(Arc::clone(&first_client)).fail_nth_after_http(
            "complete",
            2,
            "simulated crash after the boundary request before result persistence",
        ),
    );
    let tool = Arc::new(GatedToolProvider::new("crash_echo"));
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool.clone()),
    );
    {
        let store = Arc::new(SqliteSessionStore::open(&path).await.unwrap());
        let factory = provider_factory(faulting_client).with_tool_router(router);
        let host = RuntimeHost::with_session_store(Arc::new(factory), store);
        let thread = host
            .start_thread(coordinates.clone(), ThreadTopology::root())
            .await
            .unwrap();
        let mut events = thread.subscribe_events();

        host.submit(thread.context().coordinates.thread_id, "turn-root", "root")
            .await
            .unwrap();
        tool.wait_until_started().await;
        host.submit_with_mode(
            thread.context().coordinates.thread_id,
            "turn-crash-steer",
            "survive the delivery window",
            TurnSubmissionMode::Steer,
        )
        .await
        .unwrap();
        tool.release();
        collect_until_signal(&mut events, ThreadSignalKind::UserSteer).await;
        collect_until_failed(&mut events, "simulated crash").await;

        let requests = first_client.requests();
        assert_eq!(
            steering_injection_count(
                &requests[1],
                "turn-crash-steer",
                "survive the delivery window"
            ),
            1,
            "the crash cut must be after the steer-bearing request was assembled",
        );
    }

    let reopened = Arc::new(SqliteSessionStore::open(&path).await.unwrap());
    let persisted_events = reopened
        .read_events(&EventStreamId::for_thread(&coordinates), None)
        .await
        .unwrap();
    let steer_entry_id = persisted_events
        .iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload["turn_id"] == "turn-crash-steer"
        })
        .and_then(|event| event.payload["entry_id"].as_str())
        .expect("persisted steer entry must exist before replay");
    assert!(
        persisted_events.iter().any(|event| {
            event.kind == EventKind::ContextCompileCompleted
                && event.payload["session_entry_ids"]
                    .as_array()
                    .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(steer_entry_id)))
        }),
        "the steer-bearing request must have a durable context compile witness",
    );

    let replay_client = Arc::new(ScriptedProviderClient::with_responses(vec![response_text(
        "replayed reply",
    )]));
    let replay_host = RuntimeHost::with_session_store(
        Arc::new(provider_factory(Arc::clone(&replay_client))),
        reopened,
    );
    let replayed = replay_host
        .start_thread(coordinates, ThreadTopology::root())
        .await
        .unwrap();
    let mut replay_events = replayed.subscribe_events();
    replay_host
        .submit(
            replayed.context().coordinates.thread_id,
            "turn-root",
            "root",
        )
        .await
        .unwrap();
    collect_until_output(&mut replay_events, "replayed reply").await;

    let replay_texts = request_texts(&replay_client.requests()[0]);
    assert_eq!(
        replay_texts
            .iter()
            .filter(|text| text.as_str() == "survive the delivery window")
            .count(),
        1,
        "replay must retain exactly one persisted steer input: {replay_texts:?}",
    );
    assert!(
        !replay_texts.iter().any(|text| {
            text.contains("Additional user steering for active turn turn-crash-steer:")
        }),
        "the prior compile witness must prevent a second steer injection: {replay_texts:?}",
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn steer_mode_rejects_non_steerable_active_runtime() {
    let config = VirtualBashRuntimeConfig {
        execution_timeout: Duration::from_secs(30),
        max_output_bytes: 1024,
        ..VirtualBashRuntimeConfig::default()
    };
    let host = RuntimeHost::new(Arc::new(VirtualBashRuntimeFactory::new(config)));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-loop",
        "while true; do :; done",
    )
    .await
    .unwrap();
    wait_for_status(&thread, ThreadStatus::Running).await;
    host.steer(
        thread.context().coordinates.thread_id,
        "turn-steer",
        "please change direction",
    )
    .await
    .unwrap();

    collect_until_policy_rejected(&mut events, "active_turn_not_steerable").await;
    host.cancel(thread.context().coordinates.thread_id, "stop loop")
        .await
        .unwrap();
    collect_until_cancelled(&mut events, "stop loop").await;
}

#[tokio::test]
async fn subthread_spawn_scenario_reports_parent_child_events_and_history() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_tool_call(
            THREAD_SPAWN_OPERATION,
            json!({
                "task_name": "worker",
                "message": "child task",
            }),
        ),
        response_text("spawned child"),
    ]));
    let root_factory = Arc::new(
        provider_factory(Arc::clone(&client))
            .with_tool_router(Arc::new(kernel_thread_router().await)),
    );
    let host = RuntimeHost::new(Arc::new(RootProviderChildEchoFactory::new(root_factory)));
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-spawn",
        "spawn worker",
    )
    .await
    .unwrap();
    let trace = collect_until_output(&mut events, "spawned child").await;

    assert!(
        trace
            .runtime_events()
            .iter()
            .any(|event| matches!(event, RuntimeEventKind::SubthreadStarted { .. }))
    );
    assert!(trace.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ToolCallResult {
            output,
            success: true,
            ..
        } if output.contains("cooldis.thread_spawn")
    )));
    let children = host
        .children_of(thread.context().coordinates.thread_id)
        .await;
    assert_eq!(children.len(), 1);
    assert_eq!(
        session_texts(&children[0].session_context().await.unwrap()),
        vec!["child task"]
    );
    assert!(
        session_texts(&thread.session_context().await.unwrap())
            .iter()
            .any(|text| text.contains("cooldis.thread_spawn"))
    );
    host.shutdown_all().await.unwrap();
}

async fn kernel_thread_router() -> AgentToolRouter {
    let registry = Arc::new(OperationRegistry::new());
    let package = verlet_threads_kernel_package();
    let mut registration =
        KernelOperationRegistration::new(verlet::VERLET_THREADS_PACKAGE, package.manifest.clone())
            .with_capability_grants(package.capability_grants.clone());
    registration.metadata.insert(
        verlet::OPERATION_METADATA_RUNTIME_KIND.to_string(),
        serde_json::Value::String(verlet::KERNEL_RUNTIME_KIND.to_string()),
    );
    registry.register_kernel(registration).await.unwrap();
    AgentToolRouter::new(registry)
        .with_capability_grants(package.capability_grants)
        .with_tool_aliases(vec![OperationToolAlias {
            tool_name: THREAD_SPAWN_OPERATION.to_string(),
            registered_name: verlet::VERLET_THREADS_PACKAGE.to_string(),
            operation_name: THREAD_SPAWN_OPERATION.to_string(),
            grant_expiries: Vec::new(),
        }])
}

#[tokio::test]
async fn manual_compaction_checkpoint_resume_scenario_replays_summary() {
    let client = Arc::new(ScriptedProviderClient::with_responses(vec![
        response_text("root reply"),
        response_text("model summary"),
        response_text("resumed reply"),
    ]));
    let host = RuntimeHost::with_session_store(
        Arc::new(provider_factory(Arc::clone(&client))),
        support::in_memory_store(),
    );
    let thread = start_thread(&host).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-root", "root")
        .await
        .unwrap();
    collect_until_output(&mut events, "root reply").await;
    host.compact_thread(thread.context().coordinates.thread_id, "compact-1", None)
        .await
        .unwrap();
    let compacted = collect_until_compaction(&mut events, "model summary").await;
    assert!(compacted.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::ModelRequestStarted {
            purpose: RuntimeModelRequestPurpose::Compaction,
            ..
        }
    )));
    assert!(compacted.runtime_events().iter().any(|event| matches!(
        event,
        RuntimeEventKind::Compaction {
            trigger: CompactionTrigger::Manual,
            summary,
        } if summary == "model summary"
    )));
    let checkpoint = host
        .create_checkpoint(
            thread.context().coordinates.thread_id,
            None,
            Some("after-compact".to_string()),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();

    let resumed = host
        .resume_thread_from_checkpoint(checkpoint)
        .await
        .unwrap();
    let mut resumed_events = resumed.subscribe_events();
    host.submit(
        resumed.context().coordinates.thread_id,
        "turn-resumed",
        "after",
    )
    .await
    .unwrap();
    collect_until_output(&mut resumed_events, "resumed reply").await;

    let requests = client.requests();
    assert_eq!(
        request_texts(&requests[2]),
        vec!["Compacted conversation summary:\nmodel summary", "after"]
    );
    assert_eq!(
        session_texts(&resumed.session_context().await.unwrap()),
        vec![
            "Compacted conversation summary:\nmodel summary",
            "after",
            "resumed reply"
        ]
    );
}

fn provider_factory<P>(client: Arc<P>) -> AgentLoopFactory
where
    P: ProviderClient + 'static,
{
    let client: Arc<dyn ProviderClient> = client;
    let mut config = AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    AgentLoopFactory::new(config, client)
}

async fn start_thread(host: &RuntimeHost) -> verlet::RuntimeThreadHandle {
    host.start_thread(
        ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
        ThreadTopology::root(),
    )
    .await
    .unwrap()
}

fn session_texts(session: &SessionContext) -> Vec<String> {
    session.messages.iter().map(text_from_message).collect()
}

fn request_texts(request: &verlet::ProviderRequest) -> Vec<String> {
    request.messages.iter().map(text_from_message).collect()
}

fn steering_injection_count(request: &ProviderRequest, turn_id: &str, text: &str) -> usize {
    let prefix = format!("Additional user steering for active turn {turn_id}:");
    request_texts(request)
        .iter()
        .filter(|message| message.contains(&prefix) && message.contains(text))
        .count()
}

fn request_text_occurrence_count(request: &ProviderRequest, text: &str) -> usize {
    request_texts(request)
        .iter()
        .map(|message| message.matches(text).count())
        .sum()
}

fn temp_db_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}.sqlite3"))
}

fn bash_tool_result_json(events: &[RuntimeEventKind], expected_call_id: &str) -> serde_json::Value {
    let output = events
        .iter()
        .find_map(|event| match event {
            RuntimeEventKind::ToolCallResult {
                call_id, output, ..
            } if call_id == expected_call_id => Some(output.as_str()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing bash tool result for {expected_call_id}"));
    serde_json::from_str(output).unwrap_or_else(|err| {
        panic!("bash tool result for {expected_call_id} was not JSON: {err}; output={output:?}")
    })
}

struct GatedProviderClient {
    requests: Mutex<Vec<ProviderRequest>>,
    responses: Mutex<VecDeque<ProviderResponse>>,
    gated_request: usize,
    first_released: AtomicBool,
    first_release: Notify,
}

struct GatedToolProvider {
    tool_name: String,
    started: AtomicBool,
    started_notify: Notify,
    released: AtomicBool,
    release_notify: Notify,
    cancellation_observed: AtomicBool,
}

impl GatedToolProvider {
    fn new(tool_name: &str) -> Self {
        Self {
            tool_name: tool_name.to_string(),
            started: AtomicBool::new(false),
            started_notify: Notify::new(),
            released: AtomicBool::new(false),
            release_notify: Notify::new(),
            cancellation_observed: AtomicBool::new(false),
        }
    }

    async fn wait_until_started(&self) {
        while !self.started.load(Ordering::SeqCst) {
            self.started_notify.notified().await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }

    async fn wait_until_released(&self) {
        while !self.released.load(Ordering::SeqCst) {
            self.release_notify.notified().await;
        }
    }

    fn result(&self, call: AgentKernelToolCall, cancelled: bool) -> CanonicalMessage {
        let input = call
            .arguments
            .get("input")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            if cancelled {
                "tool invocation cancelled".to_string()
            } else {
                format!("echo:{input}")
            },
            cancelled,
        )
    }
}

#[async_trait::async_trait]
impl AgentKernelToolProvider for GatedToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            self.tool_name.clone(),
            "Wait at the tool-round boundary and echo input.",
            json!({
                "type": "object",
                "properties": {"input": {"type": "string"}},
                "required": ["input"],
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> VerletResult<Option<CanonicalMessage>> {
        self.started.store(true, Ordering::SeqCst);
        self.started_notify.notify_waiters();
        self.wait_until_released().await;
        Ok(Some(self.result(call, false)))
    }

    async fn invoke_tool_call_cancellable(
        &self,
        call: AgentKernelToolCall,
        cancellation: ToolInvocationCancellation,
    ) -> VerletResult<AgentKernelToolOutcome> {
        self.started.store(true, Ordering::SeqCst);
        self.started_notify.notify_waiters();
        let cancelled = tokio::select! {
            _ = self.wait_until_released() => false,
            _ = cancellation.token().cancelled() => {
                self.cancellation_observed.store(true, Ordering::SeqCst);
                true
            }
        };
        Ok(AgentKernelToolOutcome::Completed(Some(
            self.result(call, cancelled),
        )))
    }
}

impl GatedProviderClient {
    fn new(responses: Vec<ProviderResponse>) -> Self {
        Self::gating_request(1, responses)
    }

    fn gating_request(gated_request: usize, responses: Vec<ProviderResponse>) -> Self {
        assert!(gated_request > 0, "gated request index is one-based");
        Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
            gated_request,
            first_released: AtomicBool::new(false),
            first_release: Notify::new(),
        }
    }

    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn release_first(&self) {
        self.release_gate();
    }

    fn release_gate(&self) {
        self.first_released.store(true, Ordering::SeqCst);
        self.first_release.notify_waiters();
    }
}

#[async_trait::async_trait]
impl ProviderClient for GatedProviderClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        let request_index = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            requests.len()
        };
        if request_index == self.gated_request {
            while !self.first_released.load(Ordering::SeqCst) {
                self.first_release.notified().await;
            }
        }
        Ok(self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("gated provider response should be queued"))
    }
}

async fn wait_for_requests(client: &ScriptedProviderClient, expected: usize) {
    for _ in 0..1_500 {
        if client.requests().len() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for {expected} provider request(s); saw {}",
        client.requests().len()
    );
}

async fn collect_until_signal(
    events: &mut tokio::sync::broadcast::Receiver<ThreadEvent>,
    expected: ThreadSignalKind,
) -> ThreadSignal {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match events.recv().await {
                Ok(ThreadEvent::Signal { signal, .. }) if signal.kind == expected => {
                    return signal;
                }
                Ok(ThreadEvent::Failed { message, .. }) => {
                    panic!("thread failed before signal {expected:?}: {message}");
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("event channel closed before signal {expected:?}");
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for signal {expected:?}"))
}

async fn collect_until_policy_rejected(
    events: &mut tokio::sync::broadcast::Receiver<ThreadEvent>,
    expected_code: &str,
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match events.recv().await {
                Ok(ThreadEvent::Runtime { event, .. }) => {
                    if matches!(
                        event.kind,
                        RuntimeEventKind::PolicyRejected { ref code, .. } if code == expected_code
                    ) {
                        return;
                    }
                }
                Ok(ThreadEvent::Failed { message, .. }) => {
                    panic!("thread failed before policy rejection {expected_code:?}: {message}");
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("event channel closed before policy rejection {expected_code:?}");
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for policy rejection {expected_code:?}"));
}

async fn wait_for_status(thread: &verlet::RuntimeThreadHandle, expected: ThreadStatus) {
    let mut status = thread.subscribe_status();
    for _ in 0..50 {
        if *status.borrow() == expected {
            return;
        }
        if status.changed().await.is_err() {
            break;
        }
    }
    panic!(
        "timed out waiting for status {expected:?}; saw {:?}",
        *status.borrow()
    );
}

async fn wait_for_gated_requests(client: &GatedProviderClient, expected: usize) {
    for _ in 0..1_500 {
        if client.requests().len() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for {expected} provider request(s); saw {}",
        client.requests().len()
    );
}
