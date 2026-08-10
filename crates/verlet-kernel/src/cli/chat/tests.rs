#[test]
fn parse_attach_target_accepts_unix_and_websocket() {
    assert_eq!(
        super::parse_attach_target("unix:///tmp/sock").expect("unix target"),
        super::ChatAttachTarget::Unix(std::path::PathBuf::from("/tmp/sock"))
    );
    assert_eq!(
        super::parse_attach_target("ws://127.0.0.1:7000/rpc").expect("ws target"),
        super::ChatAttachTarget::WebSocket("ws://127.0.0.1:7000/rpc".to_string())
    );
}

#[test]
fn parse_attach_target_rejects_empty_and_unknown_schemes() {
    assert!(super::parse_attach_target("unix://").is_err());
    assert!(super::parse_attach_target("wss://host/rpc").is_err());
    assert!(super::parse_attach_target("http://host").is_err());
}

fn notification(
    method: &str,
    params: serde_json::Value,
) -> crate::adapters::app_server::connection::JsonRpcNotification {
    crate::adapters::app_server::connection::JsonRpcNotification {
        method: method.to_string(),
        params: Some(params),
    }
}

fn driver() -> super::ChatDriver {
    super::ChatDriver {
        thread_id: "thread-1".to_string(),
        active_turn_id: Some("turn-1".to_string()),
    }
}

#[test]
fn projects_answer_and_thinking_deltas_for_the_active_turn() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "item/agentMessage/delta",
        serde_json::json!({"threadId": "thread-1", "turnId": "turn-1", "delta": "hi"}),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::AnswerDelta("hi".into())]
    );

    let events = driver.project_notification(&notification(
        "item/agentThinking/delta",
        serde_json::json!({"threadId": "thread-1", "turnId": "turn-1", "delta": "hm"}),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ThinkingDelta("hm".into())]
    );

    // Another thread's stream must not leak into this transcript.
    let events = driver.project_notification(&notification(
        "item/agentMessage/delta",
        serde_json::json!({"threadId": "thread-2", "turnId": "turn-9", "delta": "no"}),
    ));
    assert!(events.is_empty());

    let events = driver.project_notification(&notification(
        "item/agentMessage/delta",
        serde_json::json!({"threadId": "thread-1", "turnId": "turn-1", "delta": ""}),
    ));
    assert!(
        events.is_empty(),
        "empty deltas must not open transcript cells"
    );
}

#[test]
fn projects_tool_call_lifecycle() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "item/started",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {
                "type": "dynamicToolCall",
                "id": "call-1",
                "tool": "web_search",
                "arguments": {"query": "verlet"},
            },
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ToolStarted {
            id: "call-1".into(),
            title: "web_search {\"query\":\"verlet\"}".into(),
        }]
    );

    let events = driver.project_notification(&notification(
        "item/completed",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {
                "type": "dynamicToolCall",
                "id": "call-1",
                "success": false,
                "contentItems": [{"type": "inputText", "text": "boom"}],
            },
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ToolCompleted {
            id: "call-1".into(),
            success: false,
            output: "boom".into(),
        }]
    );
}

#[test]
fn projects_command_execution_output_and_exit() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "item/commandExecution/outputDelta",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "exec-1",
            "delta": "line\n",
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ToolOutputDelta {
            id: "exec-1".into(),
            delta: "line\n".into(),
        }]
    );

    let events = driver.project_notification(&notification(
        "item/completed",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "item": {
                "type": "commandExecution",
                "id": "exec-1",
                "exitCode": 2,
                "aggregatedOutput": "boom",
            },
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::ToolCompleted {
            id: "exec-1".into(),
            success: false,
            output: "boom".into(),
        }]
    );
}

#[test]
fn ignores_tool_and_error_events_outside_the_active_turn() {
    let mut driver = driver();
    for (method, params) in [
        (
            "item/started",
            serde_json::json!({
                "threadId": "thread-1",
                "turnId": "turn-2",
                "item": {"type": "dynamicToolCall", "id": "stale", "tool": "read"},
            }),
        ),
        (
            "item/commandExecution/outputDelta",
            serde_json::json!({
                "threadId": "thread-1",
                "turnId": "turn-2",
                "itemId": "stale",
                "delta": "must not leak",
            }),
        ),
        (
            "item/completed",
            serde_json::json!({
                "threadId": "thread-1",
                "turnId": "turn-2",
                "item": {"type": "dynamicToolCall", "id": "stale", "success": true},
            }),
        ),
        (
            "error",
            serde_json::json!({
                "threadId": "thread-1",
                "turnId": "turn-2",
                "error": {"message": "stale failure"},
            }),
        ),
    ] {
        assert!(
            driver
                .project_notification(&notification(method, params))
                .is_empty(),
            "{method} from another turn must be ignored"
        );
    }
    assert_eq!(driver.active_turn_id.as_deref(), Some("turn-1"));

    let events = driver.project_notification(&notification(
        "turn/completed",
        serde_json::json!({
            "threadId": "thread-2",
            "turn": {"id": "turn-1", "status": "completed"},
        }),
    ));
    assert!(
        events.is_empty(),
        "another thread cannot complete this turn"
    );
    assert_eq!(driver.active_turn_id.as_deref(), Some("turn-1"));
}

#[test]
fn projects_error_only_for_the_active_thread_and_turn() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "error",
        serde_json::json!({
            "threadId": "thread-1",
            "turnId": "turn-1",
            "error": {"message": "provider failed"},
        }),
    ));
    assert_eq!(
        events,
        vec![
            verlet_chat::ChatEvent::Error {
                title: "app-server error: provider failed".into(),
                body: Vec::new(),
            },
            verlet_chat::ChatEvent::TurnCompleted { error: None },
        ]
    );
    assert!(driver.active_turn_id.is_none());
}

#[test]
fn projects_resync_failure_for_the_current_thread_and_ends_the_turn() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "thread/resync/failed",
        serde_json::json!({
            "threadId": "thread-1",
            "reason": "broadcastLag",
            "laggedEvents": 8,
            "error": {"code": "resync_failed", "message": "durable history unavailable"},
        }),
    ));
    assert_eq!(
        events,
        vec![
            verlet_chat::ChatEvent::Error {
                title: "stream resync failed: durable history unavailable".into(),
                body: vec![
                    "the live subscription stopped; the transcript may be incomplete".into()
                ],
            },
            verlet_chat::ChatEvent::TurnCompleted { error: None },
        ]
    );
    assert!(driver.active_turn_id.is_none());

    assert!(
        driver
            .project_notification(&notification(
                "thread/resync/failed",
                serde_json::json!({
                    "threadId": "thread-2",
                    "error": {"message": "other thread"},
                }),
            ))
            .is_empty()
    );
}

#[test]
fn turn_completed_clears_the_active_turn_and_reports_errors() {
    let mut driver = driver();
    let events = driver.project_notification(&notification(
        "turn/completed",
        serde_json::json!({
            "threadId": "thread-1",
            "turn": {"id": "turn-1", "status": "failed", "error": {"message": "model unavailable"}},
        }),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::TurnCompleted {
            error: Some("model unavailable".into()),
        }]
    );
    assert!(driver.active_turn_id.is_none());

    // Once idle, an unsolicited turn/started is adopted.
    let events = driver.project_notification(&notification(
        "turn/started",
        serde_json::json!({"threadId": "thread-1", "turn": {"id": "turn-2"}}),
    ));
    assert_eq!(
        events,
        vec![verlet_chat::ChatEvent::TurnStarted {
            turn_id: "turn-2".into(),
        }]
    );
    assert_eq!(driver.active_turn_id.as_deref(), Some("turn-2"));
}

#[test]
fn session_rows_mark_the_current_thread() {
    let rows = super::session_rows(
        &serde_json::json!({
            "data": [
                {"id": "thread-1", "name": "alpha", "status": {"type": "idle"}, "preview": "  hello   world  "},
                {"id": "thread-2", "name": "", "status": {"type": "running"}},
            ],
        }),
        "thread-1",
    );
    assert_eq!(rows.len(), 2);
    assert!(rows[0].current);
    assert_eq!(rows[0].preview, "hello world");
    assert_eq!(rows[1].name, "unnamed");
    assert!(!rows[1].current);
}
