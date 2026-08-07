const PI_FIXTURE: &str = include_str!("fixtures/pi-session.jsonl");
const PI_EDGE_FIXTURE: &str = include_str!("fixtures/pi-session-edge.jsonl");
const VERLET_FIXTURE: &str = include_str!("fixtures/verlet-export.json");

#[test]
fn pi_session_normalizes_rounds_tokens_and_edit_retry_signal() {
    let records = verlet_trace_ab::convert_pi(std::io::Cursor::new(PI_FIXTURE)).unwrap();
    let stats = verlet_trace_ab::summarize(&records);

    assert_eq!(stats.turns, 1);
    assert_eq!(stats.rounds, 2);
    assert_eq!(stats.tool_calls.get("edit"), Some(&2));
    assert_eq!(stats.tokens.total, 128_624);
    assert_eq!(stats.edit_failures, 1);
    assert_eq!(stats.edit_retries, 1);
    assert!(records.iter().any(|record| {
        record.kind == verlet_trace_ab::RecordKind::ToolResult
            && record.edit.as_ref().is_some_and(|edit| edit.failed)
    }));
    assert!(records.iter().any(|record| {
        record.kind == verlet_trace_ab::RecordKind::ToolCall
            && record.edit.as_ref().is_some_and(|edit| edit.retry)
    }));
}

#[test]
fn verlet_export_preserves_context_receipts_and_edit_retry_signal() {
    let fixture: serde_json::Value = serde_json::from_str(VERLET_FIXTURE).unwrap();
    let records = verlet_trace_ab::convert_verlet_export(&fixture).unwrap();
    let stats = verlet_trace_ab::summarize(&records);

    assert_eq!(stats.turns, 1);
    assert_eq!(stats.rounds, 2);
    assert_eq!(stats.tool_calls.get("edit"), Some(&2));
    assert_eq!(stats.tokens.total, 120);
    assert_eq!(stats.edit_failures, 1);
    assert_eq!(stats.edit_retries, 1);
    assert!(records.iter().any(|record| {
        record.kind == verlet_trace_ab::RecordKind::TurnBoundary
            && record.boundary.as_deref() == Some("context_compile")
    }));
    assert!(
        records
            .iter()
            .all(|record| record.details.contains_key("verlet"))
    );
    assert!(records.iter().any(|record| {
        record
            .details
            .get("verlet")
            .and_then(|value| value.get("export_receipts"))
            .is_some()
    }));
}

#[test]
fn terminal_diff_aligns_rounds_and_prints_decision_stats() {
    let pi = verlet_trace_ab::convert_pi(std::io::Cursor::new(PI_FIXTURE)).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(VERLET_FIXTURE).unwrap();
    let verlet = verlet_trace_ab::convert_verlet_export(&fixture).unwrap();
    let rendered = verlet_trace_ab::render_diff(&pi, &verlet);

    assert!(rendered.contains("PI"));
    assert!(rendered.contains("VERLET"));
    assert!(rendered.contains("T1 R1"));
    assert!(rendered.contains("edit failures: 1"));
    assert!(rendered.contains("edit retries: 1"));
    assert!(rendered.contains("FAIL edit"));
    assert!(rendered.contains("RETRY edit"));
}

#[test]
fn pi_session_surfaces_unmapped_shapes_and_preserves_turn_outcomes() {
    let records = verlet_trace_ab::convert_pi(std::io::Cursor::new(PI_EDGE_FIXTURE)).unwrap();
    let stats = verlet_trace_ab::summarize(&records);

    assert_eq!(stats.turns, 2);
    assert_eq!(stats.rounds, 2);
    assert_eq!(stats.tokens.total, 7);
    assert_eq!(stats.unmapped_records, 2);
    assert!(records.iter().any(|record| {
        record.kind == verlet_trace_ab::RecordKind::TurnBoundary
            && record.turn == 1
            && record.boundary.as_deref() == Some("aborted")
            && record.timestamp_ms == Some(1_020)
            && record.latency_ms == Some(20)
    }));
    let result = records
        .iter()
        .find(|record| {
            record.kind == verlet_trace_ab::RecordKind::ToolResult
                && record
                    .tool
                    .as_ref()
                    .is_some_and(|tool| tool.call_id == "late-call")
        })
        .unwrap();
    assert_eq!(result.round, 1);
    assert_eq!(result.latency_ms, None);
    let call = records
        .iter()
        .find(|record| {
            record.kind == verlet_trace_ab::RecordKind::ToolCall
                && record
                    .tool
                    .as_ref()
                    .is_some_and(|tool| tool.call_id == "late-call")
        })
        .unwrap();
    assert!(!call.edit.as_ref().is_some_and(|edit| edit.retry));
    assert!(records.iter().all(|record| {
        record
            .details
            .get("pi")
            .and_then(|value| value.get("entry"))
            .is_some()
    }));
}

#[test]
fn verlet_export_surfaces_unmapped_events_compaction_and_all_assistant_text() {
    let mut fixture: serde_json::Value = serde_json::from_str(VERLET_FIXTURE).unwrap();
    let items = fixture
        .pointer_mut("/thread/turns/0/items")
        .and_then(serde_json::Value::as_array_mut)
        .unwrap();
    items.retain(|item| {
        item.get("type").and_then(serde_json::Value::as_str) != Some("agentMessage")
    });
    items.push(
        serde_json::json!({"type":"agentMessage","id":"entry-assistant-1","text":"First round"}),
    );
    items.push(
        serde_json::json!({"type":"agentMessage","id":"entry-assistant-2","text":"Second round"}),
    );
    let events = fixture
        .pointer_mut("/streams/1/data")
        .and_then(serde_json::Value::as_array_mut)
        .unwrap();
    events.push(serde_json::json!({
        "schema":"cooldis.stream.record/1", "event_id":"event-future",
        "stream_id":"thread:fixture", "sequence":10, "created_at_ms":1040,
        "kind":"future.event", "payload":{"turn_id":"turn-a","future":true}
    }));
    events.push(serde_json::json!({
        "schema":"cooldis.stream.record/1", "event_id":"event-summary",
        "stream_id":"thread:fixture", "sequence":11, "created_at_ms":1041,
        "kind":"context.summary.completed",
        "payload":{"turn_id":"turn-a","summary":"compacted context"}
    }));

    let records = verlet_trace_ab::convert_verlet_export(&fixture).unwrap();
    let stats = verlet_trace_ab::summarize(&records);
    let assistant_text = records
        .iter()
        .filter(|record| record.kind == verlet_trace_ab::RecordKind::AssistantMessage)
        .filter_map(|record| record.content.as_deref())
        .collect::<Vec<_>>();

    assert_eq!(assistant_text, vec!["First round", "Second round"]);
    assert_eq!(stats.unmapped_records, 2);
    assert_eq!(stats.wall_time_ms, Some(50));
    assert!(records.iter().any(|record| {
        record.kind == verlet_trace_ab::RecordKind::Compaction
            && record.content.as_deref() == Some("compacted context")
    }));
    assert!(records.iter().any(|record| {
        record.kind == verlet_trace_ab::RecordKind::SourceMetadata
            && record
                .details
                .get("verlet")
                .and_then(|value| value.get("thread"))
                .is_some()
    }));
}

#[test]
fn missing_timestamps_remain_unknown_in_stats_and_diff() {
    let records = verlet_trace_ab::convert_pi(std::io::Cursor::new(
        concat!(
            "{\"type\":\"message\",\"message\":{\"role\":\"user\",\"content\":\"hello\",\"timestamp\":1000}}\n",
            "{\"type\":\"message\",\"message\":{\"role\":\"assistant\",\"content\":\"untimed\"}}\n"
        ),
    ))
    .unwrap();
    assert_eq!(verlet_trace_ab::summarize(&records).wall_time_ms, None);
    assert!(verlet_trace_ab::render_diff(&records, &[]).contains("wall: n/a"));
}

#[test]
fn terminal_diff_is_deterministic_and_keeps_unilateral_turns_aligned() {
    let pi = verlet_trace_ab::convert_pi(std::io::Cursor::new(PI_EDGE_FIXTURE)).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(VERLET_FIXTURE).unwrap();
    let verlet = verlet_trace_ab::convert_verlet_export(&fixture).unwrap();
    let first = verlet_trace_ab::render_diff(&pi, &verlet);
    let second = verlet_trace_ab::render_diff(&pi, &verlet);

    assert_eq!(first, second);
    assert!(first.contains("T2 R0"));
    assert!(first.contains("UNMAPPED"));
}
