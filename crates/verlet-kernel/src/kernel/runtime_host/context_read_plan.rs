pub(crate) fn context_compile_payload_v1(
    payload: serde_json::Value,
    stream_id: &verlet_history::EventStreamId,
    source_ranges: &[verlet_history::ObservationSourceRange],
    source_streams: &[verlet_history::EventStreamId],
) -> serde_json::Value {
    let mut object = match payload {
        serde_json::Value::Object(object) => object,
        other => {
            let mut object = serde_json::Map::new();
            object.insert("receipt".to_string(), other);
            object
        }
    };
    object.entry("schema").or_insert_with(|| {
        serde_json::json!(verlet_history::EventKind::ContextCompileCompleted.payload_schema_id())
    });
    object.insert(
        "read_plan".to_string(),
        context_read_plan_v1("history.default", stream_id, source_ranges, source_streams),
    );
    serde_json::Value::Object(object)
}

pub(crate) fn is_recall_context_read_plan_event(event: &verlet_history::EventRecord) -> bool {
    event.kind == verlet_history::EventKind::ContextReadPlanSet
        && event.payload.get("scope").and_then(|value| value.as_str()) == Some("thread")
        && event
            .payload
            .get("pipeline_id")
            .and_then(|value| value.as_str())
            == Some("context.memory")
}

pub(crate) fn is_instruction_context_read_plan_event(event: &verlet_history::EventRecord) -> bool {
    event.kind == verlet_history::EventKind::ContextReadPlanSet
        && event.payload.get("scope").and_then(|value| value.as_str()) == Some("thread")
        && event
            .payload
            .get("pipeline_id")
            .and_then(|value| value.as_str())
            == Some("context.instructions")
}

pub(crate) fn render_recall_context(texts: &[String]) -> String {
    let body = texts
        .iter()
        .map(|text| format!("- {}", text.trim()))
        .collect::<Vec<_>>()
        .join("\n");
    format!("<memory_context>\n{body}\n</memory_context>")
}

pub(crate) fn render_instruction_context(instruction_texts: &[String]) -> String {
    let body = instruction_texts
        .iter()
        .map(|text| format!("- {}", text.trim()))
        .collect::<Vec<_>>()
        .join("\n");
    format!("<instruction_context>\n{body}\n</instruction_context>")
}

fn context_read_plan_v1(
    name: &str,
    stream_id: &verlet_history::EventStreamId,
    source_ranges: &[verlet_history::ObservationSourceRange],
    source_streams: &[verlet_history::EventStreamId],
) -> serde_json::Value {
    let entries = if source_ranges.is_empty() {
        source_streams
            .iter()
            .map(|source_stream| {
                serde_json::json!({
                    "kind": "raw_range",
                    "stream_id": source_stream.as_str(),
                    "range": {
                        "from": "start",
                        "to": "frontier"
                    }
                })
            })
            .collect::<Vec<_>>()
    } else {
        source_ranges
            .iter()
            .map(|range| {
                serde_json::json!({
                    "kind": "raw_range",
                    "stream_id": range.stream_id.as_str(),
                    "range": {
                        "from": read_plan_from_cursor(range.from_sequence),
                        "to": {
                            "sequence": range.to_sequence.get()
                        }
                    }
                })
            })
            .collect::<Vec<_>>()
    };
    serde_json::json!({
        "schema": verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1,
        "name": name,
        "source_stream": stream_id.as_str(),
        "frontier": "compile_frontier",
        "entries": entries,
    })
}

fn read_plan_from_cursor(sequence: verlet_history::EventSequence) -> serde_json::Value {
    if sequence.get() <= 1 {
        serde_json::Value::String("start".to_string())
    } else {
        serde_json::json!({ "sequence": sequence.get() - 1 })
    }
}

pub(crate) fn context_summary_completed_payload_v1(
    summary: &str,
    source_ranges: &[verlet_history::ObservationSourceRange],
) -> serde_json::Value {
    serde_json::json!({
        "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": summary,
        "covered_ranges": source_ranges_json(source_ranges),
        "content": {
            "sha256": format!("sha256:{}", verlet_agent::contracts::sha256_hex(summary.as_bytes())),
        },
    })
}

pub(crate) fn context_read_plan_set_payload_v1(
    name: &str,
    stream_id: &verlet_history::EventStreamId,
    summary_event_id: verlet_history::EventRecordId,
    source_ranges: &[verlet_history::ObservationSourceRange],
) -> serde_json::Value {
    serde_json::json!({
        "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": name,
        "pipeline_id": "context.default",
        "source_id": stream_id.as_str(),
        "summary_event_id": summary_event_id.to_string(),
        "read_plan": {
            "schema": verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": name,
            "source_stream": stream_id.as_str(),
            "frontier": "compile_frontier",
            "entries": summary_checkpoint_entries(summary_event_id, source_ranges),
        },
    })
}

fn summary_checkpoint_entries(
    summary_event_id: verlet_history::EventRecordId,
    source_ranges: &[verlet_history::ObservationSourceRange],
) -> Vec<serde_json::Value> {
    if source_ranges.is_empty() {
        return vec![serde_json::json!({
            "kind": "event_ref",
            "event_id": summary_event_id.to_string(),
            "event_role": "summary_checkpoint",
        })];
    }
    source_ranges
        .iter()
        .map(|range| {
            serde_json::json!({
                "kind": "event_ref",
                "stream_id": range.stream_id.as_str(),
                "event_id": summary_event_id.to_string(),
                "event_role": "summary_checkpoint",
                "covers": {
                    "from": read_plan_from_cursor(range.from_sequence),
                    "to": {
                        "sequence": range.to_sequence.get()
                    }
                }
            })
        })
        .collect()
}

fn source_ranges_json(
    source_ranges: &[verlet_history::ObservationSourceRange],
) -> Vec<serde_json::Value> {
    source_ranges
        .iter()
        .map(|range| {
            serde_json::json!({
                "stream_id": range.stream_id.as_str(),
                "from_sequence": range.from_sequence.get(),
                "to_sequence": range.to_sequence.get(),
            })
        })
        .collect()
}

pub(crate) async fn context_source_ranges(
    store: &dyn verlet_history::RuntimeStore,
    source_cuts: &[verlet_history::SessionContextSourceCut],
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::ObservationSourceRange>> {
    let mut ranges = Vec::new();
    for cut in source_cuts {
        if cut.entry_ids.is_empty() {
            continue;
        }
        let events = store
            .read_events(&cut.stream_id, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        if let Some(range) = context_source_range(&cut.stream_id, &events, &cut.entry_ids) {
            ranges.push(range);
        }
    }
    Ok(ranges)
}

fn context_source_range(
    stream_id: &verlet_history::EventStreamId,
    events: &[verlet_history::EventRecord],
    selected_entry_ids: &[verlet_history::SessionEntryId],
) -> Option<verlet_history::ObservationSourceRange> {
    let selected_entry_ids = selected_entry_ids
        .iter()
        .map(|entry_id| entry_id.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    let selected_sequences = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::SessionEntryAppended)
        .filter(|event| {
            event
                .payload
                .get("entry_id")
                .and_then(serde_json::Value::as_str)
                .map(|entry_id| selected_entry_ids.contains(entry_id))
                .unwrap_or(false)
        })
        .map(|event| event.sequence)
        .collect::<Vec<_>>();
    let from_sequence = selected_sequences.first().copied()?;
    let to_sequence = selected_sequences.last().copied()?;
    Some(verlet_history::ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence,
        to_sequence,
    })
}

pub(crate) fn session_context_source_cut_for_entries(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    session_entries: &[verlet_history::SessionEntry],
) -> Vec<verlet_history::SessionContextSourceCut> {
    let entry_ids = session_entries
        .iter()
        .filter(|entry| entry.coordinates.thread_id == coordinates.thread_id)
        .map(|entry| entry.entry_id)
        .collect::<Vec<_>>();
    if entry_ids.is_empty() {
        return Vec::new();
    }
    vec![verlet_history::SessionContextSourceCut {
        coordinates: coordinates.clone(),
        stream_id: verlet_history::EventStreamId::for_thread(coordinates),
        inherited: false,
        entry_ids,
    }]
}

pub(crate) fn context_source_streams(
    source_cuts: &[verlet_history::SessionContextSourceCut],
    fallback_stream_id: &verlet_history::EventStreamId,
) -> Vec<verlet_history::EventStreamId> {
    let mut streams = Vec::new();
    for cut in source_cuts {
        if !streams.contains(&cut.stream_id) {
            streams.push(cut.stream_id.clone());
        }
    }
    if streams.is_empty() {
        streams.push(fallback_stream_id.clone());
    }
    streams
}

pub(crate) fn primary_context_source_range(
    stream_id: &verlet_history::EventStreamId,
    source_ranges: &[verlet_history::ObservationSourceRange],
) -> Option<verlet_history::ObservationSourceRange> {
    source_ranges
        .iter()
        .rev()
        .find(|range| range.stream_id == *stream_id)
        .or_else(|| source_ranges.first())
        .cloned()
}
