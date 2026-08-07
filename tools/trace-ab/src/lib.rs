pub mod runner;

pub const COMMON_TRACE_SCHEMA: &str = "cooldis.trace.common/1";

#[derive(Clone, Debug, serde::Deserialize, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    SourceMetadata,
    AssistantMessage,
    ToolCall,
    ToolResult,
    TurnBoundary,
    Compaction,
    Unmapped,
}

#[derive(Clone, Debug, Default, serde::Deserialize, PartialEq, serde::Serialize)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub total: u64,
}

impl TokenUsage {
    fn add_assign(&mut self, other: &Self) {
        self.input = self.input.saturating_add(other.input);
        self.output = self.output.saturating_add(other.output);
        self.cache_read = self.cache_read.saturating_add(other.cache_read);
        self.cache_write = self.cache_write.saturating_add(other.cache_write);
        self.total = self.total.saturating_add(other.total);
    }
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, serde::Serialize)]
pub struct ToolRecord {
    pub call_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, PartialEq, serde::Serialize)]
pub struct EditSignal {
    #[serde(default)]
    pub application: bool,
    #[serde(default)]
    pub failed: bool,
    #[serde(default)]
    pub retry: bool,
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, serde::Serialize)]
pub struct CommonRecord {
    pub schema: String,
    pub harness: String,
    pub kind: RecordKind,
    pub turn: u32,
    pub round: u32,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<TokenUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<ToolRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edit: Option<EditSignal>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub details: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, PartialEq, serde::Serialize)]
pub struct TraceStats {
    pub turns: u64,
    pub rounds: u64,
    pub tool_calls: std::collections::BTreeMap<String, u64>,
    pub tokens: TokenUsage,
    pub wall_time_ms: Option<u64>,
    pub edit_failures: u64,
    pub edit_retries: u64,
    pub unmapped_records: u64,
}

pub fn convert_pi<R: std::io::BufRead>(reader: R) -> Result<Vec<CommonRecord>, String> {
    let mut records = Vec::new();
    let mut turn = 0_u32;
    let mut round = 0_u32;
    let mut active_turn = false;
    let mut turn_started_ms = None;
    let mut last_timestamp_ms = None;
    let mut turn_outcome = "completed";
    let mut tool_started_ms = std::collections::BTreeMap::<String, i64>::new();

    for (line_index, line) in reader.lines().enumerate() {
        let line =
            line.map_err(|err| format!("failed to read pi line {}: {err}", line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: serde_json::Value = serde_json::from_str(&line)
            .map_err(|err| format!("invalid pi JSON on line {}: {err}", line_index + 1))?;
        match entry.get("type").and_then(serde_json::Value::as_str) {
            Some("session") => {
                push_record(
                    &mut records,
                    record("pi", RecordKind::SourceMetadata, turn, round)
                        .details(pi_entry_details(&entry)),
                );
            }
            Some("message") => {
                let message = entry
                    .get("message")
                    .and_then(serde_json::Value::as_object)
                    .ok_or_else(|| {
                        format!("pi message line {} has no message object", line_index + 1)
                    })?;
                let timestamp_ms = message.get("timestamp").and_then(serde_json::Value::as_i64);
                match message.get("role").and_then(serde_json::Value::as_str) {
                    Some("user") => {
                        if active_turn {
                            finish_pi_turn(
                                &mut records,
                                turn,
                                round,
                                turn_started_ms,
                                last_timestamp_ms,
                                turn_outcome,
                                serde_json::json!({"derived": "next_user_message"}),
                            );
                        }
                        turn += 1;
                        round = 0;
                        active_turn = true;
                        turn_started_ms = timestamp_ms;
                        turn_outcome = "completed";
                        push_record(
                            &mut records,
                            record("pi", RecordKind::TurnBoundary, turn, round)
                                .timestamp(timestamp_ms)
                                .content(message_text(message.get("content")))
                                .boundary("started")
                                .details(pi_details(&entry, message)),
                        );
                    }
                    Some("assistant") => {
                        ensure_pi_turn(
                            &mut records,
                            &mut turn,
                            &mut active_turn,
                            &mut turn_started_ms,
                            timestamp_ms,
                        );
                        round += 1;
                        turn_outcome = pi_turn_outcome(message).unwrap_or(turn_outcome);
                        let usage = pi_token_usage(message.get("usage"));
                        push_record(
                            &mut records,
                            record("pi", RecordKind::AssistantMessage, turn, round)
                                .timestamp(timestamp_ms)
                                .latency(elapsed_ms(last_timestamp_ms, timestamp_ms))
                                .content(message_text(message.get("content")))
                                .tokens(usage)
                                .details(pi_details(&entry, message)),
                        );
                        if let Some(content) =
                            message.get("content").and_then(serde_json::Value::as_array)
                        {
                            for block in content {
                                if block.get("type").and_then(serde_json::Value::as_str)
                                    != Some("toolCall")
                                {
                                    continue;
                                }
                                let call_id = string_field(block, "id", "missing-call-id");
                                let name = string_field(block, "name", "unknown");
                                if let Some(timestamp_ms) = timestamp_ms {
                                    tool_started_ms.insert(call_id.clone(), timestamp_ms);
                                } else {
                                    tool_started_ms.remove(&call_id);
                                }
                                push_record(
                                    &mut records,
                                    record("pi", RecordKind::ToolCall, turn, round)
                                        .timestamp(timestamp_ms)
                                        .tool(ToolRecord {
                                            call_id,
                                            name,
                                            arguments: block.get("arguments").cloned(),
                                            success: None,
                                        })
                                        .details(pi_details(&entry, message)),
                                );
                            }
                        }
                    }
                    Some("toolResult") => {
                        ensure_pi_turn(
                            &mut records,
                            &mut turn,
                            &mut active_turn,
                            &mut turn_started_ms,
                            timestamp_ms,
                        );
                        let call_id =
                            string_field_from_map(message, "toolCallId", "missing-call-id");
                        let name = string_field_from_map(message, "toolName", "unknown");
                        let success = !message
                            .get("isError")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        push_record(
                            &mut records,
                            record("pi", RecordKind::ToolResult, turn, round)
                                .timestamp(timestamp_ms)
                                .latency(elapsed_ms(
                                    tool_started_ms.get(&call_id).copied(),
                                    timestamp_ms,
                                ))
                                .content(message_text(message.get("content")))
                                .tool(ToolRecord {
                                    call_id,
                                    name,
                                    arguments: None,
                                    success: Some(success),
                                })
                                .details(pi_details(&entry, message)),
                        );
                    }
                    _ => {
                        push_record(
                            &mut records,
                            record("pi", RecordKind::Unmapped, turn, round)
                                .timestamp(timestamp_ms)
                                .content(message_text(message.get("content")))
                                .boundary("pi_message_role")
                                .details(pi_details(&entry, message)),
                        );
                    }
                }
                last_timestamp_ms = timestamp_ms;
            }
            Some("compaction") => {
                push_record(
                    &mut records,
                    record("pi", RecordKind::Compaction, turn, round)
                        .content(
                            entry
                                .get("summary")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string),
                        )
                        .details(pi_entry_details(&entry)),
                );
            }
            _ => {
                push_record(
                    &mut records,
                    record("pi", RecordKind::Unmapped, turn, round)
                        .boundary("pi_record_type")
                        .details(pi_entry_details(&entry)),
                );
            }
        }
    }

    if active_turn {
        finish_pi_turn(
            &mut records,
            turn,
            round,
            turn_started_ms,
            last_timestamp_ms,
            turn_outcome,
            serde_json::json!({"derived": "end_of_session"}),
        );
    }
    resolve_pi_tool_result_rounds(&mut records);
    annotate_edit_signals(&mut records);
    Ok(records)
}

pub fn convert_verlet_export(value: &serde_json::Value) -> Result<Vec<CommonRecord>, String> {
    if value.get("schema").and_then(serde_json::Value::as_str)
        != Some("cooldis.debug.thread_export/1")
    {
        return Err("verlet input is not a cooldis.debug.thread_export/1 bundle".to_string());
    }
    let mut events = value
        .get("streams")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "verlet export has no streams array".to_string())?
        .iter()
        .flat_map(|stream| {
            stream
                .get("data")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .cloned()
        })
        .collect::<Vec<_>>();
    events.sort_by_key(event_sort_key);

    let turn_views = verlet_turn_views(value);
    let mut records = Vec::new();
    push_record(
        &mut records,
        record("verlet", RecordKind::SourceMetadata, 0, 0).details(verlet_source_details(value)),
    );
    let mut turn_ordinals = std::collections::BTreeMap::<String, u32>::new();
    let mut round_by_turn = std::collections::BTreeMap::<String, u32>::new();
    let mut start_by_turn = std::collections::BTreeMap::<String, i64>::new();
    let mut context_compile_by_turn = std::collections::BTreeMap::<String, i64>::new();
    let mut current_turn_id = String::new();

    for event in events {
        let event_kind = event
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let payload = event.get("payload").unwrap_or(&serde_json::Value::Null);
        let event_turn_id = event_turn_id(payload).unwrap_or_else(|| current_turn_id.clone());
        if event_kind == "turn.submitted" && !event_turn_id.is_empty() {
            current_turn_id = event_turn_id.clone();
        }
        let turn = ordinal_for_turn(&mut turn_ordinals, &event_turn_id);
        let round = round_by_turn.entry(event_turn_id.clone()).or_insert(0);
        let timestamp_ms = event_timestamp(&event);
        let details = verlet_event_details(&event);

        match event_kind {
            "turn.submitted" => {
                if let Some(timestamp_ms) = timestamp_ms {
                    start_by_turn.insert(event_turn_id.clone(), timestamp_ms);
                }
                push_record(
                    &mut records,
                    record("verlet", RecordKind::TurnBoundary, turn, *round)
                        .timestamp(timestamp_ms)
                        .content(
                            payload
                                .get("input_text")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string),
                        )
                        .boundary("started")
                        .details(details),
                );
            }
            "context.compile.completed" => {
                if let Some(timestamp_ms) = timestamp_ms {
                    context_compile_by_turn.insert(event_turn_id.clone(), timestamp_ms);
                }
                push_record(
                    &mut records,
                    record("verlet", RecordKind::TurnBoundary, turn, *round)
                        .timestamp(timestamp_ms)
                        .boundary("context_compile")
                        .details(details),
                );
            }
            "context.summary.completed" => {
                push_record(
                    &mut records,
                    record("verlet", RecordKind::Compaction, turn, *round)
                        .timestamp(timestamp_ms)
                        .content(
                            payload
                                .get("summary")
                                .or_else(|| payload.get("text"))
                                .or_else(|| payload.pointer("/content/text"))
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string),
                        )
                        .details(details),
                );
            }
            "session.entry.appended"
                if payload
                    .get("entry_kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("compaction") =>
            {
                push_record(
                    &mut records,
                    record("verlet", RecordKind::Compaction, turn, *round)
                        .timestamp(timestamp_ms)
                        .content(
                            payload
                                .get("summary")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string),
                        )
                        .details(details),
                );
            }
            "session.entry.appended"
                if is_verlet_assistant_entry(payload, &event_turn_id, &turn_views) =>
            {
                *round += 1;
                push_record(
                    &mut records,
                    record("verlet", RecordKind::AssistantMessage, turn, *round)
                        .timestamp(timestamp_ms)
                        .latency(elapsed_ms(
                            context_compile_by_turn.remove(&event_turn_id),
                            timestamp_ms,
                        ))
                        .tokens(verlet_token_usage(payload.get("usage")))
                        .details(details),
                );
            }
            "tool.call.requested" => {
                if *round == 0 {
                    *round = 1;
                }
                let call_id = payload
                    .pointer("/subject/call_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("missing-call-id")
                    .to_string();
                let item = turn_views
                    .get(&event_turn_id)
                    .and_then(|view| view.tools.get(&call_id));
                let name = payload
                    .get("tool_name")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| item.and_then(|item| item.name.as_deref()))
                    .unwrap_or("unknown")
                    .to_string();
                push_record(
                    &mut records,
                    record("verlet", RecordKind::ToolCall, turn, *round)
                        .timestamp(timestamp_ms)
                        .tool(ToolRecord {
                            call_id,
                            name,
                            arguments: payload
                                .get("arguments")
                                .cloned()
                                .or_else(|| item.and_then(|item| item.arguments.clone())),
                            success: None,
                        })
                        .details(details),
                );
            }
            "tool.call.completed" => {
                if *round == 0 {
                    *round = 1;
                }
                let call_id = payload
                    .pointer("/subject/call_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("missing-call-id")
                    .to_string();
                let item = turn_views
                    .get(&event_turn_id)
                    .and_then(|view| view.tools.get(&call_id));
                let name = payload
                    .get("tool_name")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| item.and_then(|item| item.name.as_deref()))
                    .unwrap_or("unknown")
                    .to_string();
                let success = payload
                    .get("success")
                    .and_then(serde_json::Value::as_bool)
                    .or_else(|| item.and_then(|item| item.success));
                push_record(
                    &mut records,
                    record("verlet", RecordKind::ToolResult, turn, *round)
                        .timestamp(timestamp_ms)
                        .latency(
                            payload
                                .get("duration_ms")
                                .and_then(serde_json::Value::as_u64)
                                .or_else(|| item.and_then(|item| item.duration_ms)),
                        )
                        .content(item.and_then(|item| item.content.clone()))
                        .tool(ToolRecord {
                            call_id,
                            name,
                            arguments: None,
                            success,
                        })
                        .details(details),
                );
            }
            "turn.completed" => {
                push_record(
                    &mut records,
                    record("verlet", RecordKind::TurnBoundary, turn, *round)
                        .timestamp(timestamp_ms)
                        .latency(elapsed_ms(
                            start_by_turn.get(&event_turn_id).copied(),
                            timestamp_ms,
                        ))
                        .boundary("completed")
                        .details(details),
                );
            }
            _ => {
                push_record(
                    &mut records,
                    record("verlet", RecordKind::Unmapped, turn, *round)
                        .timestamp(timestamp_ms)
                        .boundary(event_kind)
                        .details(details),
                );
            }
        }
    }

    for (turn_id, view) in turn_views {
        let Some(turn) = turn_ordinals.get(&turn_id).copied() else {
            continue;
        };
        let assistant_indices = records
            .iter()
            .enumerate()
            .filter(|(_, record)| {
                record.turn == turn && record.kind == RecordKind::AssistantMessage
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let record_offset = assistant_indices
            .len()
            .saturating_sub(view.assistant_texts.len());
        let text_offset = view
            .assistant_texts
            .len()
            .saturating_sub(assistant_indices.len());
        for (record_index, text) in assistant_indices
            .into_iter()
            .skip(record_offset)
            .zip(view.assistant_texts.into_iter().skip(text_offset))
        {
            records[record_index].content = Some(text);
        }
    }
    annotate_edit_signals(&mut records);
    Ok(records)
}

pub fn summarize(records: &[CommonRecord]) -> TraceStats {
    let mut stats = TraceStats::default();
    let mut turns = std::collections::BTreeSet::new();
    let mut first_timestamp = None;
    let mut last_timestamp = None;
    let mut first_turn_started = None;
    let mut last_turn_ended = None;
    let mut saw_turn_started = false;
    let mut saw_turn_ended = false;
    for record in records {
        if record.turn > 0 {
            turns.insert(record.turn);
        }
        if record.kind == RecordKind::AssistantMessage {
            stats.rounds += 1;
        }
        if record.kind == RecordKind::Unmapped {
            stats.unmapped_records += 1;
        }
        if record.kind == RecordKind::ToolCall
            && let Some(tool) = &record.tool
        {
            *stats.tool_calls.entry(tool.name.clone()).or_default() += 1;
        }
        if let Some(tokens) = &record.tokens {
            stats.tokens.add_assign(tokens);
        }
        if let Some(edit) = &record.edit {
            stats.edit_failures += u64::from(edit.failed);
            stats.edit_retries += u64::from(edit.retry);
        }
        if record.kind == RecordKind::TurnBoundary && record.boundary.as_deref() == Some("started")
        {
            saw_turn_started = true;
        }
        if record.kind == RecordKind::TurnBoundary
            && matches!(
                record.boundary.as_deref(),
                Some("completed" | "aborted" | "failed")
            )
        {
            saw_turn_ended = true;
        }
        if let Some(timestamp_ms) = record.timestamp_ms {
            first_timestamp =
                Some(first_timestamp.map_or(timestamp_ms, |old: i64| old.min(timestamp_ms)));
            last_timestamp =
                Some(last_timestamp.map_or(timestamp_ms, |old: i64| old.max(timestamp_ms)));
            if record.kind == RecordKind::TurnBoundary
                && record.boundary.as_deref() == Some("started")
            {
                first_turn_started =
                    Some(first_turn_started.map_or(timestamp_ms, |old: i64| old.min(timestamp_ms)));
            }
            if record.kind == RecordKind::TurnBoundary
                && matches!(
                    record.boundary.as_deref(),
                    Some("completed" | "aborted" | "failed")
                )
            {
                last_turn_ended =
                    Some(last_turn_ended.map_or(timestamp_ms, |old: i64| old.max(timestamp_ms)));
            }
        }
    }
    stats.turns = turns.len() as u64;
    stats.wall_time_ms = if saw_turn_started || saw_turn_ended {
        elapsed_ms(first_turn_started, last_turn_ended)
    } else {
        elapsed_ms(first_timestamp, last_timestamp)
    };
    stats
}

pub fn render_diff(pi: &[CommonRecord], verlet: &[CommonRecord]) -> String {
    let pi_stats = summarize(pi);
    let verlet_stats = summarize(verlet);
    let mut output = String::new();
    output.push_str("TRACE A/B\n");
    output.push_str(&format_stats("PI", &pi_stats));
    output.push('\n');
    output.push_str(&format_stats("VERLET", &verlet_stats));
    output.push_str("\n\n");

    const WIDTH: usize = 72;
    output.push_str(&format!("{:<WIDTH$} | {:<WIDTH$}\n", "PI", "VERLET"));
    output.push_str(&format!("{:-<WIDTH$}-+-{:-<WIDTH$}\n", "", ""));
    let pi_grouped = group_for_diff(pi);
    let verlet_grouped = group_for_diff(verlet);
    let keys = pi_grouped
        .keys()
        .chain(verlet_grouped.keys())
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    for (turn, round) in keys {
        let label = format!("T{turn} R{round}");
        output.push_str(&format!("{label}\n"));
        let left = pi_grouped.get(&(turn, round)).cloned().unwrap_or_default();
        let right = verlet_grouped
            .get(&(turn, round))
            .cloned()
            .unwrap_or_default();
        for (left, right) in align_records(&left, &right) {
            let left = left.map_or_else(String::new, format_record);
            let right = right.map_or_else(String::new, format_record);
            output.push_str(&format!(
                "{:<WIDTH$} | {:<WIDTH$}\n",
                truncate(&left, WIDTH),
                truncate(&right, WIDTH)
            ));
        }
    }
    output
}

pub fn write_common_jsonl<W: std::io::Write>(
    records: &[CommonRecord],
    mut writer: W,
) -> Result<(), String> {
    for record in records {
        serde_json::to_writer(&mut writer, record)
            .map_err(|err| format!("failed to encode common trace: {err}"))?;
        writer
            .write_all(b"\n")
            .map_err(|err| format!("failed to write common trace: {err}"))?;
    }
    Ok(())
}

pub fn read_common_jsonl<R: std::io::BufRead>(reader: R) -> Result<Vec<CommonRecord>, String> {
    reader
        .lines()
        .enumerate()
        .filter_map(|(index, line)| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some((index, other)),
        })
        .map(|(index, line)| {
            let line =
                line.map_err(|err| format!("failed to read common line {}: {err}", index + 1))?;
            serde_json::from_str(&line)
                .map_err(|err| format!("invalid common JSON on line {}: {err}", index + 1))
        })
        .collect()
}

struct RecordBuilder(CommonRecord);

fn record(harness: &str, kind: RecordKind, turn: u32, round: u32) -> RecordBuilder {
    RecordBuilder(CommonRecord {
        schema: COMMON_TRACE_SCHEMA.to_string(),
        harness: harness.to_string(),
        kind,
        turn,
        round,
        sequence: 0,
        timestamp_ms: None,
        latency_ms: None,
        content: None,
        boundary: None,
        tokens: None,
        tool: None,
        edit: None,
        details: std::collections::BTreeMap::new(),
    })
}

impl RecordBuilder {
    fn timestamp(mut self, value: Option<i64>) -> Self {
        self.0.timestamp_ms = value;
        self
    }

    fn latency(mut self, value: Option<u64>) -> Self {
        self.0.latency_ms = value;
        self
    }

    fn content(mut self, value: Option<String>) -> Self {
        self.0.content = value.filter(|value| !value.is_empty());
        self
    }

    fn boundary(mut self, value: &str) -> Self {
        self.0.boundary = Some(value.to_string());
        self
    }

    fn tokens(mut self, value: Option<TokenUsage>) -> Self {
        self.0.tokens = value;
        self
    }

    fn tool(mut self, value: ToolRecord) -> Self {
        self.0.tool = Some(value);
        self
    }

    fn details(mut self, value: std::collections::BTreeMap<String, serde_json::Value>) -> Self {
        self.0.details = value;
        self
    }
}

impl From<RecordBuilder> for CommonRecord {
    fn from(value: RecordBuilder) -> Self {
        value.0
    }
}

fn push_record(records: &mut Vec<CommonRecord>, record: RecordBuilder) {
    let mut record = CommonRecord::from(record);
    record.sequence = records.len() as u64 + 1;
    records.push(record);
}

fn ensure_pi_turn(
    records: &mut Vec<CommonRecord>,
    turn: &mut u32,
    active_turn: &mut bool,
    turn_started_ms: &mut Option<i64>,
    timestamp_ms: Option<i64>,
) {
    if *active_turn {
        return;
    }
    *turn += 1;
    *active_turn = true;
    *turn_started_ms = timestamp_ms;
    push_record(
        records,
        record("pi", RecordKind::TurnBoundary, *turn, 0)
            .timestamp(timestamp_ms)
            .boundary("started")
            .details(std::collections::BTreeMap::from([(
                "pi".to_string(),
                serde_json::json!({"entry": null, "derived": "missing_user_boundary"}),
            )])),
    );
}

fn finish_pi_turn(
    records: &mut Vec<CommonRecord>,
    turn: u32,
    round: u32,
    started_ms: Option<i64>,
    ended_ms: Option<i64>,
    outcome: &str,
    derived: serde_json::Value,
) {
    push_record(
        records,
        record("pi", RecordKind::TurnBoundary, turn, round)
            .timestamp(ended_ms)
            .latency(elapsed_ms(started_ms, ended_ms))
            .boundary(outcome)
            .details(std::collections::BTreeMap::from([(
                "pi".to_string(),
                serde_json::json!({"entry": null, "derivation": derived}),
            )])),
    );
}

fn pi_turn_outcome(message: &serde_json::Map<String, serde_json::Value>) -> Option<&'static str> {
    match message
        .get("stopReason")
        .and_then(serde_json::Value::as_str)
    {
        Some("aborted" | "abort" | "cancelled" | "canceled") => Some("aborted"),
        Some("error" | "failed") => Some("failed"),
        _ if message
            .get("errorMessage")
            .is_some_and(|value| !value.is_null()) =>
        {
            Some("failed")
        }
        _ => None,
    }
}

fn resolve_pi_tool_result_rounds(records: &mut [CommonRecord]) {
    let calls = records
        .iter()
        .filter(|record| record.kind == RecordKind::ToolCall)
        .filter_map(|record| {
            record
                .tool
                .as_ref()
                .map(|tool| (tool.call_id.clone(), (record.turn, record.round)))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    for record in records {
        if record.kind != RecordKind::ToolResult {
            continue;
        }
        let Some((turn, round)) = record
            .tool
            .as_ref()
            .and_then(|tool| calls.get(&tool.call_id))
            .copied()
        else {
            continue;
        };
        record.turn = turn;
        record.round = round;
    }
}

fn pi_details(
    entry: &serde_json::Value,
    message: &serde_json::Map<String, serde_json::Value>,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    std::collections::BTreeMap::from([(
        "pi".to_string(),
        serde_json::json!({
            "entry": entry,
            "role": message.get("role"),
            "provider": message.get("provider"),
            "model": message.get("model"),
            "stop_reason": message.get("stopReason"),
            "error_message": message.get("errorMessage"),
        }),
    )])
}

fn pi_entry_details(
    entry: &serde_json::Value,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    std::collections::BTreeMap::from([("pi".to_string(), serde_json::json!({"entry": entry}))])
}

fn pi_token_usage(value: Option<&serde_json::Value>) -> Option<TokenUsage> {
    let value = value?;
    if !value.is_object() {
        return None;
    }
    let input = u64_field(value, "input");
    let output = u64_field(value, "output");
    let cache_read = u64_field(value, "cacheRead");
    let cache_write = u64_field(value, "cacheWrite");
    let total = value
        .get("totalTokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| {
            input
                .saturating_add(output)
                .saturating_add(cache_read)
                .saturating_add(cache_write)
        });
    Some(TokenUsage {
        input,
        output,
        cache_read,
        cache_write,
        total,
    })
}

fn verlet_token_usage(value: Option<&serde_json::Value>) -> Option<TokenUsage> {
    let value = value?;
    if !value.is_object() {
        return None;
    }
    let input = u64_field(value, "input_tokens");
    let output = u64_field(value, "output_tokens");
    let cache_read = u64_field(value, "cache_read_input_tokens");
    let cache_write = u64_field(value, "cache_creation_input_tokens");
    Some(TokenUsage {
        input,
        output,
        cache_read,
        cache_write,
        total: input
            .saturating_add(output)
            .saturating_add(cache_read)
            .saturating_add(cache_write),
    })
}

fn verlet_source_details(
    bundle: &serde_json::Value,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    let streams = bundle
        .get("streams")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .map(|stream| {
            let mut metadata = stream.clone();
            if let Some(object) = metadata.as_object_mut() {
                object.remove("data");
            }
            metadata
        })
        .collect::<Vec<_>>();
    std::collections::BTreeMap::from([(
        "verlet".to_string(),
        serde_json::json!({
            "thread_id": bundle.get("threadId"),
            "generated_at_ms": bundle.get("generatedAtMs"),
            "backend": bundle.get("backend"),
            "ack_classes": bundle.get("ackClasses"),
            "redaction": bundle.get("redaction"),
            "export_receipts": bundle.get("receipts"),
            "thread": bundle.get("thread"),
            "streams": streams,
        }),
    )])
}

fn verlet_event_details(
    event: &serde_json::Value,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    std::collections::BTreeMap::from([("verlet".to_string(), serde_json::json!({"event": event}))])
}

#[derive(Default)]
struct TurnView {
    assistant_texts: Vec<String>,
    assistant_entry_ids: std::collections::BTreeSet<String>,
    tools: std::collections::BTreeMap<String, ToolView>,
}

#[derive(Default)]
struct ToolView {
    name: Option<String>,
    arguments: Option<serde_json::Value>,
    success: Option<bool>,
    duration_ms: Option<u64>,
    content: Option<String>,
}

fn verlet_turn_views(bundle: &serde_json::Value) -> std::collections::BTreeMap<String, TurnView> {
    let mut views = std::collections::BTreeMap::new();
    let Some(turns) = bundle
        .pointer("/thread/turns")
        .and_then(serde_json::Value::as_array)
    else {
        return views;
    };
    for turn in turns {
        let Some(turn_id) = turn.get("id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let mut view = TurnView::default();
        for item in turn
            .get("items")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            match item.get("type").and_then(serde_json::Value::as_str) {
                Some("agentMessage") => {
                    if let Some(id) = item.get("id").and_then(serde_json::Value::as_str) {
                        view.assistant_entry_ids.insert(id.to_string());
                    }
                    if let Some(text) = item
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                        .or_else(|| message_text(item.get("content")))
                    {
                        view.assistant_texts.push(text);
                    }
                }
                Some("dynamicToolCall") => {
                    let Some(call_id) = item.get("id").and_then(serde_json::Value::as_str) else {
                        continue;
                    };
                    view.tools.insert(
                        call_id.to_string(),
                        ToolView {
                            name: item
                                .get("tool")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string),
                            arguments: item.get("arguments").cloned(),
                            success: item.get("success").and_then(serde_json::Value::as_bool),
                            duration_ms: item.get("durationMs").and_then(serde_json::Value::as_u64),
                            content: message_text(item.get("contentItems")),
                        },
                    );
                }
                _ => {}
            }
        }
        views.insert(turn_id.to_string(), view);
    }
    views
}

fn is_verlet_assistant_entry(
    payload: &serde_json::Value,
    turn_id: &str,
    turn_views: &std::collections::BTreeMap<String, TurnView>,
) -> bool {
    if payload.get("usage").is_some() {
        return true;
    }
    let Some(entry_id) = payload.get("entry_id").and_then(serde_json::Value::as_str) else {
        return false;
    };
    turn_views
        .get(turn_id)
        .is_some_and(|view| view.assistant_entry_ids.contains(entry_id))
}

fn event_sort_key(event: &serde_json::Value) -> (i64, String, u64, String) {
    (
        event_timestamp(event).unwrap_or_default(),
        event
            .get("stream_id")
            .or_else(|| event.get("streamId"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        event
            .get("sequence")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        event
            .get("event_id")
            .or_else(|| event.get("eventId"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
    )
}

fn event_timestamp(event: &serde_json::Value) -> Option<i64> {
    event
        .get("created_at_ms")
        .or_else(|| event.get("atMs"))
        .and_then(serde_json::Value::as_i64)
}

fn event_turn_id(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("turn_id")
        .or_else(|| payload.pointer("/subject/turn_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn ordinal_for_turn(turns: &mut std::collections::BTreeMap<String, u32>, turn_id: &str) -> u32 {
    if turn_id.is_empty() {
        return 0;
    }
    if let Some(ordinal) = turns.get(turn_id) {
        return *ordinal;
    }
    let ordinal = turns.len() as u32 + 1;
    turns.insert(turn_id.to_string(), ordinal);
    ordinal
}

fn annotate_edit_signals(records: &mut [CommonRecord]) {
    let mut failed = std::collections::BTreeMap::<u32, std::collections::BTreeSet<String>>::new();
    for record in records {
        let Some(tool) = &record.tool else {
            continue;
        };
        if !is_edit_tool(&tool.name) {
            continue;
        }
        let key = record.turn;
        match record.kind {
            RecordKind::ToolCall => {
                record.edit = Some(EditSignal {
                    application: true,
                    failed: false,
                    retry: failed
                        .get(&key)
                        .is_some_and(|call_ids| call_ids.iter().any(|id| id != &tool.call_id)),
                });
            }
            RecordKind::ToolResult => {
                let result_failed = tool.success == Some(false);
                if result_failed {
                    failed.entry(key).or_default().insert(tool.call_id.clone());
                } else if tool.success == Some(true) {
                    failed.remove(&key);
                }
                record.edit = Some(EditSignal {
                    application: true,
                    failed: result_failed,
                    retry: false,
                });
            }
            _ => {}
        }
    }
}

fn is_edit_tool(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase().replace('-', "_");
    normalized == "edit"
        || normalized == "apply_patch"
        || normalized == "applypatch"
        || normalized.ends_with(".edit")
        || normalized.ends_with("/edit")
}

fn group_for_diff(
    records: &[CommonRecord],
) -> std::collections::BTreeMap<(u32, u32), Vec<&CommonRecord>> {
    let mut grouped = std::collections::BTreeMap::<(u32, u32), Vec<&CommonRecord>>::new();
    for record in records {
        grouped
            .entry((record.turn, record.round))
            .or_default()
            .push(record);
    }
    grouped
}

fn align_records<'a>(
    left: &[&'a CommonRecord],
    right: &[&'a CommonRecord],
) -> Vec<(Option<&'a CommonRecord>, Option<&'a CommonRecord>)> {
    let mut lengths = vec![vec![0_usize; right.len() + 1]; left.len() + 1];
    for left_index in (0..left.len()).rev() {
        for right_index in (0..right.len()).rev() {
            lengths[left_index][right_index] =
                if alignment_key(left[left_index]) == alignment_key(right[right_index]) {
                    lengths[left_index + 1][right_index + 1] + 1
                } else {
                    lengths[left_index + 1][right_index].max(lengths[left_index][right_index + 1])
                };
        }
    }

    let mut aligned = Vec::new();
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() || right_index < right.len() {
        if left_index < left.len()
            && right_index < right.len()
            && alignment_key(left[left_index]) == alignment_key(right[right_index])
        {
            aligned.push((Some(left[left_index]), Some(right[right_index])));
            left_index += 1;
            right_index += 1;
        } else if left_index < left.len()
            && (right_index == right.len()
                || lengths[left_index + 1][right_index] >= lengths[left_index][right_index + 1])
        {
            aligned.push((Some(left[left_index]), None));
            left_index += 1;
        } else {
            aligned.push((None, Some(right[right_index])));
            right_index += 1;
        }
    }
    aligned
}

fn alignment_key(record: &CommonRecord) -> String {
    match record.kind {
        RecordKind::SourceMetadata => "source".to_string(),
        RecordKind::AssistantMessage => "assistant".to_string(),
        RecordKind::ToolCall => format!(
            "call:{}",
            record.tool.as_ref().map_or("", |tool| tool.name.as_str())
        ),
        RecordKind::ToolResult => format!(
            "result:{}",
            record.tool.as_ref().map_or("", |tool| tool.name.as_str())
        ),
        RecordKind::TurnBoundary => {
            format!("boundary:{}", record.boundary.as_deref().unwrap_or(""))
        }
        RecordKind::Compaction => "compaction".to_string(),
        RecordKind::Unmapped => format!("unmapped:{}", record.boundary.as_deref().unwrap_or("")),
    }
}

fn format_stats(label: &str, stats: &TraceStats) -> String {
    let tools = if stats.tool_calls.is_empty() {
        "none".to_string()
    } else {
        stats
            .tool_calls
            .iter()
            .map(|(name, count)| format!("{name}={count}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let wall = stats
        .wall_time_ms
        .map_or_else(|| "n/a".to_string(), |value| format!("{value} ms"));
    format!(
        "{label}: turns: {}, rounds: {}, tools: {tools}, tokens: {}, wall: {wall}, edit failures: {}, edit retries: {}, unmapped: {}",
        stats.turns,
        stats.rounds,
        stats.tokens.total,
        stats.edit_failures,
        stats.edit_retries,
        stats.unmapped_records,
    )
}

fn format_record(record: &CommonRecord) -> String {
    match record.kind {
        RecordKind::SourceMetadata => "SOURCE METADATA".to_string(),
        RecordKind::AssistantMessage => format!(
            "ASSISTANT {}",
            record.content.as_deref().unwrap_or("<no text>")
        ),
        RecordKind::ToolCall => {
            let tool = record
                .tool
                .as_ref()
                .expect("tool-call records have tool metadata");
            let prefix = if record.edit.as_ref().is_some_and(|edit| edit.retry) {
                "RETRY"
            } else {
                "CALL"
            };
            format!(
                "{prefix} {} {}",
                tool.name,
                tool.arguments
                    .as_ref()
                    .map(serde_json::Value::to_string)
                    .unwrap_or_default()
            )
        }
        RecordKind::ToolResult => {
            let tool = record
                .tool
                .as_ref()
                .expect("tool-result records have tool metadata");
            let prefix = if tool.success == Some(false) {
                "FAIL"
            } else {
                "OK"
            };
            format!(
                "{prefix} {} {}",
                tool.name,
                record.content.as_deref().unwrap_or_default()
            )
        }
        RecordKind::TurnBoundary => format!(
            "BOUNDARY {}",
            record.boundary.as_deref().unwrap_or("unknown")
        ),
        RecordKind::Compaction => format!(
            "COMPACTION {}",
            record.content.as_deref().unwrap_or_default()
        ),
        RecordKind::Unmapped => format!(
            "UNMAPPED {}",
            record.boundary.as_deref().unwrap_or("unknown")
        ),
    }
}

fn truncate(value: &str, width: usize) -> String {
    let value = value
        .chars()
        .map(|character| {
            if matches!(character, '\n' | '\r' | '\t') || character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    if value.chars().count() <= width {
        return value;
    }
    value
        .chars()
        .take(width.saturating_sub(1))
        .collect::<String>()
        + "…"
}

fn message_text(value: Option<&serde_json::Value>) -> Option<String> {
    match value? {
        serde_json::Value::String(text) => Some(text.clone()),
        serde_json::Value::Array(blocks) => {
            let text = blocks
                .iter()
                .filter_map(|block| {
                    block
                        .get("text")
                        .or_else(|| block.get("thinking"))
                        .and_then(serde_json::Value::as_str)
                })
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}

fn elapsed_ms(start: Option<i64>, end: Option<i64>) -> Option<u64> {
    end?.checked_sub(start?)
        .and_then(|value| u64::try_from(value).ok())
}

fn u64_field(value: &serde_json::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default()
}

fn string_field(value: &serde_json::Value, key: &str, fallback: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or(fallback)
        .to_string()
}

fn string_field_from_map(
    value: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    fallback: &str,
) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or(fallback)
        .to_string()
}
