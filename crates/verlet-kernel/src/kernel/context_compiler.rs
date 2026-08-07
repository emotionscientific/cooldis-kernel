#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentContextCompilePolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_text_bytes: Option<usize>,
}

impl AgentContextCompilePolicy {
    pub fn unbounded() -> Self {
        Self::default()
    }

    pub fn is_unbounded(&self) -> bool {
        self.max_messages.is_none() && self.max_text_bytes.is_none()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentContextAttachment {
    pub path: std::path::PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentContextSource {
    Environment,
    Attachment,
    History,
    CompactionSummary,
    HookContext,
    TurnContext,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentContextDroppedEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<crate::SessionEntryId>,
    pub source: AgentContextSource,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentContextCompilationDiagnostics {
    pub input_entry_count: usize,
    pub output_message_count: usize,
    pub system_block_count: usize,
    pub tool_count: usize,
    pub attachment_count: usize,
    pub retained_text_bytes: usize,
    pub truncated_text_bytes: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropped_entries: Vec<AgentContextDroppedEntry>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentContextCompileInput {
    pub system: Vec<crate::SystemBlock>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub static_system_sources: Vec<crate::AgentManifestStaticContextSegment>,
    pub session_entries: Vec<crate::SessionEntry>,
    /// Persisted turn-start or thread-anchor time for synthetics without one entry source.
    pub turn_anchor_timestamp_ms: i64,
    pub turn_context: crate::TurnContextSnapshot,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hook_contexts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environment_contexts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AgentContextAttachment>,
    pub tools: Vec<crate::ToolDefinition>,
    #[serde(default)]
    pub policy: AgentContextCompilePolicy,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CompiledAgentContext {
    pub system: Vec<crate::SystemBlock>,
    pub messages: Vec<crate::CanonicalMessage>,
    pub tools: Vec<crate::ToolDefinition>,
    pub diagnostics: AgentContextCompilationDiagnostics,
}

#[derive(Clone, Debug, PartialEq)]
struct TrackedMessage {
    message: crate::CanonicalMessage,
    source: AgentContextSource,
    entry_id: Option<crate::SessionEntryId>,
}

struct CompiledSystemBlocks {
    blocks: Vec<crate::SystemBlock>,
    budgeted_text_bytes: usize,
    truncated_text_bytes: usize,
}

pub struct AgentContextCompiler;

impl AgentContextCompiler {
    pub fn compile(input: AgentContextCompileInput) -> CompiledAgentContext {
        let system = compile_system_blocks(
            input.static_system_sources,
            input.system,
            input.policy.max_text_bytes,
        );
        let mut diagnostics = AgentContextCompilationDiagnostics {
            input_entry_count: input.session_entries.len(),
            system_block_count: system.blocks.len(),
            tool_count: input.tools.len(),
            attachment_count: input.attachments.len(),
            truncated_text_bytes: system.truncated_text_bytes,
            ..AgentContextCompilationDiagnostics::default()
        };
        let mut messages = Vec::new();
        let mut message_policy = input.policy.clone();
        if let Some(max_text_bytes) = message_policy.max_text_bytes.as_mut() {
            *max_text_bytes = max_text_bytes.saturating_sub(system.budgeted_text_bytes);
        }

        for context in render_environment_contexts(&input.turn_context, &input.environment_contexts)
        {
            messages.push(TrackedMessage {
                message: crate::CanonicalMessage::user_text_at(
                    context,
                    input.turn_anchor_timestamp_ms,
                ),
                source: AgentContextSource::Environment,
                entry_id: None,
            });
        }

        for entry in &input.session_entries {
            match &entry.kind {
                crate::SessionEntryKind::Message { message } => messages.push(TrackedMessage {
                    message: message.clone(),
                    source: AgentContextSource::History,
                    entry_id: Some(entry.entry_id),
                }),
                crate::SessionEntryKind::CustomContextMessage { message } => {
                    messages.push(TrackedMessage {
                        message: message.clone(),
                        source: AgentContextSource::HookContext,
                        entry_id: Some(entry.entry_id),
                    })
                }
                crate::SessionEntryKind::Compaction { summary } => {
                    diagnostics
                        .dropped_entries
                        .extend(messages.iter().map(|message| AgentContextDroppedEntry {
                            entry_id: message.entry_id,
                            source: message.source.clone(),
                            reason: "cleared_by_compaction".to_string(),
                        }));
                    messages.clear();
                    messages.push(TrackedMessage {
                        message: crate::compaction_summary_message(summary, entry.created_at_ms),
                        source: AgentContextSource::CompactionSummary,
                        entry_id: Some(entry.entry_id),
                    });
                }
                crate::SessionEntryKind::ModelChange { .. }
                | crate::SessionEntryKind::BranchSummary { .. }
                | crate::SessionEntryKind::Runtime { .. } => {}
            }
        }

        for context in input
            .hook_contexts
            .into_iter()
            .filter(|context| !context.trim().is_empty())
        {
            messages.push(TrackedMessage {
                message: crate::CanonicalMessage::user_text_at(
                    context,
                    input.turn_anchor_timestamp_ms,
                ),
                source: AgentContextSource::HookContext,
                entry_id: None,
            });
        }

        for context in input
            .turn_context
            .model_visible_context
            .iter()
            .filter(|context| !context.trim().is_empty())
        {
            messages.push(TrackedMessage {
                message: crate::CanonicalMessage::user_text_at(
                    context.clone(),
                    input.turn_anchor_timestamp_ms,
                ),
                source: AgentContextSource::TurnContext,
                entry_id: None,
            });
        }

        if let Some(attachment_context) = render_attachment_context(&input.attachments) {
            messages.push(TrackedMessage {
                message: crate::CanonicalMessage::user_text_at(
                    attachment_context,
                    input.turn_anchor_timestamp_ms,
                ),
                source: AgentContextSource::Attachment,
                entry_id: None,
            });
        }

        apply_message_budget(&mut messages, &message_policy, &mut diagnostics);
        let messages = messages
            .into_iter()
            .map(|tracked| tracked.message)
            .collect::<Vec<_>>();
        diagnostics.output_message_count = messages.len();
        diagnostics.retained_text_bytes =
            messages_text_bytes(&messages).saturating_add(system.budgeted_text_bytes);

        CompiledAgentContext {
            system: system.blocks,
            messages,
            tools: input.tools,
            diagnostics,
        }
    }
}

fn compile_system_blocks(
    static_sources: Vec<crate::AgentManifestStaticContextSegment>,
    mut configured: Vec<crate::SystemBlock>,
    max_text_bytes: Option<usize>,
) -> CompiledSystemBlocks {
    let mut remaining_budget = max_text_bytes;
    let mut budgeted_text_bytes = 0usize;
    let mut truncated_text_bytes = 0usize;
    let mut blocks = Vec::new();

    for source in static_sources {
        if source.content.trim().is_empty() {
            continue;
        }
        let mut content = source.content;
        if !source.pinned {
            let original_len = content.len();
            if let Some(limit) =
                static_source_budget_limit(max_text_bytes, source.budget_share, remaining_budget)
                && original_len > limit
            {
                content = prefix_text_bytes(&content, limit).to_string();
            }
            if content.trim().is_empty() {
                truncated_text_bytes = truncated_text_bytes.saturating_add(original_len);
                continue;
            }
            let retained_len = content.len();
            truncated_text_bytes =
                truncated_text_bytes.saturating_add(original_len.saturating_sub(retained_len));
            if let Some(remaining) = remaining_budget.as_mut() {
                *remaining = remaining.saturating_sub(retained_len);
            }
            budgeted_text_bytes = budgeted_text_bytes.saturating_add(retained_len);
        }
        if !content.trim().is_empty() {
            blocks.push(crate::SystemBlock::text(content));
        }
    }
    blocks.append(&mut configured);
    CompiledSystemBlocks {
        blocks,
        budgeted_text_bytes,
        truncated_text_bytes,
    }
}

fn static_source_budget_limit(
    max_text_bytes: Option<usize>,
    budget_share: Option<f64>,
    remaining_budget: Option<usize>,
) -> Option<usize> {
    let max_text_bytes = max_text_bytes?;
    let remaining_budget = remaining_budget.unwrap_or(max_text_bytes);
    let share_limit = budget_share
        .map(|share| fractional_budget_limit(max_text_bytes, share))
        .unwrap_or(remaining_budget);
    Some(share_limit.min(remaining_budget))
}

fn fractional_budget_limit(max_text_bytes: usize, share: f64) -> usize {
    if !share.is_finite() || share <= 0.0 {
        return 0;
    }
    ((max_text_bytes as f64) * share).floor() as usize
}

fn prefix_text_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn render_environment_contexts(
    turn_context: &crate::TurnContextSnapshot,
    explicit_contexts: &[String],
) -> Vec<String> {
    let mut contexts = explicit_contexts
        .iter()
        .filter(|context| !context.trim().is_empty())
        .cloned()
        .collect::<Vec<_>>();
    let rendered = render_turn_environment_context(turn_context);
    if let Some(rendered) = rendered {
        contexts.push(rendered);
    }
    contexts
}

fn render_turn_environment_context(turn_context: &crate::TurnContextSnapshot) -> Option<String> {
    let has_context = turn_context.cwd.is_some()
        || !turn_context.workspace_roots.is_empty()
        || !turn_context.environment.is_empty()
        || !turn_context.metadata.is_empty();
    if !has_context {
        return None;
    }
    let mut lines = vec!["<environment_context>".to_string()];
    if let Some(cwd) = &turn_context.cwd {
        lines.push(format!("cwd={}", cwd.display()));
    }
    for root in &turn_context.workspace_roots {
        lines.push(format!("workspace_root={}", root.display()));
    }
    for (key, value) in &turn_context.environment {
        lines.push(format!("env.{key}={value}"));
    }
    for (key, value) in &turn_context.metadata {
        lines.push(format!("metadata.{key}={value}"));
    }
    lines.push("</environment_context>".to_string());
    Some(lines.join("\n"))
}

fn render_attachment_context(attachments: &[AgentContextAttachment]) -> Option<String> {
    if attachments.is_empty() {
        return None;
    }
    let mut lines = vec!["<attachments>".to_string()];
    for attachment in attachments {
        let mut fields = vec![format!("path={}", attachment.path.display())];
        if let Some(mime_type) = &attachment.mime_type {
            fields.push(format!("mime_type={mime_type}"));
        }
        if let Some(size_bytes) = attachment.size_bytes {
            fields.push(format!("size_bytes={size_bytes}"));
        }
        if let Some(sha256) = &attachment.sha256 {
            fields.push(format!("sha256={sha256}"));
        }
        for (key, value) in &attachment.metadata {
            fields.push(format!("metadata.{key}={value}"));
        }
        lines.push(fields.join(" "));
    }
    lines.push("</attachments>".to_string());
    Some(lines.join("\n"))
}

fn apply_message_budget(
    messages: &mut Vec<TrackedMessage>,
    policy: &AgentContextCompilePolicy,
    diagnostics: &mut AgentContextCompilationDiagnostics,
) {
    if let Some(max_messages) = policy.max_messages {
        let dropped = messages.len().saturating_sub(max_messages);
        for message in messages.drain(0..dropped) {
            diagnostics.dropped_entries.push(AgentContextDroppedEntry {
                entry_id: message.entry_id,
                source: message.source,
                reason: "max_messages".to_string(),
            });
        }
    }

    let original_text_bytes = messages_text_bytes_for_tracked(messages);
    if let Some(max_text_bytes) = policy.max_text_bytes {
        truncate_messages_to_recent_text_bytes(messages, max_text_bytes);
    }
    let retained_text_bytes = messages_text_bytes_for_tracked(messages);
    diagnostics.truncated_text_bytes = diagnostics
        .truncated_text_bytes
        .saturating_add(original_text_bytes.saturating_sub(retained_text_bytes));
}

fn messages_text_bytes_for_tracked(messages: &[TrackedMessage]) -> usize {
    messages
        .iter()
        .map(|message| message_text_bytes(&message.message))
        .sum()
}

fn messages_text_bytes(messages: &[crate::CanonicalMessage]) -> usize {
    messages.iter().map(message_text_bytes).sum()
}

fn message_text_bytes(message: &crate::CanonicalMessage) -> usize {
    match message {
        crate::CanonicalMessage::User { content, .. }
        | crate::CanonicalMessage::Assistant { content, .. }
        | crate::CanonicalMessage::ToolResult { content, .. } => content_text_bytes(content),
    }
}

fn content_text_bytes(content: &[crate::CanonicalContent]) -> usize {
    content
        .iter()
        .filter_map(|content| match content {
            crate::CanonicalContent::Text { text, .. }
            | crate::CanonicalContent::Thinking { text, .. } => Some(text.len()),
            crate::CanonicalContent::Image { .. } | crate::CanonicalContent::ToolCall { .. } => {
                None
            }
        })
        .sum()
}

fn truncate_messages_to_recent_text_bytes(messages: &mut [TrackedMessage], max_bytes: usize) {
    let mut remaining = max_bytes;
    for message in messages.iter_mut().rev() {
        match &mut message.message {
            crate::CanonicalMessage::User { content, .. }
            | crate::CanonicalMessage::Assistant { content, .. }
            | crate::CanonicalMessage::ToolResult { content, .. } => {
                truncate_content_to_recent_text_bytes(content, &mut remaining);
            }
        }
    }
}

fn truncate_content_to_recent_text_bytes(
    content: &mut Vec<crate::CanonicalContent>,
    remaining: &mut usize,
) {
    for block in content.iter_mut().rev() {
        match block {
            crate::CanonicalContent::Text { text, .. }
            | crate::CanonicalContent::Thinking { text, .. } => {
                truncate_string_to_recent_bytes(text, remaining);
            }
            crate::CanonicalContent::Image { .. } | crate::CanonicalContent::ToolCall { .. } => {}
        }
    }
    content.retain(|block| match block {
        crate::CanonicalContent::Text { text, .. }
        | crate::CanonicalContent::Thinking { text, .. } => !text.is_empty(),
        crate::CanonicalContent::Image { .. } | crate::CanonicalContent::ToolCall { .. } => true,
    });
}

fn truncate_string_to_recent_bytes(text: &mut String, remaining: &mut usize) {
    let bytes = text.len();
    if bytes <= *remaining {
        *remaining -= bytes;
        return;
    }
    if *remaining == 0 {
        text.clear();
        return;
    }
    let start = text
        .char_indices()
        .map(|(index, _)| index)
        .find(|index| bytes - *index <= *remaining)
        .unwrap_or(bytes);
    let suffix = text[start..].to_string();
    *remaining = (*remaining).saturating_sub(suffix.len());
    *text = suffix;
}

#[cfg(test)]
mod tests;
