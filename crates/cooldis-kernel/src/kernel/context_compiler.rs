use crate::{
    CanonicalContent, CanonicalMessage, SessionEntry, SessionEntryId, SessionEntryKind,
    SystemBlock, ToolDefinition, TurnContextSnapshot, compaction_summary_message,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentContextAttachment {
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentContextSource {
    Environment,
    Attachment,
    History,
    CompactionSummary,
    HookContext,
    TurnContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgentContextDroppedEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<SessionEntryId>,
    pub source: AgentContextSource,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentContextCompileInput {
    pub system: Vec<SystemBlock>,
    pub session_entries: Vec<SessionEntry>,
    pub turn_context: TurnContextSnapshot,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hook_contexts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environment_contexts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AgentContextAttachment>,
    pub tools: Vec<ToolDefinition>,
    #[serde(default)]
    pub policy: AgentContextCompilePolicy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompiledAgentContext {
    pub system: Vec<SystemBlock>,
    pub messages: Vec<CanonicalMessage>,
    pub tools: Vec<ToolDefinition>,
    pub diagnostics: AgentContextCompilationDiagnostics,
}

#[derive(Clone, Debug, PartialEq)]
struct TrackedMessage {
    message: CanonicalMessage,
    source: AgentContextSource,
    entry_id: Option<SessionEntryId>,
}

pub struct AgentContextCompiler;

impl AgentContextCompiler {
    pub fn compile(input: AgentContextCompileInput) -> CompiledAgentContext {
        let mut diagnostics = AgentContextCompilationDiagnostics {
            input_entry_count: input.session_entries.len(),
            system_block_count: input.system.len(),
            tool_count: input.tools.len(),
            attachment_count: input.attachments.len(),
            ..AgentContextCompilationDiagnostics::default()
        };
        let mut messages = Vec::new();

        for context in render_environment_contexts(&input.turn_context, &input.environment_contexts)
        {
            messages.push(TrackedMessage {
                message: CanonicalMessage::user_text(context),
                source: AgentContextSource::Environment,
                entry_id: None,
            });
        }

        for entry in &input.session_entries {
            match &entry.kind {
                SessionEntryKind::Message { message } => messages.push(TrackedMessage {
                    message: message.clone(),
                    source: AgentContextSource::History,
                    entry_id: Some(entry.entry_id),
                }),
                SessionEntryKind::CustomContextMessage { message } => {
                    messages.push(TrackedMessage {
                        message: message.clone(),
                        source: AgentContextSource::HookContext,
                        entry_id: Some(entry.entry_id),
                    })
                }
                SessionEntryKind::Compaction { summary } => {
                    diagnostics
                        .dropped_entries
                        .extend(messages.iter().map(|message| AgentContextDroppedEntry {
                            entry_id: message.entry_id,
                            source: message.source.clone(),
                            reason: "cleared_by_compaction".to_string(),
                        }));
                    messages.clear();
                    messages.push(TrackedMessage {
                        message: compaction_summary_message(summary),
                        source: AgentContextSource::CompactionSummary,
                        entry_id: Some(entry.entry_id),
                    });
                }
                SessionEntryKind::ModelChange { .. }
                | SessionEntryKind::BranchSummary { .. }
                | SessionEntryKind::Runtime { .. } => {}
            }
        }

        for context in input
            .hook_contexts
            .into_iter()
            .filter(|context| !context.trim().is_empty())
        {
            messages.push(TrackedMessage {
                message: CanonicalMessage::user_text(context),
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
                message: CanonicalMessage::user_text(context.clone()),
                source: AgentContextSource::TurnContext,
                entry_id: None,
            });
        }

        if let Some(attachment_context) = render_attachment_context(&input.attachments) {
            messages.push(TrackedMessage {
                message: CanonicalMessage::user_text(attachment_context),
                source: AgentContextSource::Attachment,
                entry_id: None,
            });
        }

        apply_message_budget(&mut messages, &input.policy, &mut diagnostics);
        let messages = messages
            .into_iter()
            .map(|tracked| tracked.message)
            .collect::<Vec<_>>();
        diagnostics.output_message_count = messages.len();
        diagnostics.retained_text_bytes = messages_text_bytes(&messages);

        CompiledAgentContext {
            system: input.system,
            messages,
            tools: input.tools,
            diagnostics,
        }
    }
}

fn render_environment_contexts(
    turn_context: &TurnContextSnapshot,
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

fn render_turn_environment_context(turn_context: &TurnContextSnapshot) -> Option<String> {
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

fn messages_text_bytes(messages: &[CanonicalMessage]) -> usize {
    messages.iter().map(message_text_bytes).sum()
}

fn message_text_bytes(message: &CanonicalMessage) -> usize {
    match message {
        CanonicalMessage::User { content, .. }
        | CanonicalMessage::Assistant { content, .. }
        | CanonicalMessage::ToolResult { content, .. } => content_text_bytes(content),
    }
}

fn content_text_bytes(content: &[CanonicalContent]) -> usize {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } | CanonicalContent::Thinking { text, .. } => {
                Some(text.len())
            }
            CanonicalContent::Image { .. } | CanonicalContent::ToolCall { .. } => None,
        })
        .sum()
}

fn truncate_messages_to_recent_text_bytes(messages: &mut [TrackedMessage], max_bytes: usize) {
    let mut remaining = max_bytes;
    for message in messages.iter_mut().rev() {
        match &mut message.message {
            CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => {
                truncate_content_to_recent_text_bytes(content, &mut remaining);
            }
        }
    }
}

fn truncate_content_to_recent_text_bytes(
    content: &mut Vec<CanonicalContent>,
    remaining: &mut usize,
) {
    for block in content.iter_mut().rev() {
        match block {
            CanonicalContent::Text { text, .. } | CanonicalContent::Thinking { text, .. } => {
                truncate_string_to_recent_bytes(text, remaining);
            }
            CanonicalContent::Image { .. } | CanonicalContent::ToolCall { .. } => {}
        }
    }
    content.retain(|block| match block {
        CanonicalContent::Text { text, .. } | CanonicalContent::Thinking { text, .. } => {
            !text.is_empty()
        }
        CanonicalContent::Image { .. } | CanonicalContent::ToolCall { .. } => true,
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
