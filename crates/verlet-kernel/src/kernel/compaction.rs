pub use verlet_history::{
    COMPACTION_SUMMARY_PREFIX, compaction_summary_message, render_compaction_summary,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionTrigger {
    Manual,
    Auto,
}

impl CompactionTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Auto => "auto",
        }
    }
}

impl std::fmt::Display for CompactionTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CompactionPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_max_context_text_bytes: Option<usize>,
}

impl CompactionPolicy {
    pub fn disabled() -> Self {
        Self {
            auto_max_context_text_bytes: None,
        }
    }

    pub fn auto_at_text_bytes(max_text_bytes: usize) -> Self {
        Self {
            auto_max_context_text_bytes: Some(max_text_bytes),
        }
    }

    pub fn is_disabled(&self) -> bool {
        self.auto_max_context_text_bytes.is_none()
    }
}

pub fn deterministic_compaction_summary(messages: &[crate::CanonicalMessage]) -> String {
    let mut pieces = messages
        .iter()
        .filter_map(message_text)
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>();
    if pieces.is_empty() {
        return "No prior model-visible context.".to_string();
    }
    const MAX_PIECES: usize = 8;
    if pieces.len() > MAX_PIECES {
        pieces = pieces.split_off(pieces.len() - MAX_PIECES);
    }
    pieces.join("\n")
}

fn message_text(message: &crate::CanonicalMessage) -> Option<String> {
    match message {
        crate::CanonicalMessage::User { content, .. }
        | crate::CanonicalMessage::Assistant { content, .. }
        | crate::CanonicalMessage::ToolResult { content, .. } => content_text(content),
    }
}

fn content_text(content: &[crate::CanonicalContent]) -> Option<String> {
    let chunks = content
        .iter()
        .filter_map(|content| match content {
            crate::CanonicalContent::Text { text, .. }
            | crate::CanonicalContent::Thinking { text, .. } => Some(text.as_str()),
            crate::CanonicalContent::Image { .. } | crate::CanonicalContent::ToolCall { .. } => {
                None
            }
        })
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>();
    if chunks.is_empty() {
        None
    } else {
        Some(chunks.join("\n"))
    }
}

#[cfg(test)]
mod tests;
