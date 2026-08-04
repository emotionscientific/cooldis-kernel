use super::{RuntimeEventKind, RuntimeThreadHandle};
use crate::kernel::history::{CanonicalContent, CanonicalMessage};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use verlet_runtime_contracts::{RuntimeEventId, ThreadId, ThreadInteractionKind, ThreadStatus};

pub(super) fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(super) async fn wait_until_thread_settled(thread: &RuntimeThreadHandle) {
    let mut status = thread.subscribe_status();
    loop {
        if matches!(
            *status.borrow(),
            ThreadStatus::Idle | ThreadStatus::Stopped | ThreadStatus::Failed
        ) && thread.queued_command_count() == 0
        {
            return;
        }
        if status.changed().await.is_err() {
            return;
        }
    }
}

pub(super) fn latest_message_text(messages: &[CanonicalMessage]) -> Option<String> {
    messages.iter().rev().find_map(|message| {
        let text = match message {
            CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .filter_map(|content| match content {
                    CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(""),
            CanonicalMessage::User { .. } => String::new(),
        };
        if text.is_empty() { None } else { Some(text) }
    })
}

const THREAD_INTERACTION_RESULT_PREVIEW_MAX_CHARS: usize = 512;

pub(super) fn thread_interaction_preview(output: &str) -> String {
    let mut chars = output.chars();
    let preview: String = chars
        .by_ref()
        .take(THREAD_INTERACTION_RESULT_PREVIEW_MAX_CHARS)
        .collect();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

pub(super) fn emit_thread_interaction(
    thread: &RuntimeThreadHandle,
    interaction_id: RuntimeEventId,
    kind: ThreadInteractionKind,
    source_thread_id: ThreadId,
    target_thread_id: ThreadId,
    source_turn_id: Option<String>,
    target_turn_id: Option<String>,
    result_preview: Option<String>,
    metadata: BTreeMap<String, String>,
) {
    thread.emit_runtime(RuntimeEventKind::ThreadInteraction {
        interaction_id,
        kind,
        source_thread_id,
        target_thread_id,
        source_turn_id,
        target_turn_id,
        result_preview,
        metadata,
    });
}
