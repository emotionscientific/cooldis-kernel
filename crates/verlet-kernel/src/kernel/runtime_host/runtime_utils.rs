pub(super) fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(super) async fn wait_until_thread_settled(
    thread: &crate::kernel::runtime_host::RuntimeThreadHandle,
) {
    let mut status = thread.subscribe_status();
    loop {
        if matches!(
            *status.borrow(),
            verlet_runtime_contracts::ThreadStatus::Idle
                | verlet_runtime_contracts::ThreadStatus::Stopped
                | verlet_runtime_contracts::ThreadStatus::Failed
        ) && thread.queued_command_count() == 0
        {
            return;
        }
        if status.changed().await.is_err() {
            return;
        }
    }
}

pub(super) fn latest_message_text(
    messages: &[crate::kernel::history::CanonicalMessage],
) -> Option<String> {
    messages.iter().rev().find_map(|message| {
        let text = match message {
            crate::kernel::history::CanonicalMessage::Assistant { content, .. }
            | crate::kernel::history::CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .filter_map(|content| match content {
                    crate::kernel::history::CanonicalContent::Text { text, .. } => {
                        Some(text.as_str())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(""),
            crate::kernel::history::CanonicalMessage::User { .. } => String::new(),
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
    thread: &crate::kernel::runtime_host::RuntimeThreadHandle,
    interaction_id: verlet_runtime_contracts::RuntimeEventId,
    kind: verlet_runtime_contracts::ThreadInteractionKind,
    source_thread_id: verlet_runtime_contracts::ThreadId,
    target_thread_id: verlet_runtime_contracts::ThreadId,
    source_turn_id: Option<String>,
    target_turn_id: Option<String>,
    result_preview: Option<String>,
    metadata: std::collections::BTreeMap<String, String>,
) {
    thread.emit_runtime(
        crate::kernel::runtime_host::RuntimeEventKind::ThreadInteraction {
            interaction_id,
            kind,
            source_thread_id,
            target_thread_id,
            source_turn_id,
            target_turn_id,
            result_preview,
            metadata,
        },
    );
}
