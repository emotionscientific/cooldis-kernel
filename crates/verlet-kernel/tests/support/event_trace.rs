const EVENT_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(30);

#[derive(Debug, Default)]
pub struct EventTrace {
    pub runtime_events: Vec<verlet::kernel::runtime_host::runtime_events::RuntimeEventKind>,
    pub mirrors: Vec<verlet_history::SessionEntry>,
    pub outputs: Vec<String>,
    pub failures: Vec<String>,
    pub cancellations: Vec<String>,
    pub signals: Vec<verlet_runtime_contracts::ThreadSignal>,
}

impl EventTrace {
    pub fn runtime_events(
        &self,
    ) -> &[verlet::kernel::runtime_host::runtime_events::RuntimeEventKind] {
        &self.runtime_events
    }

    pub fn text_messages(&self) -> Vec<String> {
        self.mirrors
            .iter()
            .filter_map(|entry| match &entry.kind {
                verlet_history::SessionEntryKind::Message { message } => {
                    Some(text_from_message(message))
                }
                _ => None,
            })
            .collect()
    }
}

pub async fn collect_until_output(
    events: &mut tokio::sync::broadcast::Receiver<
        verlet::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        verlet::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
            (text == expected).then_some(())
        }
        verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
            trace.failures.push(message.clone());
            panic!("thread failed before output {expected:?}: {message}; trace: {trace:#?}");
        }
        _ => None,
    })
    .await
}

pub async fn collect_until_failed(
    events: &mut tokio::sync::broadcast::Receiver<
        verlet::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected_fragment: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
            assert!(
                message.contains(expected_fragment),
                "failure {message:?} did not contain {expected_fragment:?}; trace: {trace:#?}"
            );
            Some(())
        }
        _ => None,
    })
    .await
}

pub async fn collect_until_cancelled(
    events: &mut tokio::sync::broadcast::Receiver<
        verlet::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected_reason: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        verlet::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { reason, .. } => {
            assert_eq!(reason, expected_reason, "trace: {trace:#?}");
            Some(())
        }
        verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
            trace.failures.push(message.clone());
            panic!("thread failed before cancellation {expected_reason:?}: {message}");
        }
        _ => None,
    })
    .await
}

pub async fn collect_until_compaction(
    events: &mut tokio::sync::broadcast::Receiver<
        verlet::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    expected_summary: &str,
) -> EventTrace {
    collect_until(events, |event, _trace| match event {
        verlet::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
            match &event.kind {
                verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Compaction {
                    summary,
                    ..
                } if summary == expected_summary => Some(()),
                _ => None,
            }
        }
        verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
            panic!("thread failed before compaction {expected_summary:?}: {message}");
        }
        _ => None,
    })
    .await
}

async fn collect_until(
    events: &mut tokio::sync::broadcast::Receiver<
        verlet::kernel::runtime_host::runtime_api::ThreadEvent,
    >,
    mut done: impl FnMut(
        &verlet::kernel::runtime_host::runtime_api::ThreadEvent,
        &mut EventTrace,
    ) -> Option<()>,
) -> EventTrace {
    let mut trace = EventTrace::default();
    loop {
        let event = tokio::time::timeout(EVENT_TIMEOUT, events.recv())
            .await
            .unwrap_or_else(|_| panic!("event timed out; trace: {trace:#?}"))
            .expect("event channel closed");
        match &event {
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                trace.runtime_events.push(event.kind.clone())
            }
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror {
                entry,
                ..
            } => trace.mirrors.push(entry.clone()),
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
                trace.outputs.push(text.clone())
            }
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                trace.failures.push(message.clone())
            }
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                reason, ..
            } => trace.cancellations.push(reason.clone()),
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Signal { signal, .. } => {
                trace.signals.push(signal.clone())
            }
            _ => {}
        }
        if done(&event, &mut trace).is_some() {
            return trace;
        }
    }
}

pub fn find_event_index(
    events: &[verlet::kernel::runtime_host::runtime_events::RuntimeEventKind],
    label: &str,
    predicate: impl Fn(&verlet::kernel::runtime_host::runtime_events::RuntimeEventKind) -> bool,
) -> usize {
    events
        .iter()
        .position(predicate)
        .unwrap_or_else(|| panic!("missing runtime event {label}; events: {events:#?}"))
}

pub fn assert_event_order(
    events: &[verlet::kernel::runtime_host::runtime_events::RuntimeEventKind],
    first_label: &str,
    first: impl Fn(&verlet::kernel::runtime_host::runtime_events::RuntimeEventKind) -> bool,
    second_label: &str,
    second: impl Fn(&verlet::kernel::runtime_host::runtime_events::RuntimeEventKind) -> bool,
) {
    let first = find_event_index(events, first_label, first);
    let second = find_event_index(events, second_label, second);
    assert!(
        first < second,
        "expected {first_label} before {second_label}; events: {events:#?}"
    );
}

pub fn text_from_message(message: &verlet_history::CanonicalMessage) -> String {
    match message {
        verlet_history::CanonicalMessage::User { content, .. }
        | verlet_history::CanonicalMessage::Assistant { content, .. }
        | verlet_history::CanonicalMessage::ToolResult { content, .. } => {
            text_from_content(content)
        }
    }
}

pub fn text_from_content(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}
