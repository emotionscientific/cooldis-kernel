use super::kernel_test::{
    CanonicalContent, CanonicalMessage, RuntimeEventKind, SessionEntry, SessionEntryKind,
    ThreadEvent, ThreadSignal,
};
use tokio::sync::broadcast;
use tokio::time::{Duration, timeout};

const EVENT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Default)]
pub struct EventTrace {
    pub runtime_events: Vec<RuntimeEventKind>,
    pub mirrors: Vec<SessionEntry>,
    pub outputs: Vec<String>,
    pub failures: Vec<String>,
    pub cancellations: Vec<String>,
    pub signals: Vec<ThreadSignal>,
}

impl EventTrace {
    pub fn runtime_events(&self) -> &[RuntimeEventKind] {
        &self.runtime_events
    }

    pub fn text_messages(&self) -> Vec<String> {
        self.mirrors
            .iter()
            .filter_map(|entry| match &entry.kind {
                SessionEntryKind::Message { message } => Some(text_from_message(message)),
                _ => None,
            })
            .collect()
    }
}

pub async fn collect_until_output(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        ThreadEvent::Output { text, .. } => (text == expected).then_some(()),
        ThreadEvent::Failed { message, .. } => {
            trace.failures.push(message.clone());
            panic!("thread failed before output {expected:?}: {message}; trace: {trace:#?}");
        }
        _ => None,
    })
    .await
}

pub async fn collect_until_failed(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_fragment: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        ThreadEvent::Failed { message, .. } => {
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
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_reason: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        ThreadEvent::Cancelled { reason, .. } => {
            assert_eq!(reason, expected_reason, "trace: {trace:#?}");
            Some(())
        }
        ThreadEvent::Failed { message, .. } => {
            trace.failures.push(message.clone());
            panic!("thread failed before cancellation {expected_reason:?}: {message}");
        }
        _ => None,
    })
    .await
}

pub async fn collect_until_compaction(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_summary: &str,
) -> EventTrace {
    collect_until(events, |event, _trace| match event {
        ThreadEvent::Runtime { event, .. } => match &event.kind {
            RuntimeEventKind::Compaction { summary, .. } if summary == expected_summary => Some(()),
            _ => None,
        },
        ThreadEvent::Failed { message, .. } => {
            panic!("thread failed before compaction {expected_summary:?}: {message}");
        }
        _ => None,
    })
    .await
}

async fn collect_until(
    events: &mut broadcast::Receiver<ThreadEvent>,
    mut done: impl FnMut(&ThreadEvent, &mut EventTrace) -> Option<()>,
) -> EventTrace {
    let mut trace = EventTrace::default();
    loop {
        let event = timeout(EVENT_TIMEOUT, events.recv())
            .await
            .unwrap_or_else(|_| panic!("event timed out; trace: {trace:#?}"))
            .expect("event channel closed");
        match &event {
            ThreadEvent::Runtime { event, .. } => trace.runtime_events.push(event.kind.clone()),
            ThreadEvent::CanonicalMirror { entry, .. } => trace.mirrors.push(entry.clone()),
            ThreadEvent::Output { text, .. } => trace.outputs.push(text.clone()),
            ThreadEvent::Failed { message, .. } => trace.failures.push(message.clone()),
            ThreadEvent::Cancelled { reason, .. } => trace.cancellations.push(reason.clone()),
            ThreadEvent::Signal { signal, .. } => trace.signals.push(signal.clone()),
            _ => {}
        }
        if done(&event, &mut trace).is_some() {
            return trace;
        }
    }
}

pub fn find_event_index(
    events: &[RuntimeEventKind],
    label: &str,
    predicate: impl Fn(&RuntimeEventKind) -> bool,
) -> usize {
    events
        .iter()
        .position(predicate)
        .unwrap_or_else(|| panic!("missing runtime event {label}; events: {events:#?}"))
}

pub fn assert_event_order(
    events: &[RuntimeEventKind],
    first_label: &str,
    first: impl Fn(&RuntimeEventKind) -> bool,
    second_label: &str,
    second: impl Fn(&RuntimeEventKind) -> bool,
) {
    let first = find_event_index(events, first_label, first);
    let second = find_event_index(events, second_label, second);
    assert!(
        first < second,
        "expected {first_label} before {second_label}; events: {events:#?}"
    );
}

pub fn text_from_message(message: &CanonicalMessage) -> String {
    match message {
        CanonicalMessage::User { content, .. }
        | CanonicalMessage::Assistant { content, .. }
        | CanonicalMessage::ToolResult { content, .. } => text_from_content(content),
    }
}

pub fn text_from_content(content: &[CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}
