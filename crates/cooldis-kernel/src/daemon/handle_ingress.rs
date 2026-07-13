//! Thread-handle settlement ingress.
//!
//! A spawned child settles its parent through the ordinary durable ingress
//! lane. The spawn request and spawned receipt durably bind the dispatch to
//! the parent control stream; the child's `thread.joined` record supplies the
//! terminal state. This adapter folds those records into a
//! `HandleTerminalEnvelope`, submits it to the ingress queue, and leaves
//! dedupe, admission, claim, settlement, and parent-turn execution to ADR
//! 0003's existing protocol.

use crate::kernel::thread_spawn_projector::fold_thread_handle_bindings;
use crate::{
    CanonicalContent, CanonicalMessage, CooldisError, CooldisResult, EventKind, EventRecord,
    EventStore, SessionContext, SessionEntryKind, SessionStore, SqliteSessionStore,
    ThreadCoordinates, ThreadJoinedPayload, ThreadTerminalState,
};
use cooldis_io_core::{
    ConversationKind, IngressContent, IngressEnvelope, IngressSink, IoConversation, IoDedupeKey,
    IoSource,
};
use cooldis_runtime_contracts::{
    HANDLE_OUTCOME_CONTENT_KIND, HandleId, HandleTerminalEnvelope, HandleTerminalOutcome,
    RuntimeTerminalState, RuntimeUsage,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Durable ingress source for terminal outcomes of thread handles.
///
/// Recovery is scan-based: no live subscription or durable cursor is part of
/// the contract. An instance suppresses dispatches that its sink already
/// acknowledged; a fresh process re-observes every terminal record, which is
/// safe because the emitted envelope uses the dispatch identity as its ingress
/// dedupe key.
///
/// The crash/cancellation boundaries are explicit:
///
/// - cancellation while discovering control streams, reading a stream, or
///   assembling child context writes nothing, so recovery re-folds the same
///   durable `thread.joined` record;
/// - cancellation before queue commit likewise writes nothing;
/// - cancellation after queue commit but before the submit acknowledgement
///   may cause another envelope id to be attempted, but the stable
///   `(cooldis.handle.outcome/1, dispatch_id)` dedupe key admits only one;
/// - after lease, the ordinary ADR 0003 worker retains or re-leases the queue
///   item until its durable ingress claim settles, and only then completes the
///   queue item.
pub(crate) struct ThreadHandleIngressAdapter {
    store: SqliteSessionStore,
    sink: Arc<dyn IngressSink>,
    tenant_id: String,
    user_id: String,
    poll_interval: Duration,
    acknowledged_dispatches: Mutex<BTreeSet<String>>,
}

impl ThreadHandleIngressAdapter {
    pub(crate) fn new(
        store: SqliteSessionStore,
        sink: Arc<dyn IngressSink>,
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Self {
        Self {
            store,
            sink,
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            poll_interval: Duration::from_millis(250),
            acknowledged_dispatches: Mutex::new(BTreeSet::new()),
        }
    }

    /// Scans durable spawn/terminal records once and submits each ready,
    /// previously unacknowledged settlement. A poisoned stream or settlement
    /// is diagnosed and retried on the next pass without blocking its peers.
    pub(crate) async fn enqueue_ready_once(&self) -> CooldisResult<usize> {
        let mut ready = Vec::new();
        for consumer in self
            .store
            .list_control_stream_coordinates()
            .await
            .map_err(history_error)?
        {
            if consumer.tenant_id != self.tenant_id || consumer.user_id != self.user_id {
                continue;
            }
            let stream_id = crate::control_stream_id(&consumer);
            let events = match self
                .store
                .read_events(&stream_id, None)
                .await
                .map_err(history_error)
            {
                Ok(events) => events,
                Err(err) => {
                    eprintln!(
                        "cooldis thread handle ingress skipped control stream {stream_id}: {err}"
                    );
                    continue;
                }
            };
            match fold_terminal_settlements(&consumer, &events) {
                Ok(settlements) => ready.extend(settlements),
                Err(err) => {
                    eprintln!(
                        "cooldis thread handle ingress skipped control stream {stream_id}: {err}"
                    );
                }
            }
        }

        let mut enqueued = 0;
        for settlement in ready {
            let dispatch_id = settlement.dispatch_id.to_string();
            if self
                .acknowledged_dispatches
                .lock()
                .await
                .contains(&dispatch_id)
            {
                continue;
            }
            let child_coordinates = ThreadCoordinates {
                tenant_id: settlement.consumer.tenant_id.clone(),
                user_id: settlement.consumer.user_id.clone(),
                session_id: settlement.consumer.session_id.clone(),
                thread_id: settlement.child_thread_id,
            };
            let context = match self
                .store
                .build_context(&child_coordinates)
                .await
                .map_err(history_error)
            {
                Ok(context) => context,
                Err(err) => {
                    log_settlement_skip(&settlement, "context assembly", &err);
                    continue;
                }
            };
            let envelope = match settlement.ingress_envelope(&context) {
                Ok(envelope) => envelope,
                Err(err) => {
                    log_settlement_skip(&settlement, "envelope assembly", &err);
                    continue;
                }
            };
            let ack = match self.sink.submit(envelope).await.map_err(io_error) {
                Ok(ack) => ack,
                Err(err) => {
                    log_settlement_skip(&settlement, "queue submit", &err);
                    continue;
                }
            };
            if ack.accepted {
                enqueued += 1;
            }
            if ack.accepted || ack.reason.as_deref() == Some("duplicate dedupe key") {
                self.acknowledged_dispatches
                    .lock()
                    .await
                    .insert(dispatch_id);
            } else {
                eprintln!(
                    "cooldis thread handle ingress sink rejected dispatch {} child {} consumer {}: {}",
                    settlement.dispatch_id,
                    settlement.child_thread_id,
                    crate::control_stream_id(&settlement.consumer),
                    ack.reason.as_deref().unwrap_or("unspecified rejection"),
                );
            }
        }
        Ok(enqueued)
    }

    pub(crate) async fn run(self) {
        loop {
            if let Err(err) = self.enqueue_ready_once().await {
                eprintln!("cooldis thread handle ingress adapter failed: {err}");
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

fn log_settlement_skip(
    settlement: &ThreadTerminalSettlement,
    stage: &str,
    err: &impl std::fmt::Display,
) {
    eprintln!(
        "cooldis thread handle ingress skipped dispatch {} child {} consumer {} during {stage}: {err}",
        settlement.dispatch_id,
        settlement.child_thread_id,
        crate::control_stream_id(&settlement.consumer),
    );
}

#[derive(Clone, Debug, PartialEq)]
struct ThreadTerminalSettlement {
    consumer: ThreadCoordinates,
    dispatch_id: cooldis_runtime_contracts::DispatchId,
    child_thread_id: cooldis_runtime_contracts::ThreadId,
    terminal_state: ThreadTerminalState,
    result_digest: Option<String>,
}

impl ThreadTerminalSettlement {
    fn ingress_envelope(&self, context: &SessionContext) -> CooldisResult<IngressEnvelope> {
        let (runtime_state, outcome_reason, retryable) =
            terminal_projection(self.terminal_state, self.result_digest.as_deref());
        let terminal = HandleTerminalEnvelope {
            dispatch_id: self.dispatch_id.clone(),
            handle: HandleId::thread(self.child_thread_id),
            outcome: HandleTerminalOutcome::from(runtime_state),
            outcome_reason,
            result: latest_assistant_result(context, self.child_thread_id),
            result_schema_id: None,
            artifact_refs: Vec::new(),
            usage: Some(total_thread_usage(context, self.child_thread_id)),
            retryable,
        };
        let source = IoSource::new("cooldis.handle", "thread");
        Ok(IngressEnvelope::new(
            source,
            IoConversation::new(
                format!("thread:{}", self.consumer.thread_id),
                ConversationKind::System,
            ),
            IngressContent::Event {
                kind: HANDLE_OUTCOME_CONTENT_KIND.to_string(),
                payload: serde_json::to_value(terminal).map_err(|err| {
                    CooldisError::RuntimeExecution(format!(
                        "handle terminal envelope encode failed: {err}"
                    ))
                })?,
            },
            now_ms(),
        )
        .with_dedupe_key(IoDedupeKey::new(
            HANDLE_OUTCOME_CONTENT_KIND,
            self.dispatch_id.to_string(),
        ))
        .with_metadata("cooldis_route_id", HANDLE_OUTCOME_CONTENT_KIND)
        .with_metadata("cooldis_route_policy", "queue_per_conversation"))
    }
}

fn fold_terminal_settlements(
    consumer: &ThreadCoordinates,
    events: &[EventRecord],
) -> CooldisResult<Vec<ThreadTerminalSettlement>> {
    let bindings = fold_thread_handle_bindings(events)?
        .into_iter()
        .map(|binding| (binding.spawned_event_id.to_string(), binding))
        .collect::<BTreeMap<_, _>>();
    let mut settlements = BTreeMap::<String, ThreadTerminalSettlement>::new();
    for joined in events
        .iter()
        .filter(|event| event.kind == EventKind::ThreadJoined)
    {
        let Some(spawned_event_id) = joined
            .payload
            .get("spawned_event_id")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let Some(binding) = bindings.get(spawned_event_id) else {
            continue;
        };
        let payload = serde_json::from_value::<ThreadJoinedPayload>(joined.payload.clone())
            .map_err(|err| {
                CooldisError::History(format!(
                    "thread handle terminal joined payload decode failed: {err}"
                ))
            })?;
        if binding.consumer != *consumer || payload.child_thread_id.to_string() != binding.handle.id
        {
            return Err(CooldisError::History(format!(
                "thread handle terminal join {} does not match its spawn binding",
                joined.id
            )));
        }
        let settlement = ThreadTerminalSettlement {
            consumer: binding.consumer.clone(),
            dispatch_id: binding.dispatch_id.clone(),
            child_thread_id: payload.child_thread_id,
            terminal_state: payload.terminal_state,
            result_digest: payload.result_digest,
        };
        if let Some(existing) = settlements.insert(spawned_event_id.to_string(), settlement.clone())
            && existing != settlement
        {
            return Err(CooldisError::History(format!(
                "thread handle {} has conflicting terminal settlements",
                settlement.dispatch_id
            )));
        }
    }
    Ok(settlements.into_values().collect())
}

fn terminal_projection(
    state: ThreadTerminalState,
    result_digest: Option<&str>,
) -> (RuntimeTerminalState, Option<String>, bool) {
    match state {
        ThreadTerminalState::Completed => (RuntimeTerminalState::Completed, None, false),
        ThreadTerminalState::Failed => (
            RuntimeTerminalState::Failed,
            Some(result_digest.unwrap_or("child thread failed").to_string()),
            true,
        ),
        ThreadTerminalState::Cancelled => (
            RuntimeTerminalState::Cancelled,
            Some(
                result_digest
                    .unwrap_or("child thread cancelled")
                    .to_string(),
            ),
            false,
        ),
        ThreadTerminalState::BudgetExhausted => (
            RuntimeTerminalState::Stopped,
            Some(
                result_digest
                    .unwrap_or("child thread budget exhausted")
                    .to_string(),
            ),
            true,
        ),
    }
}

fn latest_assistant_result(
    context: &SessionContext,
    child_thread_id: cooldis_runtime_contracts::ThreadId,
) -> Option<serde_json::Value> {
    context.entries.iter().rev().find_map(|entry| {
        if entry.coordinates.thread_id != child_thread_id {
            return None;
        }
        let (SessionEntryKind::Message {
            message: CanonicalMessage::Assistant { content, .. },
        }
        | SessionEntryKind::CustomContextMessage {
            message: CanonicalMessage::Assistant { content, .. },
        }) = &entry.kind
        else {
            return None;
        };
        let text = content
            .iter()
            .filter_map(|content| match content {
                CanonicalContent::Text { text, .. } => Some(text.as_str()),
                CanonicalContent::Image { .. }
                | CanonicalContent::Thinking { .. }
                | CanonicalContent::ToolCall { .. } => None,
            })
            .collect::<Vec<_>>()
            .join("");
        (!text.is_empty()).then(|| serde_json::Value::String(text))
    })
}

fn total_thread_usage(
    context: &SessionContext,
    child_thread_id: cooldis_runtime_contracts::ThreadId,
) -> RuntimeUsage {
    let mut total = RuntimeUsage {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    for entry in &context.entries {
        if entry.coordinates.thread_id != child_thread_id {
            continue;
        }
        let (SessionEntryKind::Message {
            message: CanonicalMessage::Assistant { usage, .. },
        }
        | SessionEntryKind::CustomContextMessage {
            message: CanonicalMessage::Assistant { usage, .. },
        }) = &entry.kind
        else {
            continue;
        };
        total.input_tokens = total.input_tokens.saturating_add(usage.input_tokens);
        total.output_tokens = total.output_tokens.saturating_add(usage.output_tokens);
        total.cache_creation_input_tokens = total
            .cache_creation_input_tokens
            .saturating_add(usage.cache_creation_input_tokens);
        total.cache_read_input_tokens = total
            .cache_read_input_tokens
            .saturating_add(usage.cache_read_input_tokens);
    }
    total
}

fn history_error(err: impl std::fmt::Display) -> CooldisError {
    CooldisError::History(err.to_string())
}

fn io_error(err: impl std::fmt::Display) -> CooldisError {
    CooldisError::RuntimeExecution(err.to_string())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use cooldis_runtime_contracts::DispatchId;

    #[test]
    fn rc5_spawned_and_oldest_joined_payloads_decode_as_a_settlement() {
        let consumer = ThreadCoordinates {
            tenant_id: "tenant".to_string(),
            user_id: "user".to_string(),
            session_id: "legacy-session".to_string(),
            thread_id: cooldis_runtime_contracts::ThreadId::parse_str(
                "018f0000-0000-7000-8000-000000000419",
            )
            .unwrap(),
        };
        let child_thread_id =
            cooldis_runtime_contracts::ThreadId::parse_str("018f0000-0000-7000-8000-000000000420")
                .unwrap();
        let request_id = crate::EventRecordId::from_uuid(
            uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000421").unwrap(),
        );
        let spawned_id = crate::EventRecordId::from_uuid(
            uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000422").unwrap(),
        );
        let joined_id = crate::EventRecordId::from_uuid(
            uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000423").unwrap(),
        );
        let stream_id = crate::control_stream_id(&consumer);
        let request_payload = serde_json::from_str(
            r#"{
                "schema":"cooldis.thread.spawn.requested/1",
                "parent_thread_id":"018f0000-0000-7000-8000-000000000419",
                "parent_turn_id":"legacy-parent-turn",
                "child_agent_ref":"unbound",
                "initial_submission":"legacy child work",
                "correlation_id":"legacy-dispatch",
                "block_parent":false
            }"#,
        )
        .unwrap();
        let spawned_payload = serde_json::from_str(
            r#"{
                "schema":"cooldis.thread.spawned/1",
                "parent_thread_id":"018f0000-0000-7000-8000-000000000419",
                "child_thread_id":"018f0000-0000-7000-8000-000000000420",
                "child_manifest_hash":"sha256:legacy-manifest",
                "granted":[],
                "inputs_hash":"sha256:legacy-inputs",
                "correlation_id":"legacy-dispatch"
            }"#,
        )
        .unwrap();
        let joined_payload = serde_json::from_str(
            r#"{
                "schema":"cooldis.thread.joined/1",
                "child_thread_id":"018f0000-0000-7000-8000-000000000420",
                "spawned_event_id":"018f0000-0000-7000-8000-000000000422",
                "terminal_state":"completed"
            }"#,
        )
        .unwrap();
        let events = vec![
            EventRecord {
                id: request_id,
                stream_id: stream_id.clone(),
                sequence: crate::EventSequence::new(1),
                coordinates: consumer.clone(),
                created_at_ms: 1,
                kind: EventKind::ThreadSpawnRequested,
                origin: crate::EventOrigin::Discharged,
                provenance: crate::EventProvenance::default(),
                payload: request_payload,
            },
            EventRecord {
                id: spawned_id,
                stream_id: stream_id.clone(),
                sequence: crate::EventSequence::new(2),
                coordinates: consumer.clone(),
                created_at_ms: 2,
                kind: EventKind::ThreadSpawned,
                origin: crate::EventOrigin::Discharged,
                provenance: crate::EventProvenance {
                    source_event_ids: vec![request_id],
                    ..crate::EventProvenance::default()
                },
                payload: spawned_payload,
            },
            EventRecord {
                id: joined_id,
                stream_id,
                sequence: crate::EventSequence::new(3),
                coordinates: consumer.clone(),
                created_at_ms: 3,
                kind: EventKind::ThreadJoined,
                origin: crate::EventOrigin::Discharged,
                provenance: crate::EventProvenance::default(),
                payload: joined_payload,
            },
        ];

        let settlements = fold_terminal_settlements(&consumer, &events).unwrap();

        assert_eq!(settlements.len(), 1);
        assert_eq!(
            settlements[0].dispatch_id,
            DispatchId::new("legacy-dispatch")
        );
        assert_eq!(settlements[0].child_thread_id, child_thread_id);
        assert_eq!(
            settlements[0].terminal_state,
            ThreadTerminalState::Completed
        );
        assert_eq!(settlements[0].result_digest, None);

        let mut repeated_spawn = events;
        repeated_spawn.push(repeated_spawn[1].clone());
        let bindings = fold_thread_handle_bindings(&repeated_spawn).unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].dispatch_id, DispatchId::new("legacy-dispatch"));
        assert_eq!(bindings[0].consumer, consumer);
        assert_eq!(bindings[0].handle, HandleId::thread(child_thread_id));
    }

    #[test]
    fn failure_and_cancellation_outcomes_keep_reason_detail() {
        let (failed, failed_reason, failed_retryable) =
            terminal_projection(ThreadTerminalState::Failed, None);
        assert_eq!(
            HandleTerminalOutcome::from(failed),
            HandleTerminalOutcome::Failed
        );
        assert_eq!(failed_reason.as_deref(), Some("child thread failed"));
        assert!(failed_retryable);

        let (cancelled, cancelled_reason, cancelled_retryable) =
            terminal_projection(ThreadTerminalState::Cancelled, Some("cancel requested"));
        assert_eq!(
            HandleTerminalOutcome::from(cancelled),
            HandleTerminalOutcome::Cancelled
        );
        assert_eq!(cancelled_reason.as_deref(), Some("cancel requested"));
        assert!(!cancelled_retryable);
    }
}
