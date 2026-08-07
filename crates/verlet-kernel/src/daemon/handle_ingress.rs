//! Thread-handle settlement ingress.
//!
//! A spawned child settles its parent through the ordinary durable ingress
//! lane. The spawn request and spawned receipt durably bind the dispatch to
//! the parent control stream; the child's `thread.joined` record supplies the
//! terminal state. This adapter folds those records into a
//! `HandleTerminalEnvelope`, submits it to the ingress queue, and leaves
//! dedupe, admission, claim, settlement, and parent-turn execution to ADR
//! 0003's existing protocol.
//!
//! Processes use the symmetric contract without this central scanner. Their
//! manager registry is owned by the surface that started the backend, so that
//! owning surface witnesses `cooldis.handle.dispatch/1` before start, observes
//! the manager's terminal snapshot, and submits `cooldis.handle.outcome/1`
//! directly. It retains the terminal manager entry until ingress settlement is
//! acknowledged. A daemon restart cannot reattach an orphaned host process;
//! EMO-426's startup recovery sweep re-observes every parent-control stream.
//! It fails a process dispatch witness with no outcome retryably through the
//! same outcome envelope lane. On the thread side it appends a missing
//! first-wins `thread.joined` only for a spawned child whose dispatch was
//! claimed by the dead generation: durable child terminal truth wins, while
//! an inconclusive dead claim fails retryably and an unclaimed queued request
//! is left for normal execution. This adapter then settles the recovered join
//! without any recovery-specific behavior.

use crate::EventStore as _;
use crate::SessionStore as _;

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
    store: crate::SqliteSessionStore,
    sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
    tenant_id: String,
    user_id: String,
    poll_interval: std::time::Duration,
    acknowledged_dispatches: tokio::sync::Mutex<std::collections::BTreeSet<String>>,
}

impl ThreadHandleIngressAdapter {
    pub(crate) fn new(
        store: crate::SqliteSessionStore,
        sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Self {
        Self {
            store,
            sink,
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            poll_interval: std::time::Duration::from_millis(250),
            acknowledged_dispatches: tokio::sync::Mutex::new(std::collections::BTreeSet::new()),
        }
    }

    /// Scans durable spawn/terminal records once and submits each ready,
    /// previously unacknowledged settlement. A poisoned stream or settlement
    /// is diagnosed and retried on the next pass without blocking its peers.
    pub(crate) async fn enqueue_ready_once(&self) -> crate::VerletResult<usize> {
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
                        "verlet thread handle ingress skipped control stream {stream_id}: {err}"
                    );
                    continue;
                }
            };
            match fold_terminal_settlements(&consumer, &events) {
                Ok(settlements) => ready.extend(settlements),
                Err(err) => {
                    eprintln!(
                        "verlet thread handle ingress skipped control stream {stream_id}: {err}"
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
            let child_coordinates = crate::ThreadCoordinates {
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
                    "verlet thread handle ingress sink rejected dispatch {} child {} consumer {}: {}",
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
                eprintln!("verlet thread handle ingress adapter failed: {err}");
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
        "verlet thread handle ingress skipped dispatch {} child {} consumer {} during {stage}: {err}",
        settlement.dispatch_id,
        settlement.child_thread_id,
        crate::control_stream_id(&settlement.consumer),
    );
}

#[derive(Clone, Debug, PartialEq)]
struct ThreadTerminalSettlement {
    consumer: crate::ThreadCoordinates,
    dispatch_id: verlet_runtime_contracts::DispatchId,
    child_thread_id: verlet_runtime_contracts::ThreadId,
    terminal_state: crate::ThreadTerminalState,
    result_digest: Option<String>,
}

impl ThreadTerminalSettlement {
    fn ingress_envelope(
        &self,
        context: &crate::SessionContext,
    ) -> crate::VerletResult<verlet_io_core::IngressEnvelope> {
        let (runtime_state, outcome_reason, retryable) =
            terminal_projection(self.terminal_state, self.result_digest.as_deref());
        let terminal = verlet_runtime_contracts::HandleTerminalEnvelope {
            dispatch_id: self.dispatch_id.clone(),
            handle: verlet_runtime_contracts::HandleId::thread(self.child_thread_id),
            outcome: verlet_runtime_contracts::HandleTerminalOutcome::from(runtime_state),
            outcome_reason,
            result: latest_assistant_result(context, self.child_thread_id),
            result_schema_id: None,
            artifact_refs: Vec::new(),
            usage: Some(total_thread_usage(context, self.child_thread_id)),
            retryable,
        };
        let source = verlet_io_core::IoSource::new("cooldis.handle", "thread");
        Ok(verlet_io_core::IngressEnvelope::new(
            source,
            verlet_io_core::IoConversation::new(
                format!("thread:{}", self.consumer.thread_id),
                verlet_io_core::ConversationKind::System,
            ),
            verlet_io_core::IngressContent::Event {
                kind: verlet_runtime_contracts::HANDLE_OUTCOME_CONTENT_KIND.to_string(),
                payload: serde_json::to_value(terminal).map_err(|err| {
                    crate::VerletError::RuntimeExecution(format!(
                        "handle terminal envelope encode failed: {err}"
                    ))
                })?,
            },
            now_ms(),
        )
        .with_dedupe_key(verlet_io_core::IoDedupeKey::new(
            verlet_runtime_contracts::HANDLE_OUTCOME_CONTENT_KIND,
            self.dispatch_id.to_string(),
        ))
        .with_delivery(verlet_io_core::IoDelivery::new(
            self.dispatch_id.to_string(),
        ))
        .with_principal(verlet_io_core::IoPrincipal::new(
            self.consumer.tenant_id.clone(),
            self.consumer.user_id.clone(),
            format!("handle:{}", self.dispatch_id),
        ))
        .with_metadata(
            "cooldis_route_id",
            verlet_runtime_contracts::HANDLE_OUTCOME_CONTENT_KIND,
        )
        .with_metadata("cooldis_route_policy", "queue_per_conversation"))
    }
}

fn fold_terminal_settlements(
    consumer: &crate::ThreadCoordinates,
    events: &[crate::EventRecord],
) -> crate::VerletResult<Vec<ThreadTerminalSettlement>> {
    let bindings = crate::kernel::thread_spawn_projector::fold_thread_handle_bindings(events)?
        .into_iter()
        .map(|binding| (binding.spawned_event_id.to_string(), binding))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut settlements = std::collections::BTreeMap::<String, ThreadTerminalSettlement>::new();
    for joined in events
        .iter()
        .filter(|event| event.kind == crate::EventKind::ThreadJoined)
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
        let payload = serde_json::from_value::<crate::ThreadJoinedPayload>(joined.payload.clone())
            .map_err(|err| {
                crate::VerletError::History(format!(
                    "thread handle terminal joined payload decode failed: {err}"
                ))
            })?;
        if binding.consumer != *consumer || payload.child_thread_id.to_string() != binding.handle.id
        {
            return Err(crate::VerletError::History(format!(
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
            return Err(crate::VerletError::History(format!(
                "thread handle {} has conflicting terminal settlements",
                settlement.dispatch_id
            )));
        }
    }
    Ok(settlements.into_values().collect())
}

fn terminal_projection(
    state: crate::ThreadTerminalState,
    result_digest: Option<&str>,
) -> (
    verlet_runtime_contracts::RuntimeTerminalState,
    Option<String>,
    bool,
) {
    match state {
        crate::ThreadTerminalState::Completed => (
            verlet_runtime_contracts::RuntimeTerminalState::Completed,
            None,
            false,
        ),
        crate::ThreadTerminalState::Failed => (
            verlet_runtime_contracts::RuntimeTerminalState::Failed,
            Some(result_digest.unwrap_or("child thread failed").to_string()),
            true,
        ),
        crate::ThreadTerminalState::Cancelled => (
            verlet_runtime_contracts::RuntimeTerminalState::Cancelled,
            Some(
                result_digest
                    .unwrap_or("child thread cancelled")
                    .to_string(),
            ),
            false,
        ),
        crate::ThreadTerminalState::BudgetExhausted => (
            verlet_runtime_contracts::RuntimeTerminalState::Stopped,
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
    context: &crate::SessionContext,
    child_thread_id: verlet_runtime_contracts::ThreadId,
) -> Option<serde_json::Value> {
    context.entries.iter().rev().find_map(|entry| {
        if entry.coordinates.thread_id != child_thread_id {
            return None;
        }
        let (crate::SessionEntryKind::Message {
            message: crate::CanonicalMessage::Assistant { content, .. },
        }
        | crate::SessionEntryKind::CustomContextMessage {
            message: crate::CanonicalMessage::Assistant { content, .. },
        }) = &entry.kind
        else {
            return None;
        };
        let text = content
            .iter()
            .filter_map(|content| match content {
                crate::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                crate::CanonicalContent::Image { .. }
                | crate::CanonicalContent::Thinking { .. }
                | crate::CanonicalContent::ToolCall { .. } => None,
            })
            .collect::<Vec<_>>()
            .join("");
        (!text.is_empty()).then(|| serde_json::Value::String(text))
    })
}

fn total_thread_usage(
    context: &crate::SessionContext,
    child_thread_id: verlet_runtime_contracts::ThreadId,
) -> verlet_runtime_contracts::RuntimeUsage {
    let mut total = verlet_runtime_contracts::RuntimeUsage {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    for entry in &context.entries {
        if entry.coordinates.thread_id != child_thread_id {
            continue;
        }
        let (crate::SessionEntryKind::Message {
            message: crate::CanonicalMessage::Assistant { usage, .. },
        }
        | crate::SessionEntryKind::CustomContextMessage {
            message: crate::CanonicalMessage::Assistant { usage, .. },
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

fn history_error(err: impl std::fmt::Display) -> crate::VerletError {
    crate::VerletError::History(err.to_string())
}

fn io_error(err: impl std::fmt::Display) -> crate::VerletError {
    crate::VerletError::RuntimeExecution(err.to_string())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {

    #[test]
    fn rc5_spawned_and_oldest_joined_payloads_decode_as_a_settlement() {
        let consumer = crate::ThreadCoordinates {
            tenant_id: "tenant".to_string(),
            user_id: "user".to_string(),
            session_id: "legacy-session".to_string(),
            thread_id: verlet_runtime_contracts::ThreadId::parse_str(
                "018f0000-0000-7000-8000-000000000419",
            )
            .unwrap(),
        };
        let child_thread_id =
            verlet_runtime_contracts::ThreadId::parse_str("018f0000-0000-7000-8000-000000000420")
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
            crate::EventRecord {
                id: request_id,
                stream_id: stream_id.clone(),
                sequence: crate::EventSequence::new(1),
                coordinates: consumer.clone(),
                created_at_ms: 1,
                kind: crate::EventKind::ThreadSpawnRequested,
                origin: crate::EventOrigin::Discharged,
                provenance: crate::EventProvenance::default(),
                payload: request_payload,
            },
            crate::EventRecord {
                id: spawned_id,
                stream_id: stream_id.clone(),
                sequence: crate::EventSequence::new(2),
                coordinates: consumer.clone(),
                created_at_ms: 2,
                kind: crate::EventKind::ThreadSpawned,
                origin: crate::EventOrigin::Discharged,
                provenance: crate::EventProvenance {
                    source_event_ids: vec![request_id],
                    ..crate::EventProvenance::default()
                },
                payload: spawned_payload,
            },
            crate::EventRecord {
                id: joined_id,
                stream_id,
                sequence: crate::EventSequence::new(3),
                coordinates: consumer.clone(),
                created_at_ms: 3,
                kind: crate::EventKind::ThreadJoined,
                origin: crate::EventOrigin::Discharged,
                provenance: crate::EventProvenance::default(),
                payload: joined_payload,
            },
        ];

        let settlements =
            crate::daemon::handle_ingress::fold_terminal_settlements(&consumer, &events).unwrap();

        assert_eq!(settlements.len(), 1);
        assert_eq!(
            settlements[0].dispatch_id,
            verlet_runtime_contracts::DispatchId::new("legacy-dispatch")
        );
        assert_eq!(settlements[0].child_thread_id, child_thread_id);
        assert_eq!(
            settlements[0].terminal_state,
            crate::ThreadTerminalState::Completed
        );
        assert_eq!(settlements[0].result_digest, None);

        let mut repeated_spawn = events;
        repeated_spawn.push(repeated_spawn[1].clone());
        let bindings =
            crate::kernel::thread_spawn_projector::fold_thread_handle_bindings(&repeated_spawn)
                .unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(
            bindings[0].dispatch_id,
            verlet_runtime_contracts::DispatchId::new("legacy-dispatch")
        );
        assert_eq!(bindings[0].consumer, consumer);
        assert_eq!(
            bindings[0].handle,
            verlet_runtime_contracts::HandleId::thread(child_thread_id)
        );
    }

    #[test]
    fn failure_and_cancellation_outcomes_keep_reason_detail() {
        let (failed, failed_reason, failed_retryable) =
            crate::daemon::handle_ingress::terminal_projection(
                crate::ThreadTerminalState::Failed,
                None,
            );
        assert_eq!(
            verlet_runtime_contracts::HandleTerminalOutcome::from(failed),
            verlet_runtime_contracts::HandleTerminalOutcome::Failed
        );
        assert_eq!(failed_reason.as_deref(), Some("child thread failed"));
        assert!(failed_retryable);

        let (cancelled, cancelled_reason, cancelled_retryable) =
            crate::daemon::handle_ingress::terminal_projection(
                crate::ThreadTerminalState::Cancelled,
                Some("cancel requested"),
            );
        assert_eq!(
            verlet_runtime_contracts::HandleTerminalOutcome::from(cancelled),
            verlet_runtime_contracts::HandleTerminalOutcome::Cancelled
        );
        assert_eq!(cancelled_reason.as_deref(), Some("cancel requested"));
        assert!(!cancelled_retryable);
    }
}
