//! Daemon-startup recovery for orphaned thread and process handles.
//!
//! EMO-426 defines this as a run-once, store-first fold. It scans every
//! parent-control stream in the daemon's tenant/user scope, isolates corrupt
//! streams and individual delivery failures, and carries no durable cursor:
//! a crash during the sweep simply re-folds the same records on the next
//! startup.
//!
//! Thread recovery appends only the missing first `thread.joined`. Durable
//! child terminal truth wins; otherwise a claimed spawn whose runtime died is
//! failed retryably, while an unclaimed spawn request has no handle binding
//! and is left for its ordinary projector. Process recovery submits dispatch
//! witnesses without outcome witnesses through the standard outcome envelope
//! lane. Both paths are at-least-once and use their existing first-wins or
//! dedupe guards.

use crate::kernel::process_handle_dispatch::{ProcessHandleDispatcher, validate_binding};
use crate::kernel::runtime_host::append_thread_joined_first_wins;
use crate::kernel::thread_spawn_projector::{fold_thread_handle_bindings, is_spawn_request_claim};
use crate::{
    CooldisError, CooldisResult, EventKind, EventRecord, EventStore, EventStreamId,
    IoIngressReceivedPayload, SqliteSessionStore, ThreadCoordinates, ThreadJoinedPayload,
    ThreadTerminalState, control_stream_id,
};
use cooldis_io_core::IngressContent;
use cooldis_runtime_contracts::{
    HANDLE_DISPATCH_CONTENT_KIND, HANDLE_OUTCOME_CONTENT_KIND, HandleDispatchEnvelope, HandleKind,
    HandleTerminalEnvelope,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct StartupRecoveryReceipt {
    pub(crate) thread_joins: usize,
    pub(crate) process_outcomes: usize,
}

pub(crate) struct StartupRecoverySweep {
    store: SqliteSessionStore,
    process_dispatcher: ProcessHandleDispatcher,
    tenant_id: String,
    user_id: String,
}

impl StartupRecoverySweep {
    pub(crate) fn new(
        store: SqliteSessionStore,
        process_dispatcher: ProcessHandleDispatcher,
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Self {
        Self {
            store,
            process_dispatcher,
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
        }
    }

    pub(crate) async fn run_once(&self) -> CooldisResult<StartupRecoveryReceipt> {
        let mut receipt = StartupRecoveryReceipt::default();
        let coordinates = self
            .store
            .list_control_stream_coordinates()
            .await
            .map_err(history_error)?;
        for consumer in coordinates {
            if consumer.tenant_id != self.tenant_id || consumer.user_id != self.user_id {
                continue;
            }
            let stream_id = control_stream_id(&consumer);
            let events = match self.store.read_events(&stream_id, None).await {
                Ok(events) => events,
                Err(err) => {
                    eprintln!("cooldis startup recovery skipped control stream {stream_id}: {err}");
                    continue;
                }
            };
            receipt.thread_joins += self.recover_threads(&consumer, &events).await;
            receipt.process_outcomes += self.recover_processes(&consumer, &events).await;
        }
        Ok(receipt)
    }

    async fn recover_threads(&self, consumer: &ThreadCoordinates, events: &[EventRecord]) -> usize {
        let bindings = match fold_thread_handle_bindings(events) {
            Ok(bindings) => bindings,
            Err(err) => {
                eprintln!(
                    "cooldis startup recovery skipped thread lane on {}: {err}",
                    control_stream_id(consumer),
                );
                return 0;
            }
        };
        let joined_spawn_ids = match fold_joined_spawn_ids(events) {
            Ok(joined_spawn_ids) => joined_spawn_ids,
            Err(err) => {
                eprintln!(
                    "cooldis startup recovery skipped thread lane on {}: {err}",
                    control_stream_id(consumer),
                );
                return 0;
            }
        };
        let mut recovered = 0;
        for binding in bindings {
            if joined_spawn_ids.contains(&binding.spawned_event_id.to_string()) {
                continue;
            }
            let claim = events.iter().find(|event| {
                is_spawn_request_claim(event)
                    && event
                        .provenance
                        .source_event_ids
                        .contains(&binding.request_event_id)
                    && event
                        .payload
                        .get("correlation_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(binding.dispatch_id.as_str())
            });
            let Some(claim) = claim else {
                // A durable request that has not been claimed is queued work,
                // not a dead runtime. Current valid bindings always have the
                // claim; this guard also keeps legacy/corrupt partial records
                // from being failed speculatively.
                continue;
            };
            if binding.handle.kind != HandleKind::Thread {
                eprintln!(
                    "cooldis startup recovery skipped non-thread binding {} on {}",
                    binding.dispatch_id,
                    control_stream_id(consumer),
                );
                continue;
            }
            let child_thread_id = match crate::ThreadId::parse_str(&binding.handle.id) {
                Ok(thread_id) => thread_id,
                Err(err) => {
                    eprintln!(
                        "cooldis startup recovery skipped thread dispatch {} with invalid child id: {err}",
                        binding.dispatch_id,
                    );
                    continue;
                }
            };
            let child = ThreadCoordinates {
                tenant_id: consumer.tenant_id.clone(),
                user_id: consumer.user_id.clone(),
                session_id: consumer.session_id.clone(),
                thread_id: child_thread_id,
            };
            let thread_stream = EventStreamId::for_thread(&child);
            let mut child_events = match self.store.read_events(&thread_stream, None).await {
                Ok(events) => events,
                Err(err) => {
                    log_thread_skip(&binding.dispatch_id, &child, "child thread stream", &err);
                    continue;
                }
            };
            let child_control_stream = control_stream_id(&child);
            match self.store.read_events(&child_control_stream, None).await {
                Ok(mut events) => child_events.append(&mut events),
                Err(err) => {
                    log_thread_skip(&binding.dispatch_id, &child, "child control stream", &err);
                    continue;
                }
            }
            let terminal =
                match fold_child_terminal_truth(&child_events, &binding.submitted_turn_id) {
                    Ok(terminal) => terminal,
                    Err(err) => {
                        log_thread_skip(&binding.dispatch_id, &child, "terminal fold", &err);
                        continue;
                    }
                };
            let (terminal_state, result_reason, recovery_reason, source_event) = match terminal {
                Some(terminal) => (
                    terminal.state,
                    terminal.outcome_reason,
                    format!(
                        "startup recovery re-observed durable child terminal {} after daemon restart",
                        terminal.kind.as_str()
                    ),
                    Some((terminal.stream_id, terminal.event_id)),
                ),
                None => {
                    let reason =
                        "startup recovery observed runtime death before durable terminal state"
                            .to_string();
                    (
                        ThreadTerminalState::Failed,
                        Some(reason.clone()),
                        reason,
                        Some((claim.stream_id.clone(), claim.id)),
                    )
                }
            };
            match append_thread_joined_first_wins(
                &self.store,
                consumer.clone(),
                child.clone(),
                binding.spawned_event_id,
                terminal_state,
                result_reason,
                Some(recovery_reason),
                source_event,
                "recovery:startup-sweep",
                "thread_join_recovery/v1",
            )
            .await
            {
                Ok(joined) if joined.appended => recovered += 1,
                Ok(_) => {}
                Err(err) => {
                    log_thread_skip(&binding.dispatch_id, &child, "first-join append", &err)
                }
            }
        }
        recovered
    }

    async fn recover_processes(
        &self,
        consumer: &ThreadCoordinates,
        events: &[EventRecord],
    ) -> usize {
        let bindings = match fold_orphaned_process_dispatches(consumer, events) {
            Ok(bindings) => bindings,
            Err(err) => {
                eprintln!(
                    "cooldis startup recovery skipped process lane on {}: {err}",
                    control_stream_id(consumer),
                );
                return 0;
            }
        };
        let mut recovered = 0;
        for binding in bindings {
            if self
                .process_dispatcher
                .is_live_dispatch(&binding.dispatch_id)
                .await
            {
                continue;
            }
            match self
                .process_dispatcher
                .submit_recovery_outcome(&binding)
                .await
            {
                Ok(()) => recovered += 1,
                Err(err) => eprintln!(
                    "cooldis startup recovery skipped process dispatch {} on {}: {err}",
                    binding.dispatch_id,
                    control_stream_id(consumer),
                ),
            }
        }
        recovered
    }
}

fn fold_joined_spawn_ids(events: &[EventRecord]) -> CooldisResult<BTreeSet<String>> {
    events
        .iter()
        .filter(|event| event.kind == EventKind::ThreadJoined)
        .map(|event| {
            serde_json::from_value::<ThreadJoinedPayload>(event.payload.clone())
                .map(|payload| payload.spawned_event_id.to_string())
                .map_err(|err| {
                    CooldisError::History(format!(
                        "thread.joined {} payload is malformed: {err}",
                        event.id
                    ))
                })
        })
        .collect()
}

#[derive(Clone)]
pub(crate) struct ChildTerminalTruth {
    pub(crate) state: ThreadTerminalState,
    pub(crate) outcome_reason: Option<String>,
    pub(crate) kind: EventKind,
    pub(crate) stream_id: EventStreamId,
    pub(crate) event_id: crate::EventRecordId,
}

fn fold_child_terminal_truth(
    events: &[EventRecord],
    submitted_turn_id: &str,
) -> CooldisResult<Option<ChildTerminalTruth>> {
    let mut terminal: Option<ChildTerminalTruth> = None;
    for event in events.iter() {
        let Some(projected) = project_child_terminal_record(event, submitted_turn_id) else {
            continue;
        };
        if terminal
            .as_ref()
            .is_some_and(|existing| existing.state != projected.state)
        {
            return Err(CooldisError::History(format!(
                "child stream has conflicting terminal records ending at {}",
                event.id
            )));
        }
        if terminal.is_none() {
            terminal = Some(projected);
        }
    }
    Ok(terminal)
}

/// Project one durable child record through EMO-426's turn-scoped terminal
/// law. Live remote tails and startup recovery share this exact mapping so a
/// restart cannot reinterpret the same child fact differently.
pub(crate) fn project_child_terminal_record(
    event: &EventRecord,
    submitted_turn_id: &str,
) -> Option<ChildTerminalTruth> {
    if !terminal_record_names_turn(event, submitted_turn_id) {
        return None;
    }
    let (state, default_reason) = match event.kind {
        EventKind::TurnCompleted | EventKind::LoopCompleted => {
            (ThreadTerminalState::Completed, None)
        }
        EventKind::LoopBudgetExhausted => (
            ThreadTerminalState::BudgetExhausted,
            Some("child thread budget exhausted"),
        ),
        EventKind::LoopDenied | EventKind::LoopBlocked => {
            (ThreadTerminalState::Failed, Some("child thread failed"))
        }
        _ => return None,
    };
    Some(ChildTerminalTruth {
        state,
        outcome_reason: default_reason.map(ToString::to_string),
        kind: event.kind,
        stream_id: event.stream_id.clone(),
        event_id: event.id,
    })
}

/// A spawned handle settles the first turn named durably by its spawn request.
/// Terminal-looking facts from earlier or later turns on the same child are
/// unrelated to that handle. The production provider record (`turn_id`) and
/// supervisor completion projection (`child_turn_id`) make that relationship
/// explicit; terminal records without either identity are inconclusive.
fn terminal_record_names_turn(event: &EventRecord, submitted_turn_id: &str) -> bool {
    event
        .payload
        .get("turn_id")
        .or_else(|| event.payload.get("child_turn_id"))
        .or_else(|| event.payload.pointer("/child/child_turn_id"))
        .and_then(serde_json::Value::as_str)
        == Some(submitted_turn_id)
}

fn fold_orphaned_process_dispatches(
    consumer: &ThreadCoordinates,
    events: &[EventRecord],
) -> CooldisResult<Vec<HandleDispatchEnvelope>> {
    let mut dispatches = BTreeMap::<String, HandleDispatchEnvelope>::new();
    let mut outcomes = BTreeMap::<String, HandleTerminalEnvelope>::new();
    for event in events
        .iter()
        .filter(|event| event.kind == EventKind::IoIngressReceived)
    {
        let witness =
            match serde_json::from_value::<IoIngressReceivedPayload>(event.payload.clone()) {
                Ok(witness) => witness,
                Err(err) => {
                    log_process_item_skip(consumer, event, "ingress witness", &err);
                    continue;
                }
            };
        let Some(content) = witness.content else {
            // Pre-EMO-420 and ordinary ingress witnesses intentionally have
            // no fold content.
            continue;
        };
        let content = match serde_json::from_value::<IngressContent>(content) {
            Ok(content) => content,
            Err(err) => {
                log_process_item_skip(consumer, event, "ingress content", &err);
                continue;
            }
        };
        let IngressContent::Event { kind, payload } = content else {
            continue;
        };
        match kind.as_str() {
            HANDLE_DISPATCH_CONTENT_KIND => {
                let binding = match serde_json::from_value::<HandleDispatchEnvelope>(payload) {
                    Ok(binding) => binding,
                    Err(err) => {
                        log_process_item_skip(consumer, event, "process dispatch envelope", &err);
                        continue;
                    }
                };
                if let Err(err) = validate_binding(
                    &binding,
                    consumer,
                    &binding.dispatch_id,
                    &binding.command_digest,
                ) {
                    log_process_item_skip(consumer, event, "process dispatch binding", &err);
                    continue;
                }
                let key = binding.dispatch_id.to_string();
                if let Some(existing) = dispatches.insert(key.clone(), binding.clone())
                    && existing != binding
                {
                    return Err(CooldisError::History(format!(
                        "process dispatch {key} has conflicting witnesses"
                    )));
                }
            }
            HANDLE_OUTCOME_CONTENT_KIND => {
                let terminal = match serde_json::from_value::<HandleTerminalEnvelope>(payload) {
                    Ok(terminal) => terminal,
                    Err(err) => {
                        log_process_item_skip(consumer, event, "process outcome envelope", &err);
                        continue;
                    }
                };
                if terminal.handle.kind != HandleKind::Process {
                    continue;
                }
                let key = terminal.dispatch_id.to_string();
                if let Some(existing) = outcomes.insert(key.clone(), terminal.clone())
                    && existing != terminal
                {
                    return Err(CooldisError::History(format!(
                        "process dispatch {key} has conflicting outcome witnesses"
                    )));
                }
            }
            _ => {}
        }
    }
    for (dispatch_id, outcome) in &outcomes {
        if let Some(binding) = dispatches.get(dispatch_id)
            && binding.handle != outcome.handle
        {
            return Err(CooldisError::History(format!(
                "process outcome {dispatch_id} does not match its dispatch handle"
            )));
        }
    }
    Ok(dispatches
        .into_iter()
        .filter_map(|(dispatch_id, binding)| {
            (!outcomes.contains_key(&dispatch_id)).then_some(binding)
        })
        .collect())
}

fn log_thread_skip(
    dispatch_id: &cooldis_runtime_contracts::DispatchId,
    child: &ThreadCoordinates,
    stage: &str,
    err: &impl std::fmt::Display,
) {
    eprintln!(
        "cooldis startup recovery skipped thread dispatch {dispatch_id} child {} during {stage}: {err}",
        child.thread_id,
    );
}

fn log_process_item_skip(
    consumer: &ThreadCoordinates,
    event: &EventRecord,
    stage: &str,
    err: &impl std::fmt::Display,
) {
    eprintln!(
        "cooldis startup recovery skipped process witness {} on {} during {stage}: {err}",
        event.id,
        control_stream_id(consumer),
    );
}

fn history_error(err: impl std::fmt::Display) -> CooldisError {
    CooldisError::History(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::identity::{CooldisDaemonIdentityConfig, IdentityMode, PrincipalId};
    use crate::kernel::process_handle_dispatch::{command_digest, recovery_outcome_envelope};
    use crate::test_support::{CrashCutHost, CrashCutSeam, FaultingRuntimeStore, run_crash_cut};
    use crate::{
        AgentLoopConfig, AgentLoopFactory, CanonicalContent, CanonicalStopReason, CanonicalUsage,
        CooldisAppServer, CooldisAppServerConfig, EventKind, EventProvenance, EventStore,
        EventStreamId, IoIngressReceivedPayload, NewEventRecord, ProviderApi, ProviderClient,
        ProviderRequest, ProviderResponse, ProviderResult, RuntimeHost, RuntimeStore,
        ThreadCoordinates, ThreadId, ThreadJoinedPayload, ThreadSpawnRequestedPayload,
        ThreadSpawnedPayload, ThreadTerminalState, ThreadTopology, control_stream_id,
    };
    use cooldis_io_core::{IngressContent, IngressEnvelope, IoDedupeKey};
    use cooldis_process::{
        AsyncExecutionManager, AsyncProcessStartRequest, CooldisProcessBackend,
        CooldisProcessEventKind, CooldisProcessHandle, ExecutionDeadline, LiveProcessBackend,
        LiveProcessSpawn, LiveProcessStartRequest,
    };
    use cooldis_runtime_contracts::{
        DispatchId, HANDLE_DISPATCH_CONTENT_KIND, HANDLE_OUTCOME_CONTENT_KIND,
        HandleDispatchEnvelope, HandleId, HandleTerminalEnvelope, HandleTerminalOutcome,
    };
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Mutex, Notify};
    use tokio_util::sync::CancellationToken;

    struct RecoveryCutProvider;

    #[async_trait::async_trait]
    impl ProviderClient for RecoveryCutProvider {
        async fn complete(&self, _request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
            Ok(ProviderResponse {
                content: vec![CanonicalContent::text("terminal before join cut")],
                usage: CanonicalUsage {
                    input_tokens: 1,
                    output_tokens: 4,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                },
                stop_reason: CanonicalStopReason::EndTurn,
            })
        }
    }

    #[derive(Default)]
    struct RecordingProcessIngress {
        store: Mutex<Option<SqliteSessionStore>>,
        seen: Mutex<BTreeSet<String>>,
        envelopes: Mutex<Vec<IngressEnvelope>>,
    }

    impl RecordingProcessIngress {
        fn with_store(store: SqliteSessionStore) -> Self {
            Self {
                store: Mutex::new(Some(store)),
                ..Self::default()
            }
        }

        async fn outcomes(&self) -> Vec<HandleTerminalEnvelope> {
            self.envelopes
                .lock()
                .await
                .iter()
                .filter_map(|envelope| match &envelope.content {
                    IngressContent::Event { kind, payload }
                        if kind == HANDLE_OUTCOME_CONTENT_KIND =>
                    {
                        serde_json::from_value(payload.clone()).ok()
                    }
                    _ => None,
                })
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl crate::ProcessHandleIngressSink for RecordingProcessIngress {
        async fn submit_process_handle_envelope(
            &self,
            envelope: IngressEnvelope,
        ) -> CooldisResult<()> {
            let stable_key = envelope
                .dedupe_key
                .as_ref()
                .map(IoDedupeKey::stable_key)
                .unwrap_or_else(|| envelope.id.clone());
            if !self.seen.lock().await.insert(stable_key) {
                return Ok(());
            }
            let IngressContent::Event { kind, .. } = &envelope.content else {
                panic!("process handle ingress must use event content");
            };
            let store = self.store.lock().await.clone().unwrap();
            let consumer = envelope_consumer(&store, &envelope).await;
            store
                .append_events(
                    &control_stream_id(&consumer),
                    vec![NewEventRecord::witnessed(
                        consumer,
                        EventKind::IoIngressReceived,
                        serde_json::to_value(IoIngressReceivedPayload {
                            route_id: Some(kind.clone()),
                            dedupe_key: envelope.dedupe_key.as_ref().map(IoDedupeKey::stable_key),
                            external_conversation_id: Some(
                                envelope.conversation.external_conversation_id.clone(),
                            ),
                            external_actor_id: None,
                            external_message_id: None,
                            content: Some(serde_json::to_value(&envelope.content).unwrap()),
                            envelope_digest: "sha256:recovery-test".to_string(),
                        })
                        .unwrap(),
                    )],
                )
                .await
                .unwrap();
            self.envelopes.lock().await.push(envelope);
            Ok(())
        }
    }

    struct FailOnceProcessIngress {
        inner: Arc<RecordingProcessIngress>,
        dispatch_id: String,
        failed: Mutex<bool>,
    }

    #[async_trait::async_trait]
    impl crate::ProcessHandleIngressSink for FailOnceProcessIngress {
        async fn submit_process_handle_envelope(
            &self,
            envelope: IngressEnvelope,
        ) -> CooldisResult<()> {
            let dispatch_id = match &envelope.content {
                IngressContent::Event { payload, .. } => payload
                    .get("dispatch_id")
                    .and_then(serde_json::Value::as_str),
                _ => None,
            };
            let mut failed = self.failed.lock().await;
            if !*failed && dispatch_id == Some(self.dispatch_id.as_str()) {
                *failed = true;
                return Err(CooldisError::History(
                    "synthetic sweep interruption".to_string(),
                ));
            }
            drop(failed);
            self.inner.submit_process_handle_envelope(envelope).await
        }
    }

    async fn envelope_consumer(
        store: &SqliteSessionStore,
        envelope: &IngressEnvelope,
    ) -> ThreadCoordinates {
        let IngressContent::Event { payload, .. } = &envelope.content else {
            panic!("handle envelope must be event content");
        };
        if let Ok(dispatch) = serde_json::from_value::<HandleDispatchEnvelope>(payload.clone()) {
            return dispatch.consumer;
        }
        let terminal: HandleTerminalEnvelope = serde_json::from_value(payload.clone()).unwrap();
        for coordinates in store.list_control_stream_coordinates().await.unwrap() {
            let events = store
                .read_events(&control_stream_id(&coordinates), None)
                .await
                .unwrap();
            if events.iter().any(|event| {
                event.kind == EventKind::IoIngressReceived
                    && event
                        .payload
                        .pointer("/content/payload/dispatch_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(terminal.dispatch_id.as_str())
                    && event
                        .payload
                        .pointer("/content/kind")
                        .and_then(serde_json::Value::as_str)
                        == Some(HANDLE_DISPATCH_CONTENT_KIND)
            }) {
                return coordinates;
            }
        }
        panic!("terminal outcome has no durable dispatch binding")
    }

    fn test_root(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join("cooldis-recovery-sweep-tests")
            .join(format!("{name}-{}", uuid::Uuid::now_v7()))
    }

    fn coordinates(tenant: &str, user: &str, label: &str) -> ThreadCoordinates {
        ThreadCoordinates {
            tenant_id: tenant.to_string(),
            user_id: user.to_string(),
            session_id: format!("session-{label}"),
            thread_id: ThreadId::new(),
        }
    }

    async fn open_store(name: &str) -> SqliteSessionStore {
        let root = test_root(name);
        std::fs::create_dir_all(&root).unwrap();
        SqliteSessionStore::open(root.join("history.sqlite3"))
            .await
            .unwrap()
    }

    async fn seed_thread_binding(
        store: &SqliteSessionStore,
        parent: &ThreadCoordinates,
        child: &ThreadCoordinates,
        dispatch_id: &str,
    ) -> crate::EventRecordId {
        let request = NewEventRecord::discharged(
            parent.clone(),
            EventKind::ThreadSpawnRequested,
            serde_json::to_value(ThreadSpawnRequestedPayload {
                parent_thread_id: parent.thread_id,
                parent_turn_id: None,
                task_name: Some(format!("task-{dispatch_id}")),
                submitted_turn_id: Some(format!("thread-spawn-{dispatch_id}")),
                child_agent_ref: "unbound".to_string(),
                initial_submission: "recover me".to_string(),
                correlation_id: dispatch_id.to_string(),
                block_parent: false,
            })
            .unwrap(),
            EventProvenance {
                discharged_by: Some("dispatcher:thread-spawn".to_string()),
                function: Some("thread_spawn_dispatch/v1".to_string()),
                ..EventProvenance::default()
            },
        );
        let request_id = request.id;
        let claim = NewEventRecord::discharged(
            parent.clone(),
            EventKind::ThreadSpawnRequested,
            request.payload.clone(),
            EventProvenance {
                source_streams: vec![control_stream_id(parent)],
                source_event_ids: vec![request_id],
                discharged_by: Some("projector:thread-spawn".to_string()),
                function: Some("thread_spawn_projector/v1".to_string()),
                ..EventProvenance::default()
            },
        );
        let mut spawned_payload = serde_json::to_value(ThreadSpawnedPayload {
            parent_thread_id: parent.thread_id,
            parent_turn_id: None,
            child_thread_id: child.thread_id,
            child_manifest_hash: "sha256:child".to_string(),
            child_policy_hash: None,
            granted: Vec::new(),
            inputs_hash: "sha256:input".to_string(),
            fork: None,
        })
        .unwrap();
        spawned_payload["correlation_id"] = json!(dispatch_id);
        let mut spawned =
            NewEventRecord::witnessed(parent.clone(), EventKind::ThreadSpawned, spawned_payload);
        spawned.provenance = EventProvenance {
            source_streams: vec![control_stream_id(parent)],
            source_event_ids: vec![request_id],
            ..EventProvenance::default()
        };
        let spawned_id = spawned.id;
        store
            .append_events(&control_stream_id(parent), vec![request, claim, spawned])
            .await
            .unwrap();
        spawned_id
    }

    async fn seed_queued_thread_request(
        store: &SqliteSessionStore,
        parent: &ThreadCoordinates,
        dispatch_id: &str,
    ) {
        store
            .append_events(
                &control_stream_id(parent),
                vec![NewEventRecord::discharged(
                    parent.clone(),
                    EventKind::ThreadSpawnRequested,
                    serde_json::to_value(ThreadSpawnRequestedPayload {
                        parent_thread_id: parent.thread_id,
                        parent_turn_id: None,
                        task_name: Some("queued".to_string()),
                        submitted_turn_id: Some(format!("thread-spawn-{dispatch_id}")),
                        child_agent_ref: "unbound".to_string(),
                        initial_submission: "not claimed".to_string(),
                        correlation_id: dispatch_id.to_string(),
                        block_parent: false,
                    })
                    .unwrap(),
                    EventProvenance {
                        discharged_by: Some("dispatcher:thread-spawn".to_string()),
                        function: Some("thread_spawn_dispatch/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();
    }

    async fn seed_child_terminal(
        store: &SqliteSessionStore,
        child: &ThreadCoordinates,
        turn_id: &str,
        kind: EventKind,
        reason: Option<&str>,
    ) -> crate::EventRecordId {
        let mut payload = json!({"turn_id": turn_id});
        if let Some(reason) = reason {
            payload["reason"] = json!(reason);
        }
        store
            .append_events(
                &EventStreamId::for_thread(child),
                vec![NewEventRecord::discharged(
                    child.clone(),
                    kind,
                    payload,
                    EventProvenance {
                        discharged_by: Some("runtime:test-child".to_string()),
                        function: Some("turn_complete/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()[0]
            .id
    }

    async fn seed_process_dispatch(
        store: &SqliteSessionStore,
        consumer: &ThreadCoordinates,
        dispatch_id: &str,
    ) -> HandleDispatchEnvelope {
        let binding = HandleDispatchEnvelope {
            dispatch_id: DispatchId::new(dispatch_id),
            handle: HandleId::process(uuid::Uuid::now_v7().to_string()),
            consumer: consumer.clone(),
            command_digest: command_digest(dispatch_id.as_bytes()),
        };
        let content = IngressContent::Event {
            kind: HANDLE_DISPATCH_CONTENT_KIND.to_string(),
            payload: serde_json::to_value(&binding).unwrap(),
        };
        store
            .append_events(
                &control_stream_id(consumer),
                vec![NewEventRecord::witnessed(
                    consumer.clone(),
                    EventKind::IoIngressReceived,
                    serde_json::to_value(IoIngressReceivedPayload {
                        route_id: Some(HANDLE_DISPATCH_CONTENT_KIND.to_string()),
                        dedupe_key: Some(format!("{HANDLE_DISPATCH_CONTENT_KIND}:{dispatch_id}")),
                        external_conversation_id: Some(format!("thread:{}", consumer.thread_id)),
                        external_actor_id: None,
                        external_message_id: None,
                        content: Some(serde_json::to_value(content).unwrap()),
                        envelope_digest: "sha256:dispatch".to_string(),
                    })
                    .unwrap(),
                )],
            )
            .await
            .unwrap();
        binding
    }

    async fn joined_payloads(
        store: &SqliteSessionStore,
        parent: &ThreadCoordinates,
    ) -> Vec<(ThreadJoinedPayload, serde_json::Value)> {
        store
            .read_events(&control_stream_id(parent), None)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == EventKind::ThreadJoined)
            .map(|event| {
                (
                    serde_json::from_value(event.payload.clone()).unwrap(),
                    event.payload,
                )
            })
            .collect()
    }

    struct ThreadTerminalJoinCrashHost {
        store: SqliteSessionStore,
        fault_store: Option<Arc<FaultingRuntimeStore<SqliteSessionStore>>>,
        host: Option<RuntimeHost>,
        parent: ThreadCoordinates,
        child: ThreadCoordinates,
        recovery: Option<StartupRecoveryReceipt>,
    }

    struct ThreadTerminalJoinCrashState {
        store: SqliteSessionStore,
        parent: ThreadCoordinates,
        child: ThreadCoordinates,
    }

    impl ThreadTerminalJoinCrashHost {
        async fn build() -> Self {
            let store = open_store("thread-terminal-join-cut").await;
            // The request and projector claim are the first two fenced
            // appends. The third is the live runtime's thread.joined commit;
            // delaying it exposes the exact durable-terminal/pre-join cut.
            let fault_store = Arc::new(
                FaultingRuntimeStore::new(Arc::new(store.clone())).delay_nth(
                    "append_events_fenced",
                    3,
                    Duration::from_secs(60),
                ),
            );
            let mut config = AgentLoopConfig::new(
                ProviderApi::Other("recovery-cut".to_string()),
                "recovery-cut",
                "recovery-cut-model",
            );
            config.max_tokens = 32;
            let factory = Arc::new(AgentLoopFactory::new(config, Arc::new(RecoveryCutProvider)));
            let host = RuntimeHost::with_session_store(
                factory,
                fault_store.clone() as Arc<dyn RuntimeStore>,
            );
            let parent = coordinates("tenant", "user", "provider-cut-parent");
            let parent_handle = host
                .start_thread(parent.clone(), ThreadTopology::root())
                .await
                .unwrap();
            let dispatch = host
                .kernel_control()
                .dispatch_thread_spawn(
                    parent_handle.context(),
                    DispatchId::new("provider-terminal-join-cut"),
                    "cut-child".to_string(),
                    "finish before the join append".to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();
            let child = ThreadCoordinates {
                tenant_id: parent.tenant_id.clone(),
                user_id: parent.user_id.clone(),
                session_id: parent.session_id.clone(),
                thread_id: dispatch.thread_id,
            };
            Self {
                store,
                fault_store: Some(fault_store),
                host: Some(host),
                parent,
                child,
                recovery: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl CrashCutHost for ThreadTerminalJoinCrashHost {
        type StoreState = ThreadTerminalJoinCrashState;

        async fn run_to_cut(&mut self, seam: CrashCutSeam) {
            assert_eq!(seam, CrashCutSeam::ThreadTerminalJoinCommit);
            let fault_store = self.fault_store.as_ref().unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if fault_store.call_count("append_events_fenced") >= 3 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("agent loop did not reach the pre-join commit cut");
            let child_events = self
                .store
                .read_events(&EventStreamId::for_thread(&self.child), None)
                .await
                .unwrap();
            assert!(
                child_events
                    .iter()
                    .any(|event| event.kind == EventKind::TurnCompleted),
                "the cut must occur after durable child terminal truth"
            );
            assert!(
                joined_payloads(&self.store, &self.parent).await.is_empty(),
                "the cut must occur before the parent thread.joined commit"
            );
        }

        fn tear_down(mut self) -> Self::StoreState {
            self.host.take();
            self.fault_store.take();
            ThreadTerminalJoinCrashState {
                store: self.store,
                parent: self.parent,
                child: self.child,
            }
        }

        async fn rebuild(state: Self::StoreState) -> Self {
            Self {
                store: state.store,
                fault_store: None,
                host: None,
                parent: state.parent,
                child: state.child,
                recovery: None,
            }
        }

        async fn recover(&mut self) {
            let ingress = Arc::new(RecordingProcessIngress::with_store(self.store.clone()));
            let dispatcher = ProcessHandleDispatcher::new(
                Arc::new(self.store.clone()) as Arc<dyn RuntimeStore>,
                ingress,
            );
            self.recovery = Some(
                StartupRecoverySweep::new(
                    self.store.clone(),
                    dispatcher,
                    &self.parent.tenant_id,
                    &self.parent.user_id,
                )
                .run_once()
                .await
                .unwrap(),
            );
        }
    }

    #[tokio::test]
    async fn provider_terminal_before_join_commit_converges_after_crash_cut_restart() {
        let rebuilt = run_crash_cut(
            "thread-terminal-join-commit",
            ThreadTerminalJoinCrashHost::build().await,
        )
        .await;
        assert_eq!(
            rebuilt.recovery,
            Some(StartupRecoveryReceipt {
                thread_joins: 1,
                process_outcomes: 0,
            })
        );
        let joined = joined_payloads(&rebuilt.store, &rebuilt.parent).await;
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].0.child_thread_id, rebuilt.child.thread_id);
        assert_eq!(joined[0].0.terminal_state, ThreadTerminalState::Completed);
        assert!(
            joined[0].1["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("startup recovery"))
        );
    }

    #[tokio::test]
    async fn thread_truth_and_dead_claim_recover_but_queued_request_is_left_alone() {
        let store = open_store("thread-classification").await;
        let completed_parent = coordinates("tenant", "user", "completed-parent");
        let completed_child = coordinates("tenant", "user", "completed-child");
        seed_thread_binding(
            &store,
            &completed_parent,
            &completed_child,
            "completed-dispatch",
        )
        .await;
        seed_child_terminal(
            &store,
            &completed_child,
            "thread-spawn-completed-dispatch",
            EventKind::TurnCompleted,
            None,
        )
        .await;

        let dead_parent = coordinates("tenant", "user", "dead-parent");
        let dead_child = coordinates("tenant", "user", "dead-child");
        seed_thread_binding(&store, &dead_parent, &dead_child, "dead-dispatch").await;

        let queued_parent = coordinates("tenant", "user", "queued-parent");
        seed_queued_thread_request(&store, &queued_parent, "queued-dispatch").await;

        let ingress = Arc::new(RecordingProcessIngress::with_store(store.clone()));
        let dispatcher =
            ProcessHandleDispatcher::new(Arc::new(store.clone()) as Arc<dyn RuntimeStore>, ingress);
        let sweep = StartupRecoverySweep::new(store.clone(), dispatcher, "tenant", "user");

        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt {
                thread_joins: 2,
                process_outcomes: 0,
            }
        );
        let completed = joined_payloads(&store, &completed_parent).await;
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].0.terminal_state,
            ThreadTerminalState::Completed
        );
        assert!(
            completed[0].1["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("startup recovery"))
        );
        let failed = joined_payloads(&store, &dead_parent).await;
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0.terminal_state, ThreadTerminalState::Failed);
        assert!(
            failed[0]
                .0
                .result_digest
                .as_deref()
                .is_some_and(|reason| reason.contains("runtime death"))
        );
        assert!(joined_payloads(&store, &queued_parent).await.is_empty());
        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt::default(),
            "a re-sweep after partial or complete recovery must be idempotent"
        );
    }

    #[tokio::test]
    async fn thread_recovery_scopes_terminal_truth_to_the_dispatched_turn() {
        let store = open_store("thread-terminal-turn-scope").await;

        let in_flight_parent = coordinates("tenant", "user", "in-flight-parent");
        let in_flight_child = coordinates("tenant", "user", "in-flight-child");
        seed_thread_binding(
            &store,
            &in_flight_parent,
            &in_flight_child,
            "in-flight-dispatch",
        )
        .await;
        seed_child_terminal(
            &store,
            &in_flight_child,
            "earlier-turn",
            EventKind::TurnCompleted,
            None,
        )
        .await;

        let completed_parent = coordinates("tenant", "user", "scoped-completed-parent");
        let completed_child = coordinates("tenant", "user", "scoped-completed-child");
        seed_thread_binding(
            &store,
            &completed_parent,
            &completed_child,
            "scoped-completed-dispatch",
        )
        .await;
        seed_child_terminal(
            &store,
            &completed_child,
            "thread-spawn-scoped-completed-dispatch",
            EventKind::TurnCompleted,
            None,
        )
        .await;
        seed_child_terminal(
            &store,
            &completed_child,
            "later-turn",
            EventKind::LoopDenied,
            Some("later turn denied"),
        )
        .await;

        let ingress = Arc::new(RecordingProcessIngress::with_store(store.clone()));
        let sweep = StartupRecoverySweep::new(
            store.clone(),
            ProcessHandleDispatcher::new(Arc::new(store.clone()) as Arc<dyn RuntimeStore>, ingress),
            "tenant",
            "user",
        );

        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt {
                thread_joins: 2,
                process_outcomes: 0,
            }
        );
        let in_flight = joined_payloads(&store, &in_flight_parent).await;
        assert_eq!(in_flight.len(), 1);
        assert_eq!(in_flight[0].0.terminal_state, ThreadTerminalState::Failed);
        assert!(
            in_flight[0]
                .0
                .result_digest
                .as_deref()
                .is_some_and(|reason| reason.contains("runtime death"))
        );
        let completed = joined_payloads(&store, &completed_parent).await;
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].0.terminal_state,
            ThreadTerminalState::Completed
        );
    }

    #[tokio::test]
    async fn app_server_construction_runs_recovery_before_local_surfaces_are_available() {
        let root = test_root("app-server-startup-order");
        let state_home = root.join("state");
        let runtime_home = root.join("runtime");
        std::fs::create_dir_all(&state_home).unwrap();
        let store = SqliteSessionStore::open(state_home.join("session_history.sqlite3"))
            .await
            .unwrap();
        let parent = coordinates("tenant", "user", "app-server-parent");
        let child = coordinates("tenant", "user", "app-server-child");
        seed_thread_binding(&store, &parent, &child, "app-server-dispatch").await;
        seed_child_terminal(
            &store,
            &child,
            "thread-spawn-app-server-dispatch",
            EventKind::TurnCompleted,
            None,
        )
        .await;
        let foreign_parent = coordinates("tenant", "other-user", "foreign-parent");
        let foreign_child = coordinates("tenant", "other-user", "foreign-child");
        seed_thread_binding(&store, &foreign_parent, &foreign_child, "foreign-dispatch").await;
        seed_child_terminal(
            &store,
            &foreign_child,
            "thread-spawn-foreign-dispatch",
            EventKind::TurnCompleted,
            None,
        )
        .await;

        let listen = crate::AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap());
        let mut config = CooldisAppServerConfig::local(listen, &root);
        config.apply_daemon_identity_config(&CooldisDaemonIdentityConfig {
            mode: IdentityMode::Local,
            tenant_id: Some("tenant".to_string()),
            console_principal: Some(PrincipalId::new("user")),
        });
        config.runtime_home = runtime_home;
        config.state_home = state_home;
        config.user_state_home = root.join("user-state");
        config.agent_registry_root = root.join("agents");
        config.blob_registry_root = root.join("blobs");
        config.skill_registry_root = root.join("skills");

        let _app = CooldisAppServer::new_local(config).await.unwrap();
        let joined = joined_payloads(&store, &parent).await;
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].0.terminal_state, ThreadTerminalState::Completed);
        assert!(joined_payloads(&store, &foreign_parent).await.is_empty());
    }

    #[tokio::test]
    async fn committed_join_is_first_wins_and_is_never_overwritten() {
        let store = open_store("thread-first-join-wins").await;
        let parent = coordinates("tenant", "user", "first-join-parent");
        let child = coordinates("tenant", "user", "first-join-child");
        let spawned_event_id =
            seed_thread_binding(&store, &parent, &child, "first-join-dispatch").await;
        seed_child_terminal(
            &store,
            &child,
            "thread-spawn-first-join-dispatch",
            EventKind::TurnCompleted,
            None,
        )
        .await;
        store
            .append_events(
                &control_stream_id(&parent),
                vec![NewEventRecord::discharged(
                    parent.clone(),
                    EventKind::ThreadJoined,
                    serde_json::to_value(ThreadJoinedPayload {
                        child_thread_id: child.thread_id,
                        spawned_event_id,
                        terminal_state: ThreadTerminalState::Failed,
                        result_digest: Some("original committed outcome".to_string()),
                    })
                    .unwrap(),
                    EventProvenance {
                        discharged_by: Some("runtime:thread-lifecycle".to_string()),
                        function: Some("thread_join/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();
        let ingress = Arc::new(RecordingProcessIngress::with_store(store.clone()));
        let sweep = StartupRecoverySweep::new(
            store.clone(),
            ProcessHandleDispatcher::new(Arc::new(store.clone()) as Arc<dyn RuntimeStore>, ingress),
            "tenant",
            "user",
        );

        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt::default()
        );
        let joined = joined_payloads(&store, &parent).await;
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].0.terminal_state, ThreadTerminalState::Failed);
        assert_eq!(
            joined[0].0.result_digest.as_deref(),
            Some("original committed outcome")
        );
    }

    #[tokio::test]
    async fn concurrent_live_and_recovery_join_attempts_commit_only_the_first() {
        let store = open_store("thread-concurrent-first-join-wins").await;
        let parent = coordinates("tenant", "user", "concurrent-first-join-parent");
        let child = coordinates("tenant", "user", "concurrent-first-join-child");
        let spawned_event_id =
            seed_thread_binding(&store, &parent, &child, "concurrent-first-join").await;

        let live = append_thread_joined_first_wins(
            &store,
            parent.clone(),
            child.clone(),
            spawned_event_id,
            ThreadTerminalState::Completed,
            Some("live completed".to_string()),
            None,
            None,
            "runtime:thread-lifecycle",
            "thread_join/v1",
        );
        let recovery = append_thread_joined_first_wins(
            &store,
            parent.clone(),
            child,
            spawned_event_id,
            ThreadTerminalState::Failed,
            Some("recovery failed".to_string()),
            Some("startup recovery race".to_string()),
            None,
            "recovery:startup-sweep",
            "thread_join_recovery/v1",
        );
        let (live, recovery) = tokio::join!(live, recovery);
        let live = live.unwrap();
        let recovery = recovery.unwrap();

        assert_ne!(live.appended, recovery.appended);
        assert_eq!(live.record.id, recovery.record.id);
        assert_eq!(joined_payloads(&store, &parent).await.len(), 1);
    }

    #[tokio::test]
    async fn process_orphans_fold_every_stream_and_duplicate_outcomes_deliver_once() {
        let store = open_store("process-fold").await;
        let first = coordinates("tenant", "user", "process-first");
        let second = coordinates("tenant", "user", "process-second");
        let settled = coordinates("tenant", "user", "process-settled");
        let first_binding = seed_process_dispatch(&store, &first, "process-first").await;
        seed_process_dispatch(&store, &second, "process-second").await;
        let settled_binding = seed_process_dispatch(&store, &settled, "process-settled").await;

        let ingress = Arc::new(RecordingProcessIngress::with_store(store.clone()));
        let dispatcher = ProcessHandleDispatcher::new(
            Arc::new(store.clone()) as Arc<dyn RuntimeStore>,
            ingress.clone(),
        );
        dispatcher.assert_startup_registry_empty().await.unwrap();
        dispatcher
            .submit_recovery_outcome(&settled_binding)
            .await
            .unwrap();
        let legacy = coordinates("tenant", "user", "legacy-content");
        store
            .append_events(
                &control_stream_id(&legacy),
                vec![NewEventRecord::witnessed(
                    legacy,
                    EventKind::IoIngressReceived,
                    json!({
                        "route_id": "legacy",
                        "envelope_digest": "sha256:legacy"
                    }),
                )],
            )
            .await
            .unwrap();

        let sweep = StartupRecoverySweep::new(store.clone(), dispatcher.clone(), "tenant", "user");
        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt {
                thread_joins: 0,
                process_outcomes: 2,
            }
        );
        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt::default()
        );
        dispatcher
            .submit_recovery_outcome(&first_binding)
            .await
            .unwrap();

        let outcomes = ingress.outcomes().await;
        assert_eq!(outcomes.len(), 3, "one settled plus two orphan deliveries");
        for outcome in outcomes {
            assert_eq!(outcome.outcome, HandleTerminalOutcome::Failed);
            assert!(outcome.retryable);
            assert!(
                outcome
                    .outcome_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("observer death")
                        && reason.contains("exit status unknown"))
            );
        }
    }

    #[tokio::test]
    async fn process_partial_sweep_then_restart_converges_without_redelivery() {
        let store = open_store("process-partial-resweep").await;
        let first = coordinates("tenant", "user", "process-partial-first");
        let second = coordinates("tenant", "user", "process-partial-second");
        seed_process_dispatch(&store, &first, "process-partial-first").await;
        seed_process_dispatch(&store, &second, "process-partial-second").await;

        let recording = Arc::new(RecordingProcessIngress::with_store(store.clone()));
        let failing = Arc::new(FailOnceProcessIngress {
            inner: Arc::clone(&recording),
            dispatch_id: "process-partial-second".to_string(),
            failed: Mutex::new(false),
        });
        let interrupted = StartupRecoverySweep::new(
            store.clone(),
            ProcessHandleDispatcher::new(Arc::new(store.clone()) as Arc<dyn RuntimeStore>, failing),
            "tenant",
            "user",
        );
        assert_eq!(
            interrupted.run_once().await.unwrap(),
            StartupRecoveryReceipt {
                thread_joins: 0,
                process_outcomes: 1,
            }
        );

        let restarted = StartupRecoverySweep::new(
            store.clone(),
            ProcessHandleDispatcher::new(
                Arc::new(store.clone()) as Arc<dyn RuntimeStore>,
                recording.clone(),
            ),
            "tenant",
            "user",
        );
        assert_eq!(
            restarted.run_once().await.unwrap(),
            StartupRecoveryReceipt {
                thread_joins: 0,
                process_outcomes: 1,
            }
        );
        assert_eq!(
            restarted.run_once().await.unwrap(),
            StartupRecoveryReceipt::default()
        );
        assert_eq!(recording.outcomes().await.len(), 2);
    }

    #[tokio::test]
    async fn corrupt_control_stream_does_not_block_healthy_process_recovery() {
        let store = open_store("process-corrupt-stream-isolation").await;
        let corrupt = coordinates("tenant", "user", "process-corrupt");
        let healthy = coordinates("tenant", "user", "process-healthy");
        store
            .append_events(
                &control_stream_id(&corrupt),
                vec![NewEventRecord::witnessed(
                    corrupt.clone(),
                    EventKind::IoIngressReceived,
                    serde_json::to_value(IoIngressReceivedPayload {
                        route_id: Some(HANDLE_DISPATCH_CONTENT_KIND.to_string()),
                        dedupe_key: Some(format!("{HANDLE_DISPATCH_CONTENT_KIND}:process-corrupt")),
                        external_conversation_id: Some(format!("thread:{}", corrupt.thread_id)),
                        external_actor_id: None,
                        external_message_id: None,
                        content: Some(json!({
                            "kind": HANDLE_DISPATCH_CONTENT_KIND,
                            "payload": {"dispatch_id": "process-corrupt"}
                        })),
                        envelope_digest: "sha256:corrupt".to_string(),
                    })
                    .unwrap(),
                )],
            )
            .await
            .unwrap();
        seed_process_dispatch(&store, &healthy, "process-healthy").await;

        let recording = Arc::new(RecordingProcessIngress::with_store(store.clone()));
        let sweep = StartupRecoverySweep::new(
            store.clone(),
            ProcessHandleDispatcher::new(
                Arc::new(store.clone()) as Arc<dyn RuntimeStore>,
                recording.clone(),
            ),
            "tenant",
            "user",
        );
        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt {
                thread_joins: 0,
                process_outcomes: 1,
            }
        );
        let outcomes = recording.outcomes().await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].dispatch_id.as_str(), "process-healthy");
    }

    #[tokio::test]
    async fn malformed_process_content_isolates_only_that_item() {
        let store = open_store("process-malformed-item-isolation").await;
        let consumer = coordinates("tenant", "user", "process-malformed-item");
        seed_process_dispatch(&store, &consumer, "process-malformed-item").await;
        store
            .append_events(
                &control_stream_id(&consumer),
                vec![NewEventRecord::witnessed(
                    consumer.clone(),
                    EventKind::IoIngressReceived,
                    serde_json::to_value(IoIngressReceivedPayload {
                        route_id: Some("malformed".to_string()),
                        dedupe_key: Some("malformed:item".to_string()),
                        external_conversation_id: Some(format!("thread:{}", consumer.thread_id)),
                        external_actor_id: None,
                        external_message_id: None,
                        content: Some(json!({"malformed": true})),
                        envelope_digest: "sha256:malformed".to_string(),
                    })
                    .unwrap(),
                )],
            )
            .await
            .unwrap();

        let recording = Arc::new(RecordingProcessIngress::with_store(store.clone()));
        let sweep = StartupRecoverySweep::new(
            store.clone(),
            ProcessHandleDispatcher::new(
                Arc::new(store) as Arc<dyn RuntimeStore>,
                recording.clone(),
            ),
            "tenant",
            "user",
        );
        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt {
                thread_joins: 0,
                process_outcomes: 1,
            }
        );
        assert_eq!(recording.outcomes().await.len(), 1);
    }

    struct BlockingProcessBackend {
        started: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl LiveProcessBackend for BlockingProcessBackend {
        fn backend_kind(&self) -> CooldisProcessBackend {
            CooldisProcessBackend::Bridge
        }

        async fn start(
            &self,
            _request: LiveProcessStartRequest,
            process: CooldisProcessHandle,
            cancellation: CancellationToken,
        ) -> cooldis_process::CooldisProcessResult<LiveProcessSpawn> {
            process.record(CooldisProcessEventKind::Started {
                command: Some("blocking recovery test".to_string()),
            });
            self.started.notify_one();
            let join = tokio::spawn(async move {
                cancellation.cancelled().await;
                process.record(CooldisProcessEventKind::Cancelled {
                    reason: "test cleanup".to_string(),
                });
                Ok(())
            });
            Ok(LiveProcessSpawn { stdin: None, join })
        }
    }

    #[tokio::test]
    async fn process_live_in_the_dispatcher_registry_is_not_swept() {
        let store = open_store("process-live-skip").await;
        let consumer = coordinates("tenant", "user", "process-live");
        let ingress = Arc::new(RecordingProcessIngress::with_store(store.clone()));
        let dispatcher = ProcessHandleDispatcher::new(
            Arc::new(store.clone()) as Arc<dyn RuntimeStore>,
            ingress.clone(),
        );
        let manager = AsyncExecutionManager::default();
        let started = Arc::new(Notify::new());
        let started_wait = started.notified();
        let dispatch_id = DispatchId::new("process-live");
        let outcome = dispatcher
            .dispatch_start(
                &consumer,
                dispatch_id.clone(),
                command_digest(b"blocking recovery test"),
                manager.clone(),
                Arc::new(BlockingProcessBackend {
                    started: Arc::clone(&started),
                }),
                AsyncProcessStartRequest::virtual_bash_script("blocking recovery test")
                    .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(30)))
                    .with_yield_time(Duration::ZERO),
            )
            .await
            .unwrap();
        started_wait.await;
        assert!(dispatcher.is_live_dispatch(&dispatch_id).await);
        assert!(dispatcher.assert_startup_registry_empty().await.is_err());

        let sweep = StartupRecoverySweep::new(store, dispatcher.clone(), "tenant", "user");
        assert_eq!(
            sweep.run_once().await.unwrap(),
            StartupRecoveryReceipt::default()
        );
        assert!(ingress.outcomes().await.is_empty());

        manager
            .terminate(
                outcome
                    .snapshot
                    .process_id
                    .expect("started process snapshot must carry its process id"),
                "test cleanup",
                Duration::from_millis(10),
                1024,
            )
            .await
            .unwrap();
    }

    #[test]
    fn recovery_process_envelope_reuses_the_standard_outcome_builder() {
        let binding = HandleDispatchEnvelope {
            dispatch_id: DispatchId::new("builder-reuse"),
            handle: HandleId::process("018f0000-0000-7000-8000-000000000426"),
            consumer: coordinates("tenant", "user", "builder-reuse"),
            command_digest: command_digest(b"builder reuse"),
        };
        let envelope = recovery_outcome_envelope(&binding).unwrap();
        assert_eq!(
            envelope.dedupe_key,
            Some(IoDedupeKey::new(
                HANDLE_OUTCOME_CONTENT_KIND,
                "builder-reuse"
            ))
        );
    }
}
