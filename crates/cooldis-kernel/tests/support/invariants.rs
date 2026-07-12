#![allow(dead_code)]

//! Scenario invariant library v1 (ADR 0004, invariants 1-5).
//!
//! The runner emits the durable witness shapes below without widening
//! [`ScenarioWorld`]:
//!
//! - event streams are discovered from normalized transcript event values
//!   (the runner must preserve literal `stream_id` values while normalizing);
//! - inv2 reads `runtime_id` plus `runtime_state = active | terminal` from
//!   durable event payloads and uses the ordered `shutdown_all.completed`
//!   transcript receipt as the completed-shutdown cut;
//! - inv3 reads non-mutating queue probe receipts (`queue.lease`,
//!   `queue.redelivery`, `queue.complete`, `queue.clock`, and
//!   `queue.drain.completed`) because `IngressQueueStore` has no read-only
//!   inspection operation;
//! - inv5 uses a `recovery.probe` transcript receipt naming a
//!   `reservation_key`. A durable event with `resident_state = failed |
//!   completed` and that key must be followed by a durable event carrying
//!   `reservation_progress` with the same key.
//!
//! The inv5 shape is a deviation note for architect ratification: v1 treats a
//! completed recovery probe plus later durable progress evidence as the proof
//! that terminal resident residue did not wedge the reservation. It does not
//! inspect host reservation maps or require a new lifecycle event kind.

use super::kernel_test::{
    EventKind, EventRecord, EventStreamId, SessionEntryId, ThreadCoordinates,
};
use super::{InvariantViolation, ScenarioInvariant, ScenarioWorld};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

pub const INV1_REPLAY_EQUIVALENCE: &str = "inv1-replay-equivalence";
pub const INV2_UNIQUE_ACTIVE_TOPOLOGY: &str = "inv2-unique-active-topology";
pub const INV3_BOUNDED_QUEUE: &str = "inv3-bounded-queue";
pub const INV4_NO_DUPLICATE_PROJECTED_OUTPUT: &str = "inv4-no-duplicate-projected-output";
pub const INV5_TERMINAL_CONSISTENCY: &str = "inv5-terminal-consistency";

#[derive(Clone, Copy, Debug, Default)]
pub struct ReplayEquivalenceInvariant;

#[derive(Clone, Copy, Debug, Default)]
pub struct UniqueActiveTopologyInvariant;

#[derive(Clone, Copy, Debug, Default)]
pub struct BoundedQueueInvariant;

#[derive(Clone, Copy, Debug, Default)]
pub struct NoDuplicateProjectedOutputInvariant;

#[derive(Clone, Copy, Debug, Default)]
pub struct TerminalConsistencyInvariant;

pub fn invariant_set_v1() -> Vec<Box<dyn ScenarioInvariant>> {
    vec![
        Box::new(ReplayEquivalenceInvariant),
        Box::new(UniqueActiveTopologyInvariant),
        Box::new(BoundedQueueInvariant),
        Box::new(NoDuplicateProjectedOutputInvariant),
        Box::new(TerminalConsistencyInvariant),
    ]
}

fn violation(invariant: &'static str, detail: impl Into<String>) -> InvariantViolation {
    InvariantViolation {
        invariant,
        detail: detail.into(),
    }
}

fn transcript_stream_ids(world: &ScenarioWorld<'_>) -> Vec<EventStreamId> {
    let mut stream_ids = world
        .transcript
        .items
        .iter()
        .filter(|item| item.kind == "event")
        .filter_map(|item| item.value.get("stream_id").and_then(Value::as_str))
        .filter(|stream_id| !stream_id.starts_with('$'))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    stream_ids.sort();
    stream_ids.dedup();
    stream_ids.into_iter().map(EventStreamId::new).collect()
}

async fn durable_events(
    world: &ScenarioWorld<'_>,
    invariant: &'static str,
) -> Result<Vec<EventRecord>, Vec<InvariantViolation>> {
    let mut events = Vec::new();
    let mut violations = Vec::new();
    for stream_id in transcript_stream_ids(world) {
        match world.store.read_events(&stream_id, None).await {
            Ok(mut stream_events) => events.append(&mut stream_events),
            Err(error) => violations.push(violation(
                invariant,
                format!("could not read durable stream {stream_id}: {error}"),
            )),
        }
    }
    if violations.is_empty() {
        Ok(events)
    } else {
        Err(violations)
    }
}

#[async_trait]
impl ScenarioInvariant for ReplayEquivalenceInvariant {
    fn name(&self) -> &'static str {
        INV1_REPLAY_EQUIVALENCE
    }

    async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
        let mut violations = Vec::new();
        for stream_id in transcript_stream_ids(world) {
            let events = match world.store.read_events(&stream_id, None).await {
                Ok(events) => events,
                Err(error) => {
                    violations.push(violation(
                        self.name(),
                        format!("could not replay durable stream {stream_id}: {error}"),
                    ));
                    continue;
                }
            };

            for (index, event) in events.iter().enumerate() {
                let expected = index as i64 + 1;
                if event.sequence.get() != expected {
                    violations.push(violation(
                        self.name(),
                        format!(
                            "stream {stream_id} sequence is not strictly monotonic: expected {expected}, found {} at event {}",
                            event.sequence.get(),
                            event.id
                        ),
                    ));
                }
            }

            // SessionEntryAppended and ThreadBranchSelected are the journal
            // records that define the store's active-leaf fold. Replay them
            // from sequence one, then compare the result with durable folded
            // state exposed by SessionStore::active_leaf.
            let mut replayed_leaf: BTreeMap<String, (ThreadCoordinates, Option<String>)> =
                BTreeMap::new();
            for event in &events {
                let key = event.coordinates.thread_id.to_string();
                match event.kind {
                    EventKind::SessionEntryAppended => {
                        if let Some(entry_id) =
                            event.payload.get("entry_id").and_then(Value::as_str)
                        {
                            replayed_leaf.insert(
                                key,
                                (event.coordinates.clone(), Some(entry_id.to_string())),
                            );
                        }
                    }
                    EventKind::ThreadBranchSelected => {
                        let selected = event
                            .payload
                            .get("selected_entry_id")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned);
                        replayed_leaf.insert(key, (event.coordinates.clone(), selected));
                    }
                    _ => {}
                }
            }

            for (_, (coordinates, replayed)) in replayed_leaf {
                match world.store.active_leaf(&coordinates).await {
                    Ok(current) => {
                        let current = current.map(|entry_id: SessionEntryId| entry_id.to_string());
                        if current != replayed {
                            violations.push(violation(
                                self.name(),
                                format!(
                                    "stream {stream_id} replay derived active leaf {replayed:?} for thread {}, but durable folded state is {current:?}",
                                    coordinates.thread_id
                                ),
                            ));
                        }
                    }
                    Err(error) => violations.push(violation(
                        self.name(),
                        format!(
                            "could not read folded active leaf for thread {} after replaying {stream_id}: {error}",
                            coordinates.thread_id
                        ),
                    )),
                }
            }
        }
        violations
    }
}

#[async_trait]
impl ScenarioInvariant for UniqueActiveTopologyInvariant {
    fn name(&self) -> &'static str {
        INV2_UNIQUE_ACTIVE_TOPOLOGY
    }

    async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
        let events = match durable_events(world, self.name()).await {
            Ok(events) => events,
            Err(violations) => return violations,
        };
        let mut violations = Vec::new();
        let mut active: HashMap<String, HashSet<String>> = HashMap::new();
        let mut ordered = events;
        ordered.sort_by_key(|event| {
            (
                event.created_at_ms,
                event.stream_id.as_str().to_string(),
                event.sequence.get(),
            )
        });
        for event in ordered {
            let Some(runtime_id) = event.payload.get("runtime_id").and_then(Value::as_str) else {
                continue;
            };
            let thread_id = event.coordinates.thread_id.to_string();
            match event.payload.get("runtime_state").and_then(Value::as_str) {
                Some("active") => {
                    let runtimes = active.entry(thread_id.clone()).or_default();
                    runtimes.insert(runtime_id.to_string());
                    if runtimes.len() > 1 {
                        let mut ids = runtimes.iter().cloned().collect::<Vec<_>>();
                        ids.sort();
                        violations.push(violation(
                            self.name(),
                            format!(
                                "thread {thread_id} has {} durable active runtime records: {}",
                                ids.len(),
                                ids.join(", ")
                            ),
                        ));
                    }
                }
                Some("terminal") => {
                    active.entry(thread_id).or_default().remove(runtime_id);
                }
                _ => {}
            }
        }

        if world.shut_down {
            let Some(cut) = world
                .transcript
                .items
                .iter()
                .rposition(|item| item.label == "shutdown_all.completed")
            else {
                violations.push(violation(
                    self.name(),
                    "world is shut down but transcript has no shutdown_all.completed cut",
                ));
                return violations;
            };
            for item in world.transcript.items.iter().skip(cut + 1) {
                let kind = item.value.get("kind").and_then(Value::as_str);
                if kind.is_some_and(is_execution_event_kind) {
                    violations.push(violation(
                        self.name(),
                        format!(
                            "execution event {} was recorded after shutdown_all completed at transcript item {cut}",
                            kind.unwrap()
                        ),
                    ));
                }
            }
        }
        violations
    }
}

fn is_execution_event_kind(kind: &str) -> bool {
    matches!(
        kind,
        "turn.submitted"
            | "turn.resumed"
            | "context.compile.completed"
            | "tool.call.requested"
            | "tool.call.completed"
            | "turn.completed"
            | "loop.completed"
    )
}

#[derive(Clone, Debug)]
struct LeaseWitness {
    item_index: usize,
    visible_until_tick: u64,
    attempt: u64,
}

#[async_trait]
impl ScenarioInvariant for BoundedQueueInvariant {
    fn name(&self) -> &'static str {
        INV3_BOUNDED_QUEUE
    }

    async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
        if world.queue.is_none() {
            return Vec::new();
        }

        let mut now = None;
        let mut leases: HashMap<String, LeaseWitness> = HashMap::new();
        let mut redeliveries: HashMap<String, Vec<(usize, u64)>> = HashMap::new();
        let mut completions: HashMap<String, Vec<usize>> = HashMap::new();
        let mut violations = Vec::new();
        for (index, item) in world.transcript.items.iter().enumerate() {
            match item.label.as_str() {
                "queue.clock" => now = item.value.get("tick").and_then(Value::as_u64),
                "queue.lease" => {
                    if let (Some(message_id), Some(visible_until_tick), Some(attempt)) = (
                        item.value.get("message_id").and_then(Value::as_str),
                        item.value.get("visible_until_tick").and_then(Value::as_u64),
                        item.value.get("attempt").and_then(Value::as_u64),
                    ) {
                        leases.insert(
                            message_id.to_string(),
                            LeaseWitness {
                                item_index: index,
                                visible_until_tick,
                                attempt,
                            },
                        );
                    }
                }
                "queue.redelivery" => {
                    if let (Some(message_id), Some(attempt)) = (
                        item.value.get("message_id").and_then(Value::as_str),
                        item.value.get("attempt").and_then(Value::as_u64),
                    ) {
                        redeliveries
                            .entry(message_id.to_string())
                            .or_default()
                            .push((index, attempt));
                    }
                }
                "queue.complete" => {
                    if let Some(message_id) = item.value.get("message_id").and_then(Value::as_str) {
                        completions
                            .entry(message_id.to_string())
                            .or_default()
                            .push(index);
                    }
                }
                "queue.drain.completed" => {
                    if let Some(remaining) = item.value.get("remaining").and_then(Value::as_u64)
                        && remaining != 0
                    {
                        violations.push(violation(
                            self.name(),
                            format!(
                                "queue drain completed with {remaining} message(s) still durable"
                            ),
                        ));
                    }
                }
                _ => {}
            }
        }

        if let Some(now) = now {
            for (message_id, lease) in leases {
                if now <= lease.visible_until_tick {
                    continue;
                }
                let completed = completions
                    .get(&message_id)
                    .is_some_and(|indices| indices.iter().any(|index| *index > lease.item_index));
                if completed {
                    continue;
                }
                let redelivered = redeliveries.get(&message_id).is_some_and(|attempts| {
                    attempts.iter().any(|(index, attempt)| {
                        *index > lease.item_index && *attempt > lease.attempt
                    })
                });
                if !redelivered {
                    violations.push(violation(
                        self.name(),
                        format!(
                            "queue message {message_id} lease attempt {} expired at tick {}, but no later redelivery was recorded by tick {now}",
                            lease.attempt, lease.visible_until_tick
                        ),
                    ));
                }
            }
        }
        violations
    }
}

#[async_trait]
impl ScenarioInvariant for NoDuplicateProjectedOutputInvariant {
    fn name(&self) -> &'static str {
        INV4_NO_DUPLICATE_PROJECTED_OUTPUT
    }

    async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
        let events = match durable_events(world, self.name()).await {
            Ok(events) => events,
            Err(violations) => return violations,
        };
        let mut delivered: HashMap<String, Vec<String>> = HashMap::new();
        for event in events
            .into_iter()
            .filter(|event| event.kind == EventKind::IoEgressDelivered)
        {
            let correlation = event
                .payload
                .get("dedupe_key")
                .or_else(|| event.payload.get("correlation_id"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    let source = event.payload.get("source_event_id")?.as_str()?;
                    let index = event.payload.get("envelope_index")?.as_u64()?;
                    Some(format!("{source}:{index}"))
                });
            if let Some(correlation) = correlation {
                delivered
                    .entry(correlation)
                    .or_default()
                    .push(event.id.to_string());
            }
        }

        delivered
            .into_iter()
            .filter(|(_, event_ids)| event_ids.len() > 1)
            .map(|(correlation, event_ids)| {
                violation(
                    self.name(),
                    format!(
                        "egress correlation {correlation} has {} published delivery records: {}",
                        event_ids.len(),
                        event_ids.join(", ")
                    ),
                )
            })
            .collect()
    }
}

#[async_trait]
impl ScenarioInvariant for TerminalConsistencyInvariant {
    fn name(&self) -> &'static str {
        INV5_TERMINAL_CONSISTENCY
    }

    async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
        match durable_events(world, self.name()).await {
            Ok(_) => {}
            Err(violations) => return violations,
        }
        let mut probes: HashMap<String, Vec<usize>> = HashMap::new();
        let mut terminal: HashMap<String, Vec<(String, usize)>> = HashMap::new();
        let mut progress: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, item) in world.transcript.items.iter().enumerate() {
            if item.label == "recovery.probe"
                && let Some(key) = item.value.get("reservation_key").and_then(Value::as_str)
            {
                probes.entry(key.to_string()).or_default().push(index);
            }
            if item.kind != "event" {
                continue;
            }
            let Some(payload) = item.value.get("payload") else {
                continue;
            };
            if let (Some(state @ ("failed" | "completed")), Some(key)) = (
                payload.get("resident_state").and_then(Value::as_str),
                payload.get("reservation_key").and_then(Value::as_str),
            ) {
                terminal
                    .entry(key.to_string())
                    .or_default()
                    .push((state.to_string(), index));
            }
            if let Some(key) = payload.get("reservation_progress").and_then(Value::as_str) {
                progress.entry(key.to_string()).or_default().push(index);
            }
        }
        if probes.is_empty() {
            return Vec::new();
        }
        terminal
            .into_iter()
            .flat_map(|(key, terminals)| {
                terminals.into_iter().filter_map({
                    let probes = probes.get(&key);
                    let progress = progress.get(&key);
                    move |(state, terminal_index)| {
                        let probe_index = probes?
                            .iter()
                            .copied()
                            .find(|probe_index| *probe_index > terminal_index)?;
                        progress
                            .is_none_or(|indices| {
                                !indices
                                    .iter()
                                    .any(|progress_index| *progress_index > probe_index)
                            })
                            .then_some((key.clone(), state, terminal_index, probe_index))
                    }
                })
            })
            .map(|(key, state, terminal_index, probe_index)| {
                violation(
                    self.name(),
                    format!(
                        "{state} resident for reservation {key} at transcript item {terminal_index} has a completed recovery probe at item {probe_index} but no later durable progress evidence"
                    ),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::kernel_test::{
        CanonicalMessage, EventOrigin, EventProvenance, EventRecordId, EventStore,
        InMemorySessionStore, NewEventRecord, SessionEntryKind, SessionStore, ThreadId,
    };
    use super::super::transcript::{
        NormalizedTranscript, NormalizedTranscriptItem, TypedTranscript,
    };
    use super::*;
    use async_trait::async_trait;
    use cooldis_io_core::{
        IngressAck, IngressEnvelope, IngressQueueStore, IngressSink, IoResult,
        LeasedIngressEnvelope,
    };
    use serde_json::json;
    use uuid::Uuid;

    fn coordinates() -> ThreadCoordinates {
        ThreadCoordinates {
            tenant_id: "scenario-tenant".to_string(),
            user_id: "scenario-user".to_string(),
            session_id: "scenario-session".to_string(),
            thread_id: ThreadId::parse_str("00000000-0000-0000-0000-000000000400").unwrap(),
        }
    }

    fn stream_id() -> EventStreamId {
        EventStreamId::new(
            "thread:scenario-tenant:scenario-user:scenario-session:00000000-0000-0000-0000-000000000400",
        )
    }

    fn event(id: u128, at: i64, kind: EventKind, payload: Value) -> NewEventRecord {
        NewEventRecord {
            id: EventRecordId::from_uuid(Uuid::from_u128(id)),
            coordinates: coordinates(),
            created_at_ms: at,
            kind,
            origin: EventOrigin::Witnessed,
            provenance: EventProvenance::default(),
            payload,
        }
    }

    fn transcript_for(events: &[EventRecord]) -> NormalizedTranscript {
        let mut transcript = TypedTranscript::new();
        transcript.preserve_id(stream_id().as_str());
        for event in events {
            transcript.push_event("journal", event);
        }
        transcript.normalize()
    }

    fn receipt(label: &str, value: Value) -> NormalizedTranscriptItem {
        NormalizedTranscriptItem {
            kind: "receipt".to_string(),
            label: label.to_string(),
            value,
        }
    }

    async fn append(
        store: &InMemorySessionStore,
        records: Vec<NewEventRecord>,
    ) -> Vec<EventRecord> {
        store.append_events(&stream_id(), records).await.unwrap()
    }

    fn world<'a>(
        store: &'a InMemorySessionStore,
        transcript: &'a NormalizedTranscript,
    ) -> ScenarioWorld<'a> {
        ScenarioWorld {
            store,
            queue: None,
            transcript,
            step: 0,
            shut_down: false,
        }
    }

    #[tokio::test]
    async fn inv1_holds_for_monotonic_journal_whose_replay_matches_fold() {
        let store = InMemorySessionStore::new();
        store
            .append(
                &coordinates(),
                None,
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text_at("hello", 1),
                },
            )
            .await
            .unwrap();
        let events = store.read_events(&stream_id(), None).await.unwrap();
        let transcript = transcript_for(&events);
        assert!(
            ReplayEquivalenceInvariant
                .check(&world(&store, &transcript))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inv1_reports_replay_fold_mismatch_from_bad_durable_history() {
        let store = InMemorySessionStore::new();
        store
            .append(
                &coordinates(),
                None,
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text_at("hello", 1),
                },
            )
            .await
            .unwrap();
        append(
            &store,
            vec![event(
                2,
                2,
                EventKind::ThreadBranchSelected,
                json!({"selected_entry_id": null}),
            )],
        )
        .await;
        let events = store.read_events(&stream_id(), None).await.unwrap();
        let transcript = transcript_for(&events);
        let violations = ReplayEquivalenceInvariant
            .check(&world(&store, &transcript))
            .await;
        assert_eq!(violations.len(), 1);
        assert!(violations[0].detail.contains("durable folded state"));
    }

    #[tokio::test]
    async fn inv2_holds_for_one_active_runtime_and_clean_shutdown_cut() {
        let store = InMemorySessionStore::new();
        let events = append(
            &store,
            vec![event(
                10,
                1,
                EventKind::PlacementDecision,
                json!({
                    "runtime_id": "runtime-a", "runtime_state": "active"
                }),
            )],
        )
        .await;
        let mut transcript = transcript_for(&events);
        transcript
            .items
            .push(receipt("shutdown_all.completed", json!({"step": 1})));
        let mut scenario_world = world(&store, &transcript);
        scenario_world.shut_down = true;
        assert!(
            UniqueActiveTopologyInvariant
                .check(&scenario_world)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inv2_reports_two_durable_active_runtimes_for_one_thread() {
        let store = InMemorySessionStore::new();
        let events = append(
            &store,
            vec![
                event(
                    11,
                    1,
                    EventKind::PlacementDecision,
                    json!({"runtime_id": "runtime-a", "runtime_state": "active"}),
                ),
                event(
                    12,
                    2,
                    EventKind::PlacementDecision,
                    json!({"runtime_id": "runtime-b", "runtime_state": "active"}),
                ),
            ],
        )
        .await;
        let transcript = transcript_for(&events);
        let violations = UniqueActiveTopologyInvariant
            .check(&world(&store, &transcript))
            .await;
        assert_eq!(violations.len(), 1);
        assert!(violations[0].detail.contains("runtime-a, runtime-b"));
    }

    #[tokio::test]
    async fn inv2_reports_durable_execution_after_shutdown_completed() {
        let store = InMemorySessionStore::new();
        let events = append(
            &store,
            vec![
                event(
                    13,
                    1,
                    EventKind::PlacementDecision,
                    json!({"runtime_id": "runtime-a", "runtime_state": "active"}),
                ),
                event(
                    14,
                    2,
                    EventKind::TurnSubmitted,
                    json!({"turn_id": "after-shutdown"}),
                ),
            ],
        )
        .await;
        let normalized = transcript_for(&events);
        let transcript = NormalizedTranscript {
            items: vec![
                normalized.items[0].clone(),
                receipt("shutdown_all.completed", json!({"step": 1})),
                normalized.items[1].clone(),
            ],
        };
        let mut scenario_world = world(&store, &transcript);
        scenario_world.shut_down = true;
        let violations = UniqueActiveTopologyInvariant.check(&scenario_world).await;
        assert_eq!(violations.len(), 1);
        assert!(violations[0].detail.contains("turn.submitted"));
        assert!(
            violations[0]
                .detail
                .contains("after shutdown_all completed")
        );
    }

    #[derive(Default)]
    struct EmptyQueue;

    #[async_trait]
    impl IngressSink for EmptyQueue {
        async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
            Ok(IngressAck::accepted(&envelope))
        }
    }

    #[async_trait]
    impl IngressQueueStore for EmptyQueue {
        async fn lease_ingress(
            &self,
            _: &str,
            _: usize,
            _: u32,
        ) -> IoResult<Vec<LeasedIngressEnvelope>> {
            Ok(Vec::new())
        }
        async fn complete_ingress(&self, _: &str) -> IoResult<()> {
            Ok(())
        }
        async fn hold_ingress_until(&self, _: &str, _: u64) -> IoResult<()> {
            Ok(())
        }
        async fn retry_ingress(&self, _: &str, _: &str) -> IoResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn inv3_holds_when_expired_lease_was_redelivered_and_drain_is_empty() {
        let store = InMemorySessionStore::new();
        let queue = EmptyQueue;
        let transcript = NormalizedTranscript {
            items: vec![
                receipt(
                    "queue.lease",
                    json!({"message_id": "m1", "attempt": 1, "visible_until_tick": 5}),
                ),
                receipt(
                    "queue.redelivery",
                    json!({"message_id": "m1", "attempt": 2}),
                ),
                receipt("queue.clock", json!({"tick": 6})),
                receipt("queue.drain.completed", json!({"remaining": 0})),
            ],
        };
        let mut scenario_world = world(&store, &transcript);
        scenario_world.queue = Some(&queue);
        assert!(
            BoundedQueueInvariant
                .check(&scenario_world)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inv3_discharges_completed_lease_but_reports_uncompleted_expired_lease() {
        let store = InMemorySessionStore::new();
        let queue = EmptyQueue;
        let completed = NormalizedTranscript {
            items: vec![
                receipt(
                    "queue.lease",
                    json!({"message_id": "completed", "attempt": 1, "visible_until_tick": 5}),
                ),
                receipt(
                    "queue.complete",
                    json!({"message_id": "completed", "attempt": 1, "tick": 3}),
                ),
                receipt("queue.clock", json!({"tick": 6})),
            ],
        };
        let mut completed_world = world(&store, &completed);
        completed_world.queue = Some(&queue);
        assert!(
            BoundedQueueInvariant
                .check(&completed_world)
                .await
                .is_empty(),
            "an accepted completion after the lease must discharge it"
        );

        let uncompleted = NormalizedTranscript {
            items: vec![
                receipt(
                    "queue.lease",
                    json!({"message_id": "uncompleted", "attempt": 1, "visible_until_tick": 5}),
                ),
                receipt("queue.clock", json!({"tick": 6})),
            ],
        };
        let mut uncompleted_world = world(&store, &uncompleted);
        uncompleted_world.queue = Some(&queue);
        let violations = BoundedQueueInvariant.check(&uncompleted_world).await;
        assert_eq!(violations.len(), 1);
        assert!(violations[0].detail.contains("no later redelivery"));
    }

    #[tokio::test]
    async fn inv3_reports_expired_unredelivered_lease_and_nonempty_drain() {
        let store = InMemorySessionStore::new();
        let queue = EmptyQueue;
        let transcript = NormalizedTranscript {
            items: vec![
                receipt(
                    "queue.lease",
                    json!({"message_id": "m1", "attempt": 1, "visible_until_tick": 5}),
                ),
                receipt("queue.clock", json!({"tick": 9})),
                receipt("queue.drain.completed", json!({"remaining": 2})),
            ],
        };
        let mut scenario_world = world(&store, &transcript);
        scenario_world.queue = Some(&queue);
        let violations = BoundedQueueInvariant.check(&scenario_world).await;
        assert_eq!(violations.len(), 2);
        assert!(
            violations
                .iter()
                .any(|item| item.detail.contains("no later redelivery"))
        );
        assert!(
            violations
                .iter()
                .any(|item| item.detail.contains("2 message(s)"))
        );
    }

    #[tokio::test]
    async fn inv4_holds_for_one_delivery_per_correlation() {
        let store = InMemorySessionStore::new();
        let events = append(&store, vec![event(20, 1, EventKind::IoEgressDelivered, json!({
            "dedupe_key": "source:0", "egress_kind": "text", "route_id": "route", "attempts": 2
        }))]).await;
        let transcript = transcript_for(&events);
        assert!(
            NoDuplicateProjectedOutputInvariant
                .check(&world(&store, &transcript))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inv4_reports_duplicate_delivery_records_for_one_correlation() {
        let store = InMemorySessionStore::new();
        let events = append(
            &store,
            vec![
                event(
                    21,
                    1,
                    EventKind::IoEgressDelivered,
                    json!({"dedupe_key": "source:0"}),
                ),
                event(
                    22,
                    2,
                    EventKind::IoEgressDelivered,
                    json!({"dedupe_key": "source:0"}),
                ),
            ],
        )
        .await;
        let transcript = transcript_for(&events);
        let violations = NoDuplicateProjectedOutputInvariant
            .check(&world(&store, &transcript))
            .await;
        assert_eq!(violations.len(), 1);
        assert!(violations[0].detail.contains("source:0"));
        assert!(
            violations[0]
                .detail
                .contains("2 published delivery records")
        );
    }

    #[tokio::test]
    async fn inv5_holds_when_recovery_records_progress_after_terminal_resident() {
        let store = InMemorySessionStore::new();
        let events = append(
            &store,
            vec![
                event(
                    31,
                    1,
                    EventKind::CouplingRunFailed,
                    json!({"resident_state": "failed", "reservation_key": "turn-1"}),
                ),
                event(
                    32,
                    2,
                    EventKind::TurnResumed,
                    json!({"reservation_progress": "turn-1"}),
                ),
            ],
        )
        .await;
        let mut transcript = transcript_for(&events);
        transcript.items.insert(
            1,
            receipt("recovery.probe", json!({"reservation_key": "turn-1"})),
        );
        assert!(
            TerminalConsistencyInvariant
                .check(&world(&store, &transcript))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inv5_orders_terminal_and_progress_by_transcript_sequence() {
        let store = InMemorySessionStore::new();
        let events = append(
            &store,
            vec![
                event(
                    34,
                    1,
                    EventKind::CouplingRunFailed,
                    json!({"resident_state": "failed", "reservation_key": "turn-3"}),
                ),
                event(
                    35,
                    1,
                    EventKind::TurnResumed,
                    json!({"reservation_progress": "turn-3"}),
                ),
            ],
        )
        .await;
        let mut transcript = transcript_for(&events);
        transcript.items.insert(
            1,
            receipt("recovery.probe", json!({"reservation_key": "turn-3"})),
        );
        assert!(
            TerminalConsistencyInvariant
                .check(&world(&store, &transcript))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inv5_does_not_apply_an_earlier_probe_to_a_later_terminal_record() {
        let store = InMemorySessionStore::new();
        let events = append(
            &store,
            vec![
                event(
                    36,
                    1,
                    EventKind::CouplingRunFailed,
                    json!({"resident_state": "failed", "reservation_key": "turn-4"}),
                ),
                event(
                    37,
                    1,
                    EventKind::TurnResumed,
                    json!({"reservation_progress": "turn-4"}),
                ),
                event(
                    38,
                    1,
                    EventKind::CouplingRunCompleted,
                    json!({"resident_state": "completed", "reservation_key": "turn-4"}),
                ),
            ],
        )
        .await;
        let mut transcript = transcript_for(&events);
        transcript.items.insert(
            1,
            receipt("recovery.probe", json!({"reservation_key": "turn-4"})),
        );
        assert!(
            TerminalConsistencyInvariant
                .check(&world(&store, &transcript))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn inv5_reports_terminal_resident_without_post_recovery_progress() {
        let store = InMemorySessionStore::new();
        let events = append(
            &store,
            vec![event(
                33,
                1,
                EventKind::CouplingRunCompleted,
                json!({
                    "resident_state": "completed", "reservation_key": "turn-2"
                }),
            )],
        )
        .await;
        let mut transcript = transcript_for(&events);
        transcript.items.push(receipt(
            "recovery.probe",
            json!({"reservation_key": "turn-2"}),
        ));
        let violations = TerminalConsistencyInvariant
            .check(&world(&store, &transcript))
            .await;
        assert_eq!(violations.len(), 1);
        assert!(violations[0].detail.contains("completed resident"));
        assert!(violations[0].detail.contains("turn-2"));
    }
}
