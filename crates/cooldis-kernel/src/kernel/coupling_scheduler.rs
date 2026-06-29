use crate::{
    BoundCoupling, BoundCouplingSet, CooldisError, CooldisResult, EventKind, EventOrigin,
    EventProvenance, EventRecord, EventRecordId, EventStore, EventStreamId, NewEventRecord,
    ThreadCoordinates,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingSchedulerConfig {
    pub max_depth: u32,
    pub max_discharge_events_per_cycle: u32,
}

impl Default for CouplingSchedulerConfig {
    fn default() -> Self {
        Self {
            max_depth: 8,
            max_discharge_events_per_cycle: 128,
        }
    }
}

pub struct CouplingScheduler<'a, S: ?Sized, E> {
    store: &'a S,
    executor: &'a E,
    config: CouplingSchedulerConfig,
}

impl<'a, S, E> CouplingScheduler<'a, S, E>
where
    S: EventStore + ?Sized,
    E: CouplingExecutor,
{
    pub fn new(store: &'a S, executor: &'a E) -> Self {
        Self::with_config(store, executor, CouplingSchedulerConfig::default())
    }

    pub fn with_config(store: &'a S, executor: &'a E, config: CouplingSchedulerConfig) -> Self {
        Self {
            store,
            executor,
            config,
        }
    }

    pub async fn run_batch(
        &self,
        coupling_set: &BoundCouplingSet,
        appended: Vec<EventRecord>,
    ) -> CooldisResult<CouplingSchedulerCycleReceipt> {
        let mut seen = BTreeSet::new();
        let mut queue = VecDeque::new();
        self.enqueue_matches(
            coupling_set,
            appended,
            &mut seen,
            &mut queue,
            RootDepth::FromEvent,
        );

        let mut runs = Vec::new();
        let mut appended_events = Vec::new();
        let mut run_counts = HashMap::<String, u32>::new();
        let mut remaining_discharge_budget = self.config.max_discharge_events_per_cycle;
        while let Some(queued) = queue.pop_front() {
            if queued.activation.depth > self.config.max_depth {
                let (run, receipt) = self
                    .append_run_receipt(
                        &queued,
                        CouplingRunStatus::Skipped,
                        Some("depth_limit_exhausted".to_string()),
                        CouplingSourceCut::default(),
                        Vec::new(),
                        Vec::new(),
                    )
                    .await?;
                self.enqueue_matches(
                    coupling_set,
                    receipt.clone(),
                    &mut seen,
                    &mut queue,
                    RootDepth::Inherited {
                        root_event_id: queued.activation.root_event_id,
                        depth: queued.activation.depth + 1,
                    },
                );
                appended_events.extend(receipt);
                runs.push(run);
                continue;
            }
            if quota_exhausted(&queued.coupling, &run_counts) {
                let (run, receipt) = self
                    .append_run_receipt(
                        &queued,
                        CouplingRunStatus::Skipped,
                        Some("quota_exhausted".to_string()),
                        CouplingSourceCut::default(),
                        Vec::new(),
                        Vec::new(),
                    )
                    .await?;
                self.enqueue_matches(
                    coupling_set,
                    receipt.clone(),
                    &mut seen,
                    &mut queue,
                    RootDepth::Inherited {
                        root_event_id: queued.activation.root_event_id,
                        depth: queued.activation.depth + 1,
                    },
                );
                appended_events.extend(receipt);
                runs.push(run);
                continue;
            }
            *run_counts.entry(queued.coupling.id.clone()).or_default() += 1;

            let (source_cut, source_events) = match self
                .resolve_source_cut(&queued.coupling, &queued.trigger_event)
                .await
            {
                Ok(source) => source,
                Err(err) => {
                    let (run, receipt) = self
                        .append_run_receipt(
                            &queued,
                            CouplingRunStatus::Failed,
                            Some(err.to_string()),
                            CouplingSourceCut::default(),
                            Vec::new(),
                            Vec::new(),
                        )
                        .await?;
                    self.enqueue_matches(
                        coupling_set,
                        receipt.clone(),
                        &mut seen,
                        &mut queue,
                        RootDepth::Inherited {
                            root_event_id: queued.activation.root_event_id,
                            depth: queued.activation.depth + 1,
                        },
                    );
                    appended_events.extend(receipt);
                    runs.push(run);
                    continue;
                }
            };

            let request = CouplingInvocation {
                activation: queued.activation.clone(),
                coupling: queued.coupling.clone(),
                trigger_event: queued.trigger_event.clone(),
                source_cut: source_cut.clone(),
                source_events: source_events.clone(),
            };
            let execution = self.executor.invoke(request).await;
            let execution = match execution {
                Ok(execution) => execution,
                Err(err) => {
                    let (run, receipt) = self
                        .append_run_receipt(
                            &queued,
                            CouplingRunStatus::Failed,
                            Some(err.to_string()),
                            source_cut,
                            source_events,
                            Vec::new(),
                        )
                        .await?;
                    self.enqueue_matches(
                        coupling_set,
                        receipt.clone(),
                        &mut seen,
                        &mut queue,
                        RootDepth::Inherited {
                            root_event_id: queued.activation.root_event_id,
                            depth: queued.activation.depth + 1,
                        },
                    );
                    appended_events.extend(receipt);
                    runs.push(run);
                    continue;
                }
            };

            if let Err(reason) = validate_discharges(
                &queued.coupling,
                &execution.discharges,
                remaining_discharge_budget,
            ) {
                let (run, receipt) = self
                    .append_run_receipt(
                        &queued,
                        CouplingRunStatus::Failed,
                        Some(reason),
                        source_cut,
                        source_events,
                        Vec::new(),
                    )
                    .await?;
                self.enqueue_matches(
                    coupling_set,
                    receipt.clone(),
                    &mut seen,
                    &mut queue,
                    RootDepth::Inherited {
                        root_event_id: queued.activation.root_event_id,
                        depth: queued.activation.depth + 1,
                    },
                );
                appended_events.extend(receipt);
                runs.push(run);
                continue;
            }

            let sink_events = self
                .append_sink_events(&queued, &source_cut, &source_events, execution.discharges)
                .await?;
            remaining_discharge_budget =
                remaining_discharge_budget.saturating_sub(sink_events.len() as u32);
            let discharged_event_ids = sink_events.iter().map(|event| event.id).collect::<Vec<_>>();
            let (run, receipt) = self
                .append_run_receipt(
                    &queued,
                    CouplingRunStatus::Completed,
                    None,
                    source_cut,
                    source_events,
                    discharged_event_ids,
                )
                .await?;
            let generated = sink_events
                .iter()
                .cloned()
                .chain(receipt.iter().cloned())
                .collect::<Vec<_>>();
            self.enqueue_matches(
                coupling_set,
                generated.clone(),
                &mut seen,
                &mut queue,
                RootDepth::Inherited {
                    root_event_id: queued.activation.root_event_id,
                    depth: queued.activation.depth + 1,
                },
            );
            appended_events.extend(sink_events);
            appended_events.extend(receipt);
            runs.push(run);
        }

        Ok(CouplingSchedulerCycleReceipt {
            snapshot_id: coupling_set.snapshot_id.clone(),
            runs,
            appended_events,
        })
    }

    pub fn stream_id_for(&self, coordinates: &ThreadCoordinates, stream: &str) -> EventStreamId {
        stream_id_for(coordinates, stream)
    }

    fn enqueue_matches(
        &self,
        coupling_set: &BoundCouplingSet,
        events: Vec<EventRecord>,
        seen: &mut BTreeSet<ActivationKey>,
        queue: &mut VecDeque<QueuedActivation>,
        root_depth: RootDepth,
    ) {
        let mut candidates = Vec::new();
        for (batch_index, event) in events.into_iter().enumerate() {
            let (root_event_id, depth) = match root_depth {
                RootDepth::FromEvent => root_depth_from_event(&event),
                RootDepth::Inherited {
                    root_event_id,
                    depth,
                } => (root_event_id, depth),
            };
            for coupling in &coupling_set.couplings {
                if !coupling_matches_event(coupling, &event) {
                    continue;
                }
                let activation = CouplingActivation {
                    root_event_id,
                    trigger_event_id: event.id,
                    trigger_stream_id: event.stream_id.to_string(),
                    trigger_sequence: event.sequence.get(),
                    coupling_id: coupling.id.clone(),
                    depth,
                    snapshot_id: coupling_set.snapshot_id.clone(),
                };
                let key = ActivationKey::from_activation(&activation);
                if !seen.insert(key) {
                    continue;
                }
                candidates.push(QueuedActivation {
                    batch_index,
                    activation,
                    trigger_event: event.clone(),
                    coupling: coupling.clone(),
                });
            }
        }
        candidates.sort_by(|left, right| {
            (
                left.batch_index,
                left.activation.trigger_stream_id.as_str(),
                left.activation.trigger_sequence,
                left.activation.coupling_id.as_str(),
            )
                .cmp(&(
                    right.batch_index,
                    right.activation.trigger_stream_id.as_str(),
                    right.activation.trigger_sequence,
                    right.activation.coupling_id.as_str(),
                ))
        });
        queue.extend(candidates);
    }

    async fn resolve_source_cut(
        &self,
        coupling: &BoundCoupling,
        trigger_event: &EventRecord,
    ) -> CooldisResult<(CouplingSourceCut, Vec<EventRecord>)> {
        let mut entries = BTreeMap::<String, i64>::new();
        let mut selected = Vec::new();
        let mut seen_event_ids = HashSet::new();
        for selector in &coupling.source_selectors {
            if !has_stream_grant(&coupling.grants, "read", &selector.stream) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "coupling {:?} is missing stream.read grant for {:?}",
                    coupling.id, selector.stream
                )));
            }
            let stream_id = stream_id_for(&trigger_event.coordinates, &selector.stream);
            let events = self
                .store
                .read_events(&stream_id, None)
                .await
                .map_err(|err| CooldisError::History(err.to_string()))?;
            let max_sequence = events
                .iter()
                .map(|event| event.sequence.get())
                .max()
                .unwrap_or(0);
            entries
                .entry(stream_id.to_string())
                .and_modify(|existing| *existing = (*existing).max(max_sequence))
                .or_insert(max_sequence);
            for event in events
                .into_iter()
                .filter(|event| selector.kinds.contains(&event.kind))
            {
                if seen_event_ids.insert(event.id) {
                    selected.push(event);
                }
            }
        }
        selected.sort_by(|left, right| {
            (
                left.stream_id.to_string(),
                left.sequence.get(),
                left.id.to_string(),
            )
                .cmp(&(
                    right.stream_id.to_string(),
                    right.sequence.get(),
                    right.id.to_string(),
                ))
        });
        Ok((
            CouplingSourceCut {
                entries: entries
                    .into_iter()
                    .map(|(stream_id, max_sequence)| CouplingSourceCutEntry {
                        stream_id,
                        max_sequence,
                    })
                    .collect(),
            },
            selected,
        ))
    }

    async fn append_sink_events(
        &self,
        queued: &QueuedActivation,
        source_cut: &CouplingSourceCut,
        source_events: &[EventRecord],
        discharges: Vec<CouplingDischarge>,
    ) -> CooldisResult<Vec<EventRecord>> {
        if discharges.is_empty() {
            return Ok(Vec::new());
        }
        let mut records_by_stream = BTreeMap::<String, Vec<NewEventRecord>>::new();
        for discharge in discharges {
            let provenance = event_provenance(
                &queued.activation,
                &queued.coupling,
                source_cut,
                source_events,
            );
            let mut record = NewEventRecord::discharged(
                queued.trigger_event.coordinates.clone(),
                discharge.kind,
                discharge.payload,
                provenance,
            );
            if let Some(event_id) = discharge.event_id {
                record.id = event_id;
            }
            records_by_stream
                .entry(discharge.stream.clone())
                .or_default()
                .push(record);
        }
        let mut appended = Vec::new();
        for (stream, records) in records_by_stream {
            let stream_id = stream_id_for(&queued.trigger_event.coordinates, &stream);
            let mut events = self
                .store
                .append_events(&stream_id, records)
                .await
                .map_err(|err| CooldisError::History(err.to_string()))?;
            appended.append(&mut events);
        }
        Ok(appended)
    }

    async fn append_run_receipt(
        &self,
        queued: &QueuedActivation,
        status: CouplingRunStatus,
        reason: Option<String>,
        source_cut: CouplingSourceCut,
        source_events: Vec<EventRecord>,
        discharged_event_ids: Vec<EventRecordId>,
    ) -> CooldisResult<(CouplingRunReceipt, Vec<EventRecord>)> {
        let source_event_ids = source_events
            .iter()
            .map(|event| event.id)
            .collect::<Vec<_>>();
        let discharge_events = discharged_event_ids.len() as u32;
        let run = CouplingRunReceipt {
            coupling_id: queued.coupling.id.clone(),
            role: queued.coupling.role,
            status,
            reason,
            root_event_id: queued.activation.root_event_id,
            trigger_event_id: queued.activation.trigger_event_id,
            trigger_stream_id: queued.activation.trigger_stream_id.clone(),
            trigger_sequence: queued.activation.trigger_sequence,
            snapshot_id: queued.activation.snapshot_id.clone(),
            depth: queued.activation.depth,
            source_cut,
            source_event_ids,
            discharged_event_ids,
            function_ref: queued.coupling.function_ref.clone(),
            config_hash: queued.coupling.config_hash.clone(),
            budget_spent: CouplingBudgetSpent { discharge_events },
        };
        let payload = serde_json::to_value(&run).map_err(|err| {
            CooldisError::History(format!("coupling run receipt codec failed: {err}"))
        })?;
        let kind = match status {
            CouplingRunStatus::Completed => EventKind::CouplingRunCompleted,
            CouplingRunStatus::Failed | CouplingRunStatus::Skipped => EventKind::CouplingRunFailed,
        };
        let provenance = event_provenance(
            &queued.activation,
            &queued.coupling,
            &run.source_cut,
            &source_events,
        );
        let stream_id = stream_id_for(&queued.trigger_event.coordinates, "control");
        let appended = self
            .store
            .append_events(
                &stream_id,
                vec![NewEventRecord::discharged(
                    queued.trigger_event.coordinates.clone(),
                    kind,
                    payload,
                    provenance,
                )],
            )
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        Ok((run, appended))
    }
}

#[async_trait]
pub trait CouplingExecutor: Send + Sync {
    async fn invoke(&self, request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct CouplingInvocation {
    pub activation: CouplingActivation,
    pub coupling: BoundCoupling,
    pub trigger_event: EventRecord,
    pub source_cut: CouplingSourceCut,
    pub source_events: Vec<EventRecord>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CouplingExecutionResult {
    pub discharges: Vec<CouplingDischarge>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CouplingDischarge {
    pub event_id: Option<EventRecordId>,
    pub stream: String,
    pub kind: EventKind,
    pub payload: JsonValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CouplingSchedulerCycleReceipt {
    pub snapshot_id: String,
    pub runs: Vec<CouplingRunReceipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub appended_events: Vec<EventRecord>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CouplingRunReceipt {
    pub coupling_id: String,
    pub role: crate::CouplingRole,
    pub status: CouplingRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub root_event_id: EventRecordId,
    pub trigger_event_id: EventRecordId,
    pub trigger_stream_id: String,
    pub trigger_sequence: i64,
    pub snapshot_id: String,
    pub depth: u32,
    pub source_cut: CouplingSourceCut,
    pub source_event_ids: Vec<EventRecordId>,
    pub discharged_event_ids: Vec<EventRecordId>,
    pub function_ref: String,
    pub config_hash: String,
    pub budget_spent: CouplingBudgetSpent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CouplingRunStatus {
    Completed,
    Failed,
    Skipped,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingBudgetSpent {
    pub discharge_events: u32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingSourceCut {
    pub entries: Vec<CouplingSourceCutEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingSourceCutEntry {
    pub stream_id: String,
    pub max_sequence: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingActivation {
    pub root_event_id: EventRecordId,
    pub trigger_event_id: EventRecordId,
    pub trigger_stream_id: String,
    pub trigger_sequence: i64,
    pub coupling_id: String,
    pub depth: u32,
    pub snapshot_id: String,
}

#[derive(Clone, Debug)]
struct QueuedActivation {
    batch_index: usize,
    activation: CouplingActivation,
    trigger_event: EventRecord,
    coupling: BoundCoupling,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ActivationKey {
    trigger_event_id: String,
    coupling_id: String,
    snapshot_id: String,
}

impl ActivationKey {
    fn from_activation(activation: &CouplingActivation) -> Self {
        Self {
            trigger_event_id: activation.trigger_event_id.to_string(),
            coupling_id: activation.coupling_id.clone(),
            snapshot_id: activation.snapshot_id.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RootDepth {
    FromEvent,
    Inherited {
        root_event_id: EventRecordId,
        depth: u32,
    },
}

fn root_depth_from_event(event: &EventRecord) -> (EventRecordId, u32) {
    match event.origin {
        EventOrigin::Witnessed => (event.id, 0),
        EventOrigin::Discharged => (
            event
                .provenance
                .source_event_ids
                .first()
                .copied()
                .unwrap_or(event.id),
            1,
        ),
    }
}

fn coupling_matches_event(coupling: &BoundCoupling, event: &EventRecord) -> bool {
    coupling.trigger_kind == event.kind
        && coupling
            .trigger_match
            .iter()
            .all(|(key, expected)| event.payload.get(key) == Some(expected))
}

fn quota_exhausted(coupling: &BoundCoupling, run_counts: &HashMap<String, u32>) -> bool {
    let count = run_counts.get(&coupling.id).copied().unwrap_or_default();
    [
        coupling.trigger_quota.per_turn,
        coupling.trigger_quota.per_thread,
    ]
    .into_iter()
    .flatten()
    .any(|limit| count >= limit)
}

fn validate_discharges(
    coupling: &BoundCoupling,
    discharges: &[CouplingDischarge],
    remaining_discharge_budget: u32,
) -> Result<(), String> {
    if let Some(limit) = coupling.budget.max_discharge_events
        && discharges.len() > limit as usize
    {
        return Err(format!(
            "coupling {:?} exceeded max_discharge_events budget",
            coupling.id
        ));
    }
    if discharges.len() > remaining_discharge_budget as usize {
        return Err(format!(
            "coupling {:?} exceeded scheduler discharge budget",
            coupling.id
        ));
    }
    if !discharges.is_empty() && !has_stream_grant(&coupling.grants, "write", &coupling.sink.stream)
    {
        return Err(format!(
            "coupling {:?} is missing stream.write grant for {:?}",
            coupling.id, coupling.sink.stream
        ));
    }
    for discharge in discharges {
        if discharge.stream != coupling.sink.stream {
            return Err(format!(
                "coupling {:?} cannot discharge to sink stream {:?}; bound sink is {:?}",
                coupling.id, discharge.stream, coupling.sink.stream
            ));
        }
        if !coupling.sink.kinds.contains(&discharge.kind) {
            return Err(format!(
                "coupling {:?} cannot discharge sink kind {:?}",
                coupling.id, discharge.kind
            ));
        }
    }
    Ok(())
}

fn has_stream_grant(grants: &[String], action: &str, stream: &str) -> bool {
    let exact = format!("stream.{action}:{stream}");
    let wildcard = format!("stream.{action}:*");
    grants
        .iter()
        .any(|grant| grant == &exact || grant == &wildcard || grant == "stream.*:*")
}

fn event_provenance(
    activation: &CouplingActivation,
    coupling: &BoundCoupling,
    source_cut: &CouplingSourceCut,
    source_events: &[EventRecord],
) -> EventProvenance {
    let source_streams = if source_cut.entries.is_empty() {
        vec![EventStreamId::new(activation.trigger_stream_id.clone())]
    } else {
        source_cut
            .entries
            .iter()
            .map(|entry| EventStreamId::new(entry.stream_id.clone()))
            .collect()
    };
    let source_event_ids = if source_events.is_empty() {
        vec![activation.trigger_event_id]
    } else {
        source_events.iter().map(|event| event.id).collect()
    };
    EventProvenance {
        source_streams,
        source_event_ids,
        discharged_by: Some(format!("coupling:{}", coupling.id)),
        function: Some(coupling.function_ref.clone()),
        config_hash: Some(coupling.config_hash.clone()),
        ..EventProvenance::default()
    }
}

fn stream_id_for(coordinates: &ThreadCoordinates, stream: &str) -> EventStreamId {
    if stream == "thread" {
        EventStreamId::for_thread(coordinates)
    } else {
        EventStreamId::new(format!("{stream}:{}", coordinates.thread_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BoundCoupling, BoundCouplingFunction, BoundCouplingSelector, BoundCouplingSet,
        BoundCouplingSink, CouplingRole, EventKind, EventStore, EventStreamId,
        InMemorySessionStore, NewEventRecord, ThreadCoordinates,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct RecordingExecutor {
        calls: Arc<Mutex<Vec<CouplingInvocation>>>,
        discharges: Vec<CouplingDischarge>,
        fail: Option<String>,
    }

    #[async_trait]
    impl CouplingExecutor for RecordingExecutor {
        async fn invoke(
            &self,
            request: CouplingInvocation,
        ) -> crate::CooldisResult<CouplingExecutionResult> {
            self.calls.lock().unwrap().push(request);
            if let Some(message) = &self.fail {
                return Err(crate::CooldisError::RuntimeFactory(message.clone()));
            }
            Ok(CouplingExecutionResult {
                discharges: self.discharges.clone(),
            })
        }
    }

    #[tokio::test]
    async fn witnessed_event_starts_deterministic_activation_order_and_source_cut() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let appended = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({"turn_id": "t1"}),
                )],
            )
            .await
            .unwrap();
        let executor = RecordingExecutor::default();
        let scheduler = CouplingScheduler::new(&store, &executor);
        let coupling_set = BoundCouplingSet::new(
            "snapshot-a",
            vec![
                test_coupling("b_gate", EventKind::TurnCompleted, "control"),
                test_coupling("a_gate", EventKind::TurnCompleted, "control"),
            ],
        );

        let receipt = scheduler.run_batch(&coupling_set, appended).await.unwrap();

        assert_eq!(
            receipt
                .runs
                .iter()
                .map(|run| run.coupling_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a_gate", "b_gate"]
        );
        let calls = executor.calls.lock().unwrap();
        assert_eq!(
            calls[0].activation.root_event_id,
            calls[0].activation.trigger_event_id
        );
        assert_eq!(calls[0].activation.depth, 0);
        assert_eq!(
            calls[0].source_cut.entries,
            vec![CouplingSourceCutEntry {
                stream_id: thread_stream.to_string(),
                max_sequence: 1,
            }]
        );
    }

    #[tokio::test]
    async fn discharged_event_triggers_next_coupling_and_inherits_root() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let appended = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({}),
                )],
            )
            .await
            .unwrap();
        let first = test_coupling("extract", EventKind::TurnCompleted, "derived:memory");
        let second = test_coupling("route", EventKind::PlacementDecision, "control");
        let executor = RecordingExecutor {
            discharges: vec![CouplingDischarge {
                event_id: None,
                stream: "derived:memory".to_string(),
                kind: EventKind::PlacementDecision,
                payload: json!({"placement": "local"}),
            }],
            ..RecordingExecutor::default()
        };
        let scheduler = CouplingScheduler::with_config(
            &store,
            &executor,
            CouplingSchedulerConfig {
                max_depth: 2,
                ..CouplingSchedulerConfig::default()
            },
        );

        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![first, second]),
                appended,
            )
            .await
            .unwrap();

        assert!(
            receipt
                .runs
                .iter()
                .any(|run| run.coupling_id == "route" && run.depth == 1)
        );
        let calls = executor.calls.lock().unwrap();
        let root = calls[0].activation.root_event_id;
        let chained = calls
            .iter()
            .find(|call| call.activation.coupling_id == "route")
            .unwrap();
        assert_eq!(chained.activation.root_event_id, root);
        assert_eq!(chained.activation.depth, 1);
    }

    #[tokio::test]
    async fn invalid_sink_discharge_records_failure_without_partial_events() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let appended = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({}),
                )],
            )
            .await
            .unwrap();
        let executor = RecordingExecutor {
            discharges: vec![CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: EventKind::LoopCompleted,
                payload: json!({}),
            }],
            ..RecordingExecutor::default()
        };
        let scheduler = CouplingScheduler::new(&store, &executor);
        let coupling = test_coupling("gate", EventKind::TurnCompleted, "control");

        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![coupling]),
                appended,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs[0].status, CouplingRunStatus::Failed);
        assert!(
            receipt.runs[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("sink kind")
        );
        let control_events = store
            .read_events(&scheduler.stream_id_for(&coordinates, "control"), None)
            .await
            .unwrap();
        assert_eq!(control_events.len(), 1);
        assert_eq!(control_events[0].kind, EventKind::CouplingRunFailed);
    }

    #[tokio::test]
    async fn cyclic_trigger_graph_halts_by_depth_with_receipt() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let appended = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({}),
                )],
            )
            .await
            .unwrap();
        let executor = RecordingExecutor {
            discharges: vec![CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: EventKind::TurnCompleted,
                payload: json!({}),
            }],
            ..RecordingExecutor::default()
        };
        let scheduler = CouplingScheduler::with_config(
            &store,
            &executor,
            CouplingSchedulerConfig {
                max_depth: 1,
                ..CouplingSchedulerConfig::default()
            },
        );
        let coupling = test_coupling("loop_gate", EventKind::TurnCompleted, "control");

        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![coupling]),
                appended,
            )
            .await
            .unwrap();

        assert_eq!(
            receipt
                .runs
                .iter()
                .filter(|run| run.status == CouplingRunStatus::Completed)
                .count(),
            2
        );
        assert!(
            receipt
                .runs
                .iter()
                .any(|run| run.status == CouplingRunStatus::Skipped
                    && run.reason.as_deref() == Some("depth_limit_exhausted"))
        );
    }

    #[tokio::test]
    async fn empty_durable_batch_does_not_trigger_runtime_telemetry() {
        let store = InMemorySessionStore::default();
        let executor = RecordingExecutor::default();
        let scheduler = CouplingScheduler::new(&store, &executor);

        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![test_coupling("gate", EventKind::TurnCompleted, "control")],
                ),
                Vec::new(),
            )
            .await
            .unwrap();

        assert!(receipt.runs.is_empty());
        assert!(executor.calls.lock().unwrap().is_empty());
    }

    fn test_coupling(id: &str, trigger_kind: EventKind, sink_stream: &str) -> BoundCoupling {
        BoundCoupling {
            id: id.to_string(),
            role: if sink_stream == "control" {
                CouplingRole::Controller
            } else {
                CouplingRole::Projection
            },
            trigger_kind,
            trigger_match: Default::default(),
            trigger_quota: Default::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: sink_stream.to_string(),
                kinds: vec![EventKind::PlacementDecision, EventKind::TurnCompleted],
            },
            function_ref: format!("op://{id}/run@sha256:{}", "a".repeat(64)),
            function: BoundCouplingFunction {
                name: id.to_string(),
                artifact_hash: "a".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                format!("stream.write:{sink_stream}"),
            ],
            budget: Default::default(),
            config: json!({}),
            config_hash: "sha256:test".to_string(),
        }
    }
}
