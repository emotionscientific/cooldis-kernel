use super::context_read_plan::{
    context_compile_payload_v1, context_read_plan_set_payload_v1, context_source_ranges,
    context_source_streams, context_summary_completed_payload_v1,
    is_instruction_context_read_plan_event, is_recall_context_read_plan_event,
    primary_context_source_range, render_instruction_context, render_recall_context,
    session_context_source_cut_for_entries,
};
use super::runtime_utils::unix_timestamp_ms;
use super::{CooldisError, CooldisResult, RuntimeKernelControl, TurnInput};
use crate::agent::manifest_bind::BoundCouplingSet;
use crate::kernel::coupling_executor_registry::registered_coupling_executor_supports_template;
use crate::kernel::coupling_scheduler::CouplingScheduler;
use crate::kernel::history::{
    CONTEXT_READ_PLAN_SCHEMA_V1, CanonicalMessage, EventKind, EventProvenance, EventRecord,
    EventStreamId, NewEventRecord, NewObservationRecord, ObservationProvenance, ObservationRecord,
    RuntimeStore, SessionContext, SessionContextSourceCut, SessionEntry, SessionEntryId,
    SessionEntryKind,
};
use crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
use cooldis_runtime_contracts::ThreadCoordinates;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeExecutionPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_grace_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown_grace_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pending_inputs: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_child_threads: Option<usize>,
}

impl RuntimeExecutionPolicy {
    pub fn with_turn_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.turn_timeout_ms = Some(timeout_ms);
        self
    }

    pub fn with_cancel_grace_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.cancel_grace_timeout_ms = Some(timeout_ms);
        self
    }

    pub fn with_shutdown_grace_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.shutdown_grace_timeout_ms = Some(timeout_ms);
        self
    }

    pub fn with_max_pending_inputs(mut self, max_pending_inputs: usize) -> Self {
        self.max_pending_inputs = Some(max_pending_inputs);
        self
    }

    pub fn with_max_child_threads(mut self, max_child_threads: usize) -> Self {
        self.max_child_threads = Some(max_child_threads);
        self
    }
}

#[derive(Clone)]
pub struct RuntimeServices {
    runtime_store: Arc<dyn RuntimeStore>,
    execution_policy: RuntimeExecutionPolicy,
    kernel_control: Option<RuntimeKernelControl>,
    bound_coupling_set: Option<BoundCouplingSet>,
}

impl RuntimeServices {
    pub fn new(
        runtime_store: Arc<dyn RuntimeStore>,
        execution_policy: RuntimeExecutionPolicy,
    ) -> Self {
        Self {
            runtime_store,
            execution_policy,
            kernel_control: None,
            bound_coupling_set: None,
        }
    }

    pub fn with_kernel_control(mut self, kernel_control: RuntimeKernelControl) -> Self {
        self.kernel_control = Some(kernel_control);
        self
    }

    pub fn with_bound_coupling_set(mut self, coupling_set: BoundCouplingSet) -> Self {
        self.bound_coupling_set = Some(coupling_set);
        self
    }

    pub fn runtime_store(&self) -> Arc<dyn RuntimeStore> {
        Arc::clone(&self.runtime_store)
    }

    pub fn execution_policy(&self) -> &RuntimeExecutionPolicy {
        &self.execution_policy
    }

    pub fn kernel_control(&self) -> Option<RuntimeKernelControl> {
        self.kernel_control.clone()
    }

    pub async fn append_session_entry(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> CooldisResult<SessionEntry> {
        self.runtime_store
            .append(coordinates, parent_entry_id, kind)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))
    }

    pub async fn append_user_message(
        &self,
        coordinates: &ThreadCoordinates,
        input: impl Into<String>,
    ) -> CooldisResult<SessionEntry> {
        self.append_session_entry(
            coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text(input.into()),
            },
        )
        .await
    }

    pub async fn append_user_turn_input(
        &self,
        coordinates: &ThreadCoordinates,
        input: &TurnInput,
    ) -> CooldisResult<SessionEntry> {
        self.append_session_entry(
            coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::User {
                    content: input.canonical_content(),
                    timestamp_ms: unix_timestamp_ms() as i64,
                },
            },
        )
        .await
    }

    pub async fn append_thread_event(
        &self,
        coordinates: &ThreadCoordinates,
        record: NewEventRecord,
    ) -> CooldisResult<EventRecord> {
        self.append_event(&EventStreamId::for_thread(coordinates), record)
            .await
    }

    pub async fn append_control_event(
        &self,
        coordinates: &ThreadCoordinates,
        record: NewEventRecord,
    ) -> CooldisResult<EventRecord> {
        self.append_event(
            &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            record,
        )
        .await
    }

    async fn append_event(
        &self,
        stream_id: &EventStreamId,
        record: NewEventRecord,
    ) -> CooldisResult<EventRecord> {
        let appended = self
            .runtime_store
            .append_events(stream_id, vec![record])
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| CooldisError::History("event append returned no record".to_string()))?;
        self.run_bound_stdlib_couplings(vec![appended.clone()])
            .await?;
        Ok(appended)
    }

    async fn run_bound_stdlib_couplings(&self, appended: Vec<EventRecord>) -> CooldisResult<()> {
        if appended.is_empty() {
            return Ok(());
        }
        let Some(coupling_set) = &self.bound_coupling_set else {
            return Ok(());
        };
        let stdlib_couplings = coupling_set
            .couplings
            .iter()
            .filter(|coupling| registered_coupling_executor_supports_template(&coupling.id))
            .cloned()
            .collect::<Vec<_>>();
        if stdlib_couplings.is_empty() {
            return Ok(());
        }
        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(self.runtime_store.as_ref(), &executor);
        scheduler
            .run_batch(
                &BoundCouplingSet::new(coupling_set.snapshot_id.clone(), stdlib_couplings),
                appended,
            )
            .await?;
        Ok(())
    }

    pub async fn build_session_context(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> CooldisResult<SessionContext> {
        self.runtime_store
            .build_context(coordinates)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))
    }

    pub async fn build_recall_read_plan_contexts(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> CooldisResult<Vec<String>> {
        self.build_context_read_plan_contexts(
            coordinates,
            is_recall_context_read_plan_event,
            "memory_checkpoint",
            "memory context",
            render_recall_context,
        )
        .await
    }

    pub async fn build_instruction_read_plan_contexts(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> CooldisResult<Vec<String>> {
        self.build_context_read_plan_contexts(
            coordinates,
            is_instruction_context_read_plan_event,
            "instruction_checkpoint",
            "instruction context",
            render_instruction_context,
        )
        .await
    }

    async fn build_context_read_plan_contexts(
        &self,
        coordinates: &ThreadCoordinates,
        event_filter: fn(&EventRecord) -> bool,
        event_role: &str,
        label: &str,
        render: fn(&[String]) -> String,
    ) -> CooldisResult<Vec<String>> {
        let derived_context_stream =
            EventStreamId::new(format!("derived:context:{}", coordinates.thread_id));
        let events = self
            .runtime_store
            .read_events(&derived_context_stream, None)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        let Some(read_plan_event) = events
            .into_iter()
            .filter(event_filter)
            .max_by_key(|event| event.sequence.get())
        else {
            return Ok(Vec::new());
        };
        let Some(read_plan) = read_plan_event.payload.get("read_plan") else {
            return Err(CooldisError::History(format!(
                "{label} read plan is missing read_plan"
            )));
        };
        if read_plan.get("schema").and_then(|value| value.as_str())
            != Some(CONTEXT_READ_PLAN_SCHEMA_V1)
        {
            return Err(CooldisError::History(format!(
                "{label} read plan has unsupported schema"
            )));
        }
        let source_stream = read_plan
            .get("source_stream")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                CooldisError::History(format!("{label} read plan is missing source_stream"))
            })?;
        let entries = read_plan
            .get("entries")
            .and_then(|value| value.as_array())
            .ok_or_else(|| {
                CooldisError::History(format!("{label} read plan is missing entries"))
            })?;
        let mut seen_event_ids = BTreeSet::new();
        let mut texts = Vec::new();
        for entry in entries {
            if entry.get("kind").and_then(|value| value.as_str()) != Some("event_ref") {
                continue;
            }
            if entry.get("event_role").and_then(|value| value.as_str()) != Some(event_role) {
                continue;
            }
            let event_id = entry
                .get("event_id")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    CooldisError::History(format!(
                        "{label} read plan event_ref is missing event_id"
                    ))
                })?;
            if !seen_event_ids.insert(event_id.to_string()) {
                continue;
            }
            let stream_id = entry
                .get("stream_id")
                .and_then(|value| value.as_str())
                .unwrap_or(source_stream);
            let source_events = self
                .runtime_store
                .read_events(&EventStreamId::new(stream_id.to_string()), None)
                .await
                .map_err(|err| CooldisError::History(err.to_string()))?;
            let event = source_events
                .iter()
                .find(|event| event.id.to_string() == event_id)
                .ok_or_else(|| {
                    CooldisError::History(format!(
                        "{label} read plan referenced missing event {event_id}"
                    ))
                })?;
            if event.kind != EventKind::ContextSummaryCompleted {
                return Err(CooldisError::History(format!(
                    "{label} read plan referenced non-summary event {event_id}"
                )));
            }
            let text = event
                .payload
                .get("text")
                .and_then(|value| value.as_str())
                .filter(|text| !text.trim().is_empty())
                .ok_or_else(|| {
                    CooldisError::History(format!(
                        "{label} read plan referenced summary event {event_id} without text"
                    ))
                })?;
            texts.push(text.to_string());
        }
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![render(&texts)])
    }

    pub async fn record_context_compile_receipt(
        &self,
        coordinates: &ThreadCoordinates,
        session_entries: &[SessionEntry],
        payload: serde_json::Value,
    ) -> CooldisResult<ObservationRecord> {
        let fallback_cut = session_context_source_cut_for_entries(coordinates, session_entries);
        self.record_context_compile_receipt_with_source_cuts(
            coordinates,
            session_entries,
            &fallback_cut,
            payload,
        )
        .await
    }

    pub async fn record_context_compile_receipt_with_source_cuts(
        &self,
        coordinates: &ThreadCoordinates,
        session_entries: &[SessionEntry],
        source_cuts: &[SessionContextSourceCut],
        payload: serde_json::Value,
    ) -> CooldisResult<ObservationRecord> {
        let stream_id = EventStreamId::for_thread(coordinates);
        let source_cuts = if source_cuts.is_empty() {
            session_context_source_cut_for_entries(coordinates, session_entries)
        } else {
            source_cuts.to_vec()
        };
        let source_ranges =
            context_source_ranges(self.runtime_store.as_ref(), &source_cuts).await?;
        let source_range = primary_context_source_range(&stream_id, &source_ranges);
        let source_streams = context_source_streams(&source_cuts, &stream_id);
        let payload =
            context_compile_payload_v1(payload, &stream_id, &source_ranges, &source_streams);
        let event_provenance = EventProvenance {
            source_streams: source_streams.clone(),
            source_range: source_range.clone(),
            source_ranges: source_ranges.clone(),
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("naive_assembly/v1".to_string()),
            ..EventProvenance::default()
        };
        let compile_event = self
            .append_thread_event(
                coordinates,
                NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ContextCompileCompleted,
                    payload.clone(),
                    event_provenance,
                ),
            )
            .await?;
        let observation =
            NewObservationRecord::new("compiled_context_receipt", coordinates.clone(), payload)
                .with_provenance(ObservationProvenance {
                    source_streams,
                    source_event_ids: vec![compile_event.id],
                    source_range,
                    source_ranges,
                    derivation_strategy: "naive_assembly".to_string(),
                    derivation_version: "v1".to_string(),
                });
        self.runtime_store
            .append_observation(observation)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))
    }

    pub async fn record_context_summary_checkpoint(
        &self,
        coordinates: &ThreadCoordinates,
        session_entries: &[SessionEntry],
        source_cuts: &[SessionContextSourceCut],
        summary: &str,
    ) -> CooldisResult<(EventRecord, EventRecord)> {
        let stream_id = EventStreamId::for_thread(coordinates);
        let source_cuts = if source_cuts.is_empty() {
            session_context_source_cut_for_entries(coordinates, session_entries)
        } else {
            source_cuts.to_vec()
        };
        let source_ranges =
            context_source_ranges(self.runtime_store.as_ref(), &source_cuts).await?;
        let source_range = primary_context_source_range(&stream_id, &source_ranges);
        let source_streams = context_source_streams(&source_cuts, &stream_id);
        let summary_event = self
            .runtime_store
            .append_events(
                &stream_id,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ContextSummaryCompleted,
                    context_summary_completed_payload_v1(summary, &source_ranges),
                    EventProvenance {
                        source_streams: source_streams.clone(),
                        source_range: source_range.clone(),
                        source_ranges: source_ranges.clone(),
                        discharged_by: Some("projection:context-summarizer".to_string()),
                        function: Some("context_summary/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                CooldisError::History("context summary event append returned no record".to_string())
            })?;
        let read_plan_event = self
            .runtime_store
            .append_events(
                &stream_id,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ContextReadPlanSet,
                    context_read_plan_set_payload_v1(
                        "history.default",
                        &stream_id,
                        summary_event.id,
                        &source_ranges,
                    ),
                    EventProvenance {
                        source_streams: vec![stream_id.clone()],
                        source_event_ids: vec![summary_event.id],
                        source_range,
                        source_ranges,
                        discharged_by: Some("controller:context-budget".to_string()),
                        function: Some("context_read_plan/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                CooldisError::History(
                    "context read plan event append returned no record".to_string(),
                )
            })?;
        Ok((summary_event, read_plan_event))
    }
}
