#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
    runtime_store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
    execution_policy: RuntimeExecutionPolicy,
    kernel_control: Option<crate::kernel::runtime_host::kernel_control::RuntimeKernelControl>,
    process_handle_ingress: Option<
        std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink>,
    >,
    process_handle_dispatcher:
        Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher>,
    bound_coupling_set: Option<crate::agent::manifest_bind::BoundCouplingSet>,
    operation_registry_root: Option<std::path::PathBuf>,
    turn_watchdog_sequence: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl RuntimeServices {
    pub fn new(
        runtime_store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
        execution_policy: RuntimeExecutionPolicy,
    ) -> Self {
        Self {
            runtime_store,
            execution_policy,
            kernel_control: None,
            process_handle_ingress: None,
            process_handle_dispatcher: None,
            bound_coupling_set: None,
            operation_registry_root: None,
            turn_watchdog_sequence: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn with_kernel_control(
        mut self,
        kernel_control: crate::kernel::runtime_host::kernel_control::RuntimeKernelControl,
    ) -> Self {
        self.kernel_control = Some(kernel_control);
        self
    }

    pub(crate) fn with_process_handle_dispatcher(
        mut self,
        dispatcher: Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher>,
    ) -> Self {
        self.process_handle_dispatcher = dispatcher;
        self
    }

    pub fn with_process_handle_ingress(
        mut self,
        sink: Option<
            std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink>,
        >,
    ) -> Self {
        self.process_handle_ingress = sink;
        self
    }

    pub fn process_handle_ingress(
        &self,
    ) -> Option<
        std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink>,
    > {
        self.process_handle_ingress.clone()
    }

    pub(crate) fn process_handle_dispatcher(
        &self,
    ) -> Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher> {
        self.process_handle_dispatcher.clone()
    }

    pub fn with_bound_coupling_set(
        mut self,
        coupling_set: crate::agent::manifest_bind::BoundCouplingSet,
    ) -> Self {
        self.bound_coupling_set = Some(coupling_set);
        self
    }

    pub fn with_operation_registry_root(mut self, root: impl Into<std::path::PathBuf>) -> Self {
        self.operation_registry_root = Some(root.into());
        self
    }

    pub fn runtime_store(&self) -> std::sync::Arc<dyn verlet_history::RuntimeStore> {
        std::sync::Arc::clone(&self.runtime_store)
    }

    pub fn execution_policy(&self) -> &RuntimeExecutionPolicy {
        &self.execution_policy
    }

    pub fn kernel_control(
        &self,
    ) -> Option<crate::kernel::runtime_host::kernel_control::RuntimeKernelControl> {
        self.kernel_control.clone()
    }

    pub(super) fn register_turn_watchdog(
        &self,
        input: &mut crate::kernel::runtime_host::turn::TurnInput,
    ) -> crate::kernel::runtime_host::turn::TurnWatchdogHandle {
        let token_id = self
            .turn_watchdog_sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .wrapping_add(1);
        let (token, handle) = crate::kernel::runtime_host::turn::TurnWatchdogToken::new(token_id);
        input.set_turn_watchdog(token);
        handle
    }

    pub async fn append_session_entry(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionEntry> {
        self.runtime_store
            .append(coordinates, parent_entry_id, kind)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn append_session_entry_with_provenance(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
        provenance: verlet_history::EventProvenance,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionEntry> {
        self.runtime_store
            .append_with_provenance(coordinates, parent_entry_id, kind, provenance)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn append_agent_loop_session_entry(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
        source_event_ids: Vec<verlet_history::EventRecordId>,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionEntry> {
        if source_event_ids.is_empty() {
            return Err(crate::kernel::runtime_host::VerletError::History(
                "agent-loop session entry requires source_event_ids".to_string(),
            ));
        }
        self.append_session_entry_with_provenance(
            coordinates,
            parent_entry_id,
            kind,
            verlet_history::EventProvenance {
                source_streams: vec![verlet_history::EventStreamId::for_thread(coordinates)],
                source_event_ids,
                discharged_by: Some("propagator:agent-loop".to_string()),
                function: Some("session_entry_append/v1".to_string()),
                ..verlet_history::EventProvenance::default()
            },
        )
        .await
    }

    pub async fn append_user_message(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        input: impl Into<String>,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionEntry> {
        self.append_session_entry(
            coordinates,
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text(input.into()),
            },
        )
        .await
    }

    pub async fn append_user_turn_input(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: &str,
        input: &crate::kernel::runtime_host::turn::TurnInput,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionEntry> {
        input.start_turn_watchdog();
        self.runtime_store
            .append_turn_input(
                coordinates,
                turn_id,
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::User {
                        content: input.canonical_content(),
                        timestamp_ms: crate::kernel::runtime_host::runtime_utils::unix_timestamp_ms(
                        ) as i64,
                    },
                },
            )
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn append_thread_event(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        record: verlet_history::NewEventRecord,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
        self.append_event(
            &verlet_history::EventStreamId::for_thread(coordinates),
            record,
        )
        .await
    }

    pub(crate) async fn append_thread_events(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
        let expected = records.len();
        let appended = self
            .runtime_store
            .append_events(
                &verlet_history::EventStreamId::for_thread(coordinates),
                records,
            )
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        if appended.len() != expected {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "event batch append returned {} of {expected} records",
                appended.len()
            )));
        }
        self.run_bound_couplings(appended.clone()).await?;
        Ok(appended)
    }

    pub async fn append_control_event(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        record: verlet_history::NewEventRecord,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
        self.append_event(
            &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            record,
        )
        .await
    }

    pub async fn append_thread_joined_event_if_spawned(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
        terminal_state: verlet_history::ThreadTerminalState,
        result_digest: Option<String>,
        source_event_id: Option<verlet_history::EventRecordId>,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::EventRecord>> {
        let Some(parent_thread_id) = context.parent_thread_id else {
            return Ok(None);
        };
        let mut parent_coordinates = context.coordinates.clone();
        parent_coordinates.thread_id = parent_thread_id;
        let parent_control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", parent_coordinates.thread_id));
        let control_events = self
            .runtime_store
            .read_events(&parent_control_stream, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        let child_thread_id = context.coordinates.thread_id.to_string();
        let Some(spawned) = control_events
            .iter()
            .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawned)
            .filter(|event| {
                event
                    .payload
                    .get("child_thread_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(child_thread_id.as_str())
            })
            .max_by_key(|event| event.sequence.get())
            .cloned()
        else {
            return Ok(None);
        };
        let source_event = source_event_id.map(|event_id| {
            (
                verlet_history::EventStreamId::for_thread(&context.coordinates),
                event_id,
            )
        });
        let joined = append_thread_joined_first_wins(
            self.runtime_store.as_ref(),
            parent_coordinates,
            context.coordinates.clone(),
            spawned.id,
            terminal_state,
            result_digest,
            None,
            source_event,
            "runtime:thread-lifecycle",
            "thread_join/v1",
        )
        .await?;
        Ok(Some(joined.record))
    }

    async fn append_event(
        &self,
        stream_id: &verlet_history::EventStreamId,
        record: verlet_history::NewEventRecord,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
        let appended = self
            .runtime_store
            .append_events(stream_id, vec![record])
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(
                    "event append returned no record".to_string(),
                )
            })?;
        self.run_bound_couplings(vec![appended.clone()]).await?;
        Ok(appended)
    }

    async fn run_bound_couplings(
        &self,
        appended: Vec<verlet_history::EventRecord>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if appended.is_empty() {
            return Ok(());
        }
        let Some(coupling_set) = &self.bound_coupling_set else {
            return Ok(());
        };
        let executable_couplings = coupling_set
            .couplings
            .iter()
            .filter(|coupling| crate::kernel::coupling_executor_registry::registered_coupling_executor_supports_template(&coupling.id))
            .cloned()
            .collect::<Vec<_>>();
        if executable_couplings.is_empty() {
            return Ok(());
        }
        let executor = crate::kernel::coupling_executor_registry::CouplingExecutorRegistry::new(
            self.operation_registry_root.clone(),
        );
        let scheduler = crate::kernel::coupling_scheduler::CouplingScheduler::new(
            self.runtime_store.as_ref(),
            &executor,
        );
        scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    coupling_set.snapshot_id.clone(),
                    executable_couplings,
                ),
                appended,
            )
            .await?;
        Ok(())
    }

    pub async fn build_session_context(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionContext> {
        self.runtime_store
            .build_context(coordinates)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn build_recall_read_plan_contexts(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<String>> {
        self.build_context_read_plan_contexts(
            coordinates,
            crate::kernel::runtime_host::context_read_plan::is_recall_context_read_plan_event,
            "memory_checkpoint",
            "memory context",
            crate::kernel::runtime_host::context_read_plan::render_recall_context,
        )
        .await
    }

    pub async fn build_instruction_read_plan_contexts(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<String>> {
        self.build_context_read_plan_contexts(
            coordinates,
            crate::kernel::runtime_host::context_read_plan::is_instruction_context_read_plan_event,
            "instruction_checkpoint",
            "instruction context",
            crate::kernel::runtime_host::context_read_plan::render_instruction_context,
        )
        .await
    }

    async fn build_context_read_plan_contexts(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        event_filter: fn(&verlet_history::EventRecord) -> bool,
        event_role: &str,
        label: &str,
        render: fn(&[String]) -> String,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<String>> {
        let derived_context_stream = verlet_history::EventStreamId::new(format!(
            "derived:context:{}",
            coordinates.thread_id
        ));
        let events = self
            .runtime_store
            .read_events(&derived_context_stream, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        let Some(read_plan_event) = events
            .into_iter()
            .filter(event_filter)
            .max_by_key(|event| event.sequence.get())
        else {
            return Ok(Vec::new());
        };
        let Some(read_plan) = read_plan_event.payload.get("read_plan") else {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "{label} read plan is missing read_plan"
            )));
        };
        if read_plan.get("schema").and_then(|value| value.as_str())
            != Some(verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1)
        {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "{label} read plan has unsupported schema"
            )));
        }
        let source_stream = read_plan
            .get("source_stream")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "{label} read plan is missing source_stream"
                ))
            })?;
        let entries = read_plan
            .get("entries")
            .and_then(|value| value.as_array())
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "{label} read plan is missing entries"
                ))
            })?;
        let mut seen_event_ids = std::collections::BTreeSet::new();
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
                    crate::kernel::runtime_host::VerletError::History(format!(
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
                .read_events(
                    &verlet_history::EventStreamId::new(stream_id.to_string()),
                    None,
                )
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(err.to_string())
                })?;
            let event = source_events
                .iter()
                .find(|event| event.id.to_string() == event_id)
                .ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "{label} read plan referenced missing event {event_id}"
                    ))
                })?;
            if event.kind != verlet_history::EventKind::ContextSummaryCompleted {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
                    "{label} read plan referenced non-summary event {event_id}"
                )));
            }
            let text = event
                .payload
                .get("text")
                .and_then(|value| value.as_str())
                .filter(|text| !text.trim().is_empty())
                .ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::History(format!(
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
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        session_entries: &[verlet_history::SessionEntry],
        payload: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::ObservationRecord> {
        let fallback_cut =
            crate::kernel::runtime_host::context_read_plan::session_context_source_cut_for_entries(
                coordinates,
                session_entries,
            );
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
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        session_entries: &[verlet_history::SessionEntry],
        source_cuts: &[verlet_history::SessionContextSourceCut],
        payload: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::ObservationRecord> {
        let stream_id = verlet_history::EventStreamId::for_thread(coordinates);
        let source_cuts = if source_cuts.is_empty() {
            crate::kernel::runtime_host::context_read_plan::session_context_source_cut_for_entries(
                coordinates,
                session_entries,
            )
        } else {
            source_cuts.to_vec()
        };
        let source_ranges = crate::kernel::runtime_host::context_read_plan::context_source_ranges(
            self.runtime_store.as_ref(),
            &source_cuts,
        )
        .await?;
        let source_range =
            crate::kernel::runtime_host::context_read_plan::primary_context_source_range(
                &stream_id,
                &source_ranges,
            );
        let source_streams = crate::kernel::runtime_host::context_read_plan::context_source_streams(
            &source_cuts,
            &stream_id,
        );
        let payload = crate::kernel::runtime_host::context_read_plan::context_compile_payload_v1(
            payload,
            &stream_id,
            &source_ranges,
            &source_streams,
        );
        let event_provenance = verlet_history::EventProvenance {
            source_streams: source_streams.clone(),
            source_range: source_range.clone(),
            source_ranges: source_ranges.clone(),
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("naive_assembly/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        };
        let compile_event = self
            .append_thread_event(
                coordinates,
                verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ContextCompileCompleted,
                    payload.clone(),
                    event_provenance,
                ),
            )
            .await?;
        let observation = verlet_history::NewObservationRecord::new(
            "compiled_context_receipt",
            coordinates.clone(),
            payload,
        )
        .with_provenance(verlet_history::ObservationProvenance {
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
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn record_context_summary_checkpoint(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        session_entries: &[verlet_history::SessionEntry],
        source_cuts: &[verlet_history::SessionContextSourceCut],
        summary: &str,
    ) -> crate::kernel::runtime_host::VerletResult<(
        verlet_history::EventRecord,
        verlet_history::EventRecord,
    )> {
        let stream_id = verlet_history::EventStreamId::for_thread(coordinates);
        let source_cuts = if source_cuts.is_empty() {
            crate::kernel::runtime_host::context_read_plan::session_context_source_cut_for_entries(
                coordinates,
                session_entries,
            )
        } else {
            source_cuts.to_vec()
        };
        let source_ranges = crate::kernel::runtime_host::context_read_plan::context_source_ranges(
            self.runtime_store.as_ref(),
            &source_cuts,
        )
        .await?;
        let source_range =
            crate::kernel::runtime_host::context_read_plan::primary_context_source_range(
                &stream_id,
                &source_ranges,
            );
        let source_streams = crate::kernel::runtime_host::context_read_plan::context_source_streams(
            &source_cuts,
            &stream_id,
        );
        let summary_event = self
            .runtime_store
            .append_events(
                &stream_id,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ContextSummaryCompleted,
                    crate::kernel::runtime_host::context_read_plan::context_summary_completed_payload_v1(summary, &source_ranges),
                    verlet_history::EventProvenance {
                        source_streams: source_streams.clone(),
                        source_range: source_range.clone(),
                        source_ranges: source_ranges.clone(),
                        discharged_by: Some("projection:context-summarizer".to_string()),
                        function: Some("context_summary/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History("context summary event append returned no record".to_string())
            })?;
        let read_plan_event = self
            .runtime_store
            .append_events(
                &stream_id,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ContextReadPlanSet,
                    crate::kernel::runtime_host::context_read_plan::context_read_plan_set_payload_v1(
                        "history.default",
                        &stream_id,
                        summary_event.id,
                        &source_ranges,
                    ),
                    verlet_history::EventProvenance {
                        source_streams: vec![stream_id.clone()],
                        source_event_ids: vec![summary_event.id],
                        source_range,
                        source_ranges,
                        discharged_by: Some("controller:context-budget".to_string()),
                        function: Some("context_read_plan/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(
                    "context read plan event append returned no record".to_string(),
                )
            })?;
        Ok((summary_event, read_plan_event))
    }
}

/// Result of the fenced first-join-wins append shared by live lifecycle code
/// and startup recovery.
pub(crate) struct ThreadJoinAppend {
    pub(crate) record: verlet_history::EventRecord,
    pub(crate) appended: bool,
}

/// Atomically append one `thread.joined`, or return the already committed
/// first join for the same spawn binding. Every caller races through the
/// stream-tail fence, so recovery can never overwrite a live monitor and a
/// live retry can never append behind recovery.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_thread_joined_first_wins(
    store: &dyn verlet_history::RuntimeStore,
    parent_coordinates: verlet_runtime_contracts::ThreadCoordinates,
    child_coordinates: verlet_runtime_contracts::ThreadCoordinates,
    spawned_event_id: verlet_history::EventRecordId,
    terminal_state: verlet_history::ThreadTerminalState,
    result_digest: Option<String>,
    reason: Option<String>,
    source_event: Option<(verlet_history::EventStreamId, verlet_history::EventRecordId)>,
    discharged_by: &str,
    function: &str,
) -> crate::kernel::runtime_host::VerletResult<ThreadJoinAppend> {
    let parent_control_stream =
        verlet_history::EventStreamId::new(format!("control:{}", parent_coordinates.thread_id));
    let spawned_event_id_text = spawned_event_id.to_string();
    let mut payload = serde_json::to_value(verlet_history::ThreadJoinedPayload {
        child_thread_id: child_coordinates.thread_id,
        spawned_event_id,
        terminal_state,
        result_digest,
    })
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "thread.joined payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::ThreadJoined.payload_schema_id()),
        );
        if let Some(reason) = reason {
            object.insert("reason".to_string(), serde_json::json!(reason));
        }
    }
    let (source_streams, source_event_ids) = source_event
        .map(|(stream_id, event_id)| (vec![stream_id], vec![event_id]))
        .unwrap_or_else(|| {
            (
                vec![verlet_history::EventStreamId::for_thread(
                    &child_coordinates,
                )],
                Vec::new(),
            )
        });
    let record = verlet_history::NewEventRecord::discharged(
        parent_coordinates,
        verlet_history::EventKind::ThreadJoined,
        payload,
        verlet_history::EventProvenance {
            source_streams,
            source_event_ids,
            discharged_by: Some(discharged_by.to_string()),
            function: Some(function.to_string()),
            ..verlet_history::EventProvenance::default()
        },
    );

    loop {
        let events = store
            .read_events(&parent_control_stream, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        for existing in events
            .iter()
            .filter(|event| event.kind == verlet_history::EventKind::ThreadJoined)
        {
            let existing_payload = serde_json::from_value::<verlet_history::ThreadJoinedPayload>(
                existing.payload.clone(),
            )
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "existing thread.joined {} payload is malformed: {err}",
                    existing.id
                ))
            })?;
            if existing_payload.spawned_event_id.to_string() == spawned_event_id_text {
                return Ok(ThreadJoinAppend {
                    record: existing.clone(),
                    appended: false,
                });
            }
        }
        let expected_next_sequence = events
            .last()
            .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
            .unwrap_or_else(|| verlet_history::EventSequence::new(1));
        match store
            .append_events_fenced(
                &parent_control_stream,
                expected_next_sequence,
                vec![record.clone()],
            )
            .await
        {
            Ok(appended) => {
                let expected = verlet_history::EventRecord::from_new(
                    parent_control_stream.clone(),
                    expected_next_sequence,
                    record.clone(),
                );
                if appended.len() != 1 || appended[0] != expected {
                    return Err(crate::kernel::runtime_host::VerletError::History(format!(
                        "thread.joined fenced append returned {} unexpected record(s)",
                        appended.len()
                    )));
                }
                return Ok(ThreadJoinAppend {
                    record: appended[0].clone(),
                    appended: true,
                });
            }
            Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
            Err(err) => {
                return Err(crate::kernel::runtime_host::VerletError::History(
                    err.to_string(),
                ));
            }
        }
    }
}
