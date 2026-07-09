use super::runtime_events::emit_runtime_event;
use super::runtime_utils::unix_timestamp_ms;
use super::{
    CooldisError, CooldisResult, RuntimeEventKind, RuntimeThreadHandle, ThreadCheckpoint,
    ThreadCommand, ThreadEvent,
};
use crate::agent::manifest_bind::{
    BoundCouplingSet, MANIFEST_BINDER_DISCHARGED_BY, MANIFEST_BINDER_FUNCTION,
    MANIFEST_COMPILER_DISCHARGED_BY, MANIFEST_COMPILER_FUNCTION, coupling_set_content_hash,
};
use crate::kernel::history::{
    EventKind, EventProvenance, EventRecord, EventSequence, EventStreamId, NewEventRecord,
    PolicyBoundPayload, PolicyKind, SessionContext, SessionEntry, SessionEntryKind, StreamCursorV1,
};
use crate::kernel::runtime_host::THREAD_BOUND_COUPLING_SET_METADATA;
use cooldis_runtime_contracts::{
    ThreadCheckpointId, ThreadContext, ThreadLifecycleRecord, ThreadLifecycleStatus, ThreadSignal,
    ThreadSignalKind, ThreadStatus,
};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;

impl RuntimeThreadHandle {
    pub fn context(&self) -> &ThreadContext {
        &self.thread.context
    }

    pub fn status(&self) -> ThreadStatus {
        *self.thread.status_rx.borrow()
    }

    pub fn queued_command_count(&self) -> usize {
        self.thread.command_capacity - self.thread.command_tx.capacity()
    }

    pub fn next_turn_sequence(&self) -> u64 {
        self.thread.turn_sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn current_turn_sequence(&self) -> u64 {
        self.thread.turn_sequence.load(Ordering::SeqCst)
    }

    pub fn set_status(&self, status: ThreadStatus) {
        let _ = self.thread.status_tx.send(status);
    }

    pub async fn lifecycle_record(&self) -> ThreadLifecycleRecord {
        let mut record = self.thread.lifecycle.lock().await.clone();
        record.status = ThreadLifecycleStatus::from(self.status());
        record
    }

    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<ThreadEvent> {
        self.thread.event_tx.subscribe()
    }

    pub fn subscribe_status(&self) -> tokio::sync::watch::Receiver<ThreadStatus> {
        self.thread.status_rx.clone()
    }

    pub async fn session_context(&self) -> CooldisResult<SessionContext> {
        self.thread
            .services
            .build_session_context(&self.thread.context.coordinates)
            .await
    }

    pub async fn record_manifest_receipts(
        &self,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
    ) -> CooldisResult<(EventRecord, EventRecord)> {
        let coordinates = self.thread.context.coordinates.clone();
        let stream_id = EventStreamId::for_thread(&coordinates);
        let compile_event = NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::ManifestCompileCompleted,
            compile_payload,
            EventProvenance {
                source_streams: vec![stream_id.clone()],
                discharged_by: Some(MANIFEST_COMPILER_DISCHARGED_BY.to_string()),
                function: Some(MANIFEST_COMPILER_FUNCTION.to_string()),
                ..EventProvenance::default()
            },
        );
        let bind_event = NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::ManifestBindCompleted,
            bind_payload,
            EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_event_ids: vec![compile_event.id],
                discharged_by: Some(MANIFEST_BINDER_DISCHARGED_BY.to_string()),
                function: Some(MANIFEST_BINDER_FUNCTION.to_string()),
                ..EventProvenance::default()
            },
        );
        let mut records = vec![compile_event, bind_event];
        if let Some(raw_coupling_set) = self
            .thread
            .context
            .metadata
            .get(THREAD_BOUND_COUPLING_SET_METADATA)
        {
            let coupling_set =
                serde_json::from_str::<BoundCouplingSet>(raw_coupling_set).map_err(|err| {
                    CooldisError::RuntimeFactory(format!(
                        "thread bound coupling set is invalid: {err}"
                    ))
                })?;
            let content_hash = coupling_set_content_hash(&coupling_set)?;
            let payload = PolicyBoundPayload {
                policy_kind: PolicyKind::CouplingSet,
                policy_id: format!("coupling_set:{}", coupling_set.snapshot_id),
                content_hash: content_hash.clone(),
                valid_from_note: "valid until next policy.bound of same policy_id".to_string(),
            };
            let mut value = serde_json::to_value(payload).map_err(|err| {
                CooldisError::History(format!("policy.bound payload codec failed: {err}"))
            })?;
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "schema".to_string(),
                    serde_json::json!(EventKind::PolicyBound.payload_schema_id()),
                );
            }
            records.push(NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::PolicyBound,
                value,
                EventProvenance {
                    source_streams: vec![stream_id.clone()],
                    source_event_ids: vec![records[1].id],
                    discharged_by: Some(MANIFEST_BINDER_DISCHARGED_BY.to_string()),
                    function: Some(MANIFEST_BINDER_FUNCTION.to_string()),
                    config_hash: Some(content_hash),
                    ..EventProvenance::default()
                },
            ));
        }
        let events = self
            .thread
            .services
            .runtime_store()
            .append_events(&stream_id, records)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        if events.len() < 2 {
            return Err(CooldisError::History(format!(
                "manifest receipt append returned {} record(s)",
                events.len()
            )));
        }
        let mut events = events.into_iter();
        let compile = events.next().ok_or_else(|| {
            CooldisError::History("manifest compile event was not returned".to_string())
        })?;
        let bind = events.next().ok_or_else(|| {
            CooldisError::History("manifest bind event was not returned".to_string())
        })?;
        Ok((compile, bind))
    }

    pub async fn record_tool_universe_discovery_receipts(
        &self,
        payloads: Vec<serde_json::Value>,
    ) -> CooldisResult<Vec<EventRecord>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let coordinates = self.thread.context.coordinates.clone();
        let stream_id = EventStreamId::for_thread(&coordinates);
        let records = payloads
            .into_iter()
            .map(|payload| {
                NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::ToolUniverseDiscoveryCompleted,
                    payload,
                )
            })
            .collect::<Vec<_>>();
        self.thread
            .services
            .runtime_store()
            .append_events(&stream_id, records)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))
    }

    pub async fn append_control_event(&self, record: NewEventRecord) -> CooldisResult<EventRecord> {
        self.thread
            .services
            .append_control_event(&self.thread.context.coordinates, record)
            .await
    }

    pub async fn read_control_events(&self) -> CooldisResult<Vec<EventRecord>> {
        let stream_id = EventStreamId::new(format!(
            "control:{}",
            self.thread.context.coordinates.thread_id
        ));
        self.thread
            .services
            .runtime_store()
            .read_events(&stream_id, None)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))
    }

    pub async fn read_thread_events(
        &self,
        from_sequence: Option<EventSequence>,
    ) -> CooldisResult<Vec<EventRecord>> {
        let stream_id = EventStreamId::for_thread(&self.thread.context.coordinates);
        self.thread
            .services
            .runtime_store()
            .read_events(&stream_id, from_sequence)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))
    }

    pub async fn read_thread_events_after_cursor(
        &self,
        cursor: &StreamCursorV1,
    ) -> CooldisResult<Vec<EventRecord>> {
        let stream_id = EventStreamId::for_thread(&self.thread.context.coordinates);
        self.thread
            .services
            .runtime_store()
            .read_events_after_cursor(&stream_id, cursor)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))
    }

    pub async fn append_thread_event_record(
        &self,
        record: NewEventRecord,
    ) -> CooldisResult<EventRecord> {
        let stream_id = EventStreamId::for_thread(&self.thread.context.coordinates);
        self.thread
            .services
            .runtime_store()
            .append_events(&stream_id, vec![record])
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| CooldisError::History("event append returned no record".to_string()))
    }

    pub async fn append_runtime_session_entry(
        &self,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> CooldisResult<SessionEntry> {
        self.thread
            .services
            .append_session_entry(
                &self.thread.context.coordinates,
                None,
                SessionEntryKind::Runtime {
                    kind: kind.into(),
                    payload,
                },
            )
            .await
    }

    pub async fn send(&self, command: ThreadCommand) -> CooldisResult<()> {
        let thread_id = self.thread.context.coordinates.thread_id;
        self.thread
            .command_tx
            .send(command)
            .await
            .map_err(|_| CooldisError::ThreadClosed(thread_id))?;
        Ok(())
    }

    pub async fn reserve_command(&self) -> CooldisResult<mpsc::Permit<'_, ThreadCommand>> {
        let thread_id = self.thread.context.coordinates.thread_id;
        self.thread
            .command_tx
            .reserve()
            .await
            .map_err(|_| CooldisError::ThreadClosed(thread_id))
    }

    pub async fn record_signal(&self, signal: ThreadSignal) {
        let mut lifecycle = self.thread.lifecycle.lock().await;
        lifecycle.latest_signal_id = Some(signal.id);
        lifecycle.updated_at_ms = signal.created_at_ms;
    }

    pub async fn create_checkpoint(
        &self,
        parent_checkpoint_id: Option<ThreadCheckpointId>,
        label: Option<String>,
        metadata: BTreeMap<String, String>,
    ) -> CooldisResult<ThreadCheckpoint> {
        let requested = ThreadSignal::new(
            self.thread.context.coordinates.clone(),
            ThreadSignalKind::CheckpointRequested,
        );
        self.record_signal(requested).await;

        let checkpoint = ThreadCheckpoint {
            id: ThreadCheckpointId::new(),
            coordinates: self.thread.context.coordinates.clone(),
            parent_checkpoint_id,
            active_entry_id: None,
            label,
            metadata,
            created_at_ms: unix_timestamp_ms(),
        };
        let checkpoint_entry = self
            .thread
            .services
            .append_session_entry(
                &self.thread.context.coordinates,
                None,
                SessionEntryKind::Runtime {
                    kind: "thread_checkpoint".to_string(),
                    payload: serde_json::json!({
                        "checkpoint_id": checkpoint.id.to_string(),
                        "parent_checkpoint_id": checkpoint.parent_checkpoint_id.map(|id| id.to_string()),
                        "label": checkpoint.label.clone(),
                        "metadata": checkpoint.metadata.clone(),
                    }),
                },
            )
            .await?;
        let checkpoint = ThreadCheckpoint {
            active_entry_id: Some(checkpoint_entry.entry_id),
            ..checkpoint
        };
        self.thread
            .checkpoints
            .lock()
            .await
            .push(checkpoint.clone());

        let created = ThreadSignal::new(
            self.thread.context.coordinates.clone(),
            ThreadSignalKind::CheckpointCreated,
        );
        let mut lifecycle = self.thread.lifecycle.lock().await;
        lifecycle.latest_signal_id = Some(created.id);
        lifecycle.latest_checkpoint_id = Some(checkpoint.id);
        lifecycle.updated_at_ms = checkpoint.created_at_ms;
        drop(lifecycle);
        self.emit_runtime(RuntimeEventKind::Checkpoint {
            checkpoint_id: checkpoint.id,
            label: checkpoint.label.clone(),
        });
        Ok(checkpoint)
    }

    pub fn emit_runtime(&self, kind: RuntimeEventKind) {
        emit_runtime_event(
            &self.thread.event_tx,
            &self.thread.context.coordinates,
            kind,
        );
    }

    pub async fn cancel(&self, reason: impl Into<String>) -> CooldisResult<()> {
        self.send(ThreadCommand::Cancel {
            reason: reason.into(),
        })
        .await
    }

    pub async fn wait(&self) {
        if let Some(join_handle) = self.thread.join_handle.lock().await.take() {
            let _ = join_handle.await;
        }
    }

    pub async fn wait_timeout_or_abort(&self, timeout: Duration) -> bool {
        let mut guard = self.thread.join_handle.lock().await;
        let Some(mut join_handle) = guard.take() else {
            return true;
        };
        tokio::select! {
            result = &mut join_handle => {
                let _ = result;
                true
            }
            _ = tokio::time::sleep(timeout) => {
                join_handle.abort();
                let _ = join_handle.await;
                false
            }
        }
    }

    pub async fn abort(&self) {
        if let Some(join_handle) = self.thread.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.await;
        }
    }
}
