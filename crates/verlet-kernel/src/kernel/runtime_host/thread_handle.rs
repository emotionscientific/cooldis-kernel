impl crate::kernel::runtime_host::RuntimeThreadHandle {
    pub fn context(&self) -> &verlet_runtime_contracts::ThreadContext {
        &self.thread.context
    }

    pub fn status(&self) -> verlet_runtime_contracts::ThreadStatus {
        *self.thread.status_rx.borrow()
    }

    pub fn queued_command_count(&self) -> usize {
        self.thread.command_capacity - self.thread.command_tx.capacity()
    }

    pub fn set_status(&self, status: verlet_runtime_contracts::ThreadStatus) {
        let _ = self.thread.status_tx.send(status);
    }

    pub async fn lifecycle_record(&self) -> verlet_runtime_contracts::ThreadLifecycleRecord {
        let mut record = self.thread.lifecycle.lock().await.clone();
        record.status = verlet_runtime_contracts::ThreadLifecycleStatus::from(self.status());
        record
    }

    pub fn subscribe_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::kernel::runtime_host::runtime_api::ThreadEvent>
    {
        self.thread.event_tx.subscribe()
    }

    pub fn subscribe_status(
        &self,
    ) -> tokio::sync::watch::Receiver<verlet_runtime_contracts::ThreadStatus> {
        self.thread.status_rx.clone()
    }

    pub async fn session_context(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionContext> {
        self.thread
            .services
            .build_session_context(&self.thread.context.coordinates)
            .await
    }

    pub async fn record_manifest_receipts(
        &self,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<(
        verlet_history::EventRecord,
        verlet_history::EventRecord,
    )> {
        let principal_id = self.thread.context.coordinates.user_id.clone();
        self.record_manifest_receipts_inner(compile_payload, bind_payload, &principal_id, false)
            .await
    }

    pub(crate) async fn record_manifest_receipts_for_principal(
        &self,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
        principal_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<(
        verlet_history::EventRecord,
        verlet_history::EventRecord,
    )> {
        self.record_manifest_receipts_inner(compile_payload, bind_payload, principal_id, false)
            .await
    }

    /// Records the already-resolved remote bind receipt inside the child
    /// process that owns the remote execution. This is intentionally
    /// crate-private: execution surfaces must enter through the placement
    /// resolver and process executor before reaching this recording seam.
    pub(crate) async fn record_remote_manifest_receipts(
        &self,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<(
        verlet_history::EventRecord,
        verlet_history::EventRecord,
    )> {
        let principal_id = self.thread.context.coordinates.user_id.clone();
        self.record_manifest_receipts_inner(compile_payload, bind_payload, &principal_id, true)
            .await
    }

    async fn record_manifest_receipts_inner(
        &self,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
        principal_id: &str,
        remote_execution_authorized: bool,
    ) -> crate::kernel::runtime_host::VerletResult<(
        verlet_history::EventRecord,
        verlet_history::EventRecord,
    )> {
        if principal_id.trim().is_empty() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "manifest bind principal is required".to_string(),
            ));
        }
        let coordinates = self.thread.context.coordinates.clone();
        let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
        let operation_bindings = bind_payload
            .get("operation_bindings")
            .cloned()
            .map(
                serde_json::from_value::<
                    Vec<crate::agent::manifest_bind::AgentManifestOperationBinding>,
                >,
            )
            .transpose()
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "manifest operation binding payload codec failed: {err}"
                ))
            })?
            .unwrap_or_default();
        let mut placement =
            bind_payload
                .get("placement")
                .cloned()
                .map(
                    serde_json::from_value::<
                        crate::agent::manifest_bind::AgentManifestPlacementBinding,
                    >,
                )
                .transpose()
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "manifest bind placement payload codec failed: {err}"
                    ))
                })?
                .unwrap_or_default();
        if !remote_execution_authorized {
            placement = crate::agent::manifest_bind::resolve_manifest_placement(
                None,
                Some(&placement),
                false,
            )?;
        }
        if placement.target != crate::kernel::control_decision::PlacementTarget::Local
            && bind_payload
                .get("workspace")
                .is_some_and(|workspace| !workspace.is_null())
        {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "workspace bindings require local placement and cannot be witnessed by a remote or sandbox runtime"
                    .to_string(),
            ));
        }
        let snapshot_id = bind_payload
            .get("manifest_hash")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(
                    "manifest bind receipt is missing manifest_hash for placement witness"
                        .to_string(),
                )
            })?
            .to_string();
        let compile_event = verlet_history::NewEventRecord::discharged(
            coordinates.clone(),
            verlet_history::EventKind::ManifestCompileCompleted,
            compile_payload,
            verlet_history::EventProvenance {
                source_streams: vec![stream_id.clone()],
                discharged_by: Some(
                    crate::agent::manifest_bind::MANIFEST_COMPILER_DISCHARGED_BY.to_string(),
                ),
                function: Some(crate::agent::manifest_bind::MANIFEST_COMPILER_FUNCTION.to_string()),
                ..verlet_history::EventProvenance::default()
            },
        );
        let bind_event = verlet_history::NewEventRecord::discharged(
            coordinates.clone(),
            verlet_history::EventKind::ManifestBindCompleted,
            bind_payload,
            verlet_history::EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_event_ids: vec![compile_event.id],
                discharged_by: Some(
                    crate::agent::manifest_bind::MANIFEST_BINDER_DISCHARGED_BY.to_string(),
                ),
                function: Some(crate::agent::manifest_bind::MANIFEST_BINDER_FUNCTION.to_string()),
                ..verlet_history::EventProvenance::default()
            },
        );
        let placement_payload =
            serde_json::to_value(crate::kernel::control_decision::PlacementDecisionPayload {
                subject: crate::kernel::control_decision::PlacementSubject {
                    invocation_id: bind_event.id.to_string(),
                },
                snapshot_id,
                placement: placement.target,
            })
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "placement decision payload codec failed: {err}"
                ))
            })?;
        let placement_event = verlet_history::NewEventRecord::witnessed(
            coordinates.clone(),
            verlet_history::EventKind::PlacementDecision,
            placement_payload,
        );
        let attachment_events = operation_bindings
            .iter()
            .map(|binding| {
                let payload =
                    crate::agent::manifest_bind::binding_attached_payload(binding, principal_id);
                serde_json::to_value(payload)
                    .map(|payload| {
                        verlet_history::NewEventRecord::discharged(
                            coordinates.clone(),
                            verlet_history::EventKind::BindingAttached,
                            payload,
                            verlet_history::EventProvenance {
                                source_streams: vec![stream_id.clone()],
                                source_event_ids: vec![bind_event.id],
                                discharged_by: Some(
                                    crate::agent::manifest_bind::MANIFEST_BINDER_DISCHARGED_BY
                                        .to_string(),
                                ),
                                function: Some(
                                    crate::agent::manifest_bind::MANIFEST_BINDER_FUNCTION
                                        .to_string(),
                                ),
                                ..verlet_history::EventProvenance::default()
                            },
                        )
                    })
                    .map_err(|err| {
                        crate::kernel::runtime_host::VerletError::History(format!(
                            "binding.attached payload codec failed: {err}"
                        ))
                    })
            })
            .collect::<crate::kernel::runtime_host::VerletResult<Vec<_>>>()?;
        // Receipts and witnesses share one atomic store append. This closes the
        // crash/race window: callers never receive a bind receipt unless its
        // effective binding and placement facts committed in the same batch.
        let minimum_event_count = 3 + attachment_events.len();
        let mut records = vec![compile_event, bind_event];
        records.extend(attachment_events);
        records.push(placement_event);
        if let Some(raw_coupling_set) = self
            .thread
            .context
            .metadata
            .get(crate::kernel::runtime_host::THREAD_BOUND_COUPLING_SET_METADATA)
        {
            let coupling_set =
                serde_json::from_str::<crate::agent::manifest_bind::BoundCouplingSet>(
                    raw_coupling_set,
                )
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "thread bound coupling set is invalid: {err}"
                    ))
                })?;
            let content_hash =
                crate::agent::manifest_bind::coupling_set_content_hash(&coupling_set)?;
            let payload = verlet_history::PolicyBoundPayload {
                policy_kind: verlet_history::PolicyKind::CouplingSet,
                policy_id: format!("coupling_set:{}", coupling_set.snapshot_id),
                content_hash: content_hash.clone(),
                valid_from_note: "valid until next policy.bound of same policy_id".to_string(),
            };
            let mut value = serde_json::to_value(payload).map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "policy.bound payload codec failed: {err}"
                ))
            })?;
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "schema".to_string(),
                    serde_json::json!(verlet_history::EventKind::PolicyBound.payload_schema_id()),
                );
            }
            records.push(verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::PolicyBound,
                value,
                verlet_history::EventProvenance {
                    source_streams: vec![stream_id.clone()],
                    source_event_ids: vec![records[1].id],
                    discharged_by: Some(
                        crate::agent::manifest_bind::MANIFEST_BINDER_DISCHARGED_BY.to_string(),
                    ),
                    function: Some(
                        crate::agent::manifest_bind::MANIFEST_BINDER_FUNCTION.to_string(),
                    ),
                    config_hash: Some(content_hash),
                    ..verlet_history::EventProvenance::default()
                },
            ));
        }
        // Once the atomic append has entered the store, finish it even if the
        // RPC task that initiated the bind is cancelled. The runtime-host
        // start guards separately remove a thread cancelled during factory
        // construction, so no mounted runtime survives without this batch.
        let runtime_store = self.thread.services.runtime_store();
        let expected_next_sequence = runtime_store
            .read_events(&stream_id, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
            .last()
            .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
            .unwrap_or_else(|| verlet_history::EventSequence::new(1));
        let append = tokio::spawn(async move {
            runtime_store
                .append_events_fenced(&stream_id, expected_next_sequence, records)
                .await
        });
        let events = append
            .await
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "manifest receipt append task failed: {err}"
                ))
            })?
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        if events.len() < minimum_event_count {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "manifest receipt append returned {} record(s)",
                events.len()
            )));
        }
        let mut events = events.into_iter();
        let compile = events.next().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(
                "manifest compile event was not returned".to_string(),
            )
        })?;
        let bind = events.next().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(
                "manifest bind event was not returned".to_string(),
            )
        })?;
        Ok((compile, bind))
    }

    pub async fn record_thread_start_identity(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionEntry> {
        self.append_runtime_session_entry(
            "thread_started",
            serde_json::json!({
                "parent_thread_id": self.thread.context.parent_thread_id,
                "topology": self.thread.context.topology,
                "metadata": self.thread.context.metadata,
            }),
        )
        .await
    }

    /// Reconciles the reserved fork child's identity append before the host
    /// publishes lifecycle side effects. Only the append is retried: an
    /// unrelated history-shaped factory or lifecycle error must not re-enter
    /// the whole child start.
    pub(super) async fn record_thread_start_identity_with_reconciliation(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let mut first_error = None;
        for attempt in 0..2 {
            match self.record_thread_start_identity().await {
                Ok(_) => return Ok(()),
                Err(error) => {
                    let events = self.read_thread_events(None).await.map_err(|read_error| {
                        reconciliation_read_error(
                            "thread start identity append",
                            &error,
                            read_error,
                        )
                    })?;
                    if events.iter().any(|event| {
                        thread_start_identity_matches_context(event, &self.thread.context)
                    }) {
                        return Ok(());
                    }
                    if attempt == 1 {
                        return Err(first_error.unwrap_or(error));
                    }
                    first_error = Some(error);
                }
            }
        }
        unreachable!("thread start identity reconciliation has exactly two attempts")
    }

    pub async fn record_tool_universe_discovery_receipts(
        &self,
        payloads: Vec<serde_json::Value>,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let coordinates = self.thread.context.coordinates.clone();
        let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
        let records = payloads
            .into_iter()
            .map(|payload| {
                verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::ToolUniverseDiscoveryCompleted,
                    payload,
                )
            })
            .collect::<Vec<_>>();
        self.thread
            .services
            .runtime_store()
            .append_events(&stream_id, records)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn append_control_event(
        &self,
        record: verlet_history::NewEventRecord,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
        self.thread
            .services
            .append_control_event(&self.thread.context.coordinates, record)
            .await
    }

    pub async fn read_control_events(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
        let stream_id = verlet_history::EventStreamId::new(format!(
            "control:{}",
            self.thread.context.coordinates.thread_id
        ));
        self.thread
            .services
            .runtime_store()
            .read_events(&stream_id, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn read_thread_events(
        &self,
        from_sequence: Option<verlet_history::EventSequence>,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
        let stream_id = verlet_history::EventStreamId::for_thread(&self.thread.context.coordinates);
        self.thread
            .services
            .runtime_store()
            .read_events(&stream_id, from_sequence)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn read_thread_events_after_cursor(
        &self,
        cursor: &verlet_history::StreamCursorV1,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
        let stream_id = verlet_history::EventStreamId::for_thread(&self.thread.context.coordinates);
        self.thread
            .services
            .runtime_store()
            .read_events_after_cursor(&stream_id, cursor)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))
    }

    pub async fn append_thread_event_record(
        &self,
        record: verlet_history::NewEventRecord,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
        let stream_id = verlet_history::EventStreamId::for_thread(&self.thread.context.coordinates);
        self.thread
            .services
            .runtime_store()
            .append_events(&stream_id, vec![record])
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(
                    "event append returned no record".to_string(),
                )
            })
    }

    pub async fn append_runtime_session_entry(
        &self,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionEntry> {
        self.thread
            .services
            .append_session_entry(
                &self.thread.context.coordinates,
                None,
                verlet_history::SessionEntryKind::Runtime {
                    kind: kind.into(),
                    payload,
                },
            )
            .await
    }

    pub async fn send(
        &self,
        command: crate::kernel::runtime_host::runtime_api::ThreadCommand,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let thread_id = self.thread.context.coordinates.thread_id;
        self.thread
            .command_tx
            .send(command)
            .await
            .map_err(|_| crate::kernel::runtime_host::VerletError::ThreadClosed(thread_id))?;
        Ok(())
    }

    pub async fn reserve_command(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<
        tokio::sync::mpsc::Permit<'_, crate::kernel::runtime_host::runtime_api::ThreadCommand>,
    > {
        let thread_id = self.thread.context.coordinates.thread_id;
        self.thread
            .command_tx
            .reserve()
            .await
            .map_err(|_| crate::kernel::runtime_host::VerletError::ThreadClosed(thread_id))
    }

    pub(super) fn try_reserve_command(
        &self,
    ) -> Result<
        tokio::sync::mpsc::Permit<'_, crate::kernel::runtime_host::runtime_api::ThreadCommand>,
        tokio::sync::mpsc::error::TrySendError<()>,
    > {
        self.thread.command_tx.try_reserve()
    }

    pub async fn record_signal(&self, signal: verlet_runtime_contracts::ThreadSignal) {
        let mut lifecycle = self.thread.lifecycle.lock().await;
        lifecycle.latest_signal_id = Some(signal.id);
        lifecycle.updated_at_ms = signal.created_at_ms;
    }

    pub async fn create_checkpoint(
        &self,
        parent_checkpoint_id: Option<verlet_runtime_contracts::ThreadCheckpointId>,
        label: Option<String>,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    > {
        let requested = verlet_runtime_contracts::ThreadSignal::new(
            self.thread.context.coordinates.clone(),
            verlet_runtime_contracts::ThreadSignalKind::CheckpointRequested,
        );
        self.record_signal(requested).await;

        let checkpoint = crate::kernel::runtime_host::runtime_api::ThreadCheckpoint {
            id: verlet_runtime_contracts::ThreadCheckpointId::new(),
            coordinates: self.thread.context.coordinates.clone(),
            lineage: match self.thread.context.parent_thread_id {
                Some(parent_thread_id) => {
                    crate::kernel::runtime_host::runtime_api::ThreadCheckpointLineage::Parent {
                        parent_thread_id,
                    }
                }
                None => crate::kernel::runtime_host::runtime_api::ThreadCheckpointLineage::Root,
            },
            parent_checkpoint_id,
            active_entry_id: None,
            label,
            metadata,
            created_at_ms: crate::kernel::runtime_host::runtime_utils::unix_timestamp_ms(),
        };
        let checkpoint_kind = verlet_history::SessionEntryKind::Runtime {
            kind: "thread_checkpoint".to_string(),
            payload: serde_json::json!({
                "checkpoint_id": checkpoint.id.to_string(),
                "lineage": checkpoint.lineage,
                "parent_checkpoint_id": checkpoint.parent_checkpoint_id.map(|id| id.to_string()),
                "label": checkpoint.label.clone(),
                "metadata": checkpoint.metadata.clone(),
            }),
        };
        let checkpoint_entry_id = self
            .append_checkpoint_entry_with_reconciliation(checkpoint.id, checkpoint_kind)
            .await?;
        let checkpoint = crate::kernel::runtime_host::runtime_api::ThreadCheckpoint {
            active_entry_id: Some(checkpoint_entry_id),
            ..checkpoint
        };
        self.thread
            .checkpoints
            .lock()
            .await
            .push(checkpoint.clone());

        let created = verlet_runtime_contracts::ThreadSignal::new(
            self.thread.context.coordinates.clone(),
            verlet_runtime_contracts::ThreadSignalKind::CheckpointCreated,
        );
        let mut lifecycle = self.thread.lifecycle.lock().await;
        lifecycle.latest_signal_id = Some(created.id);
        lifecycle.latest_checkpoint_id = Some(checkpoint.id);
        lifecycle.updated_at_ms = checkpoint.created_at_ms;
        drop(lifecycle);
        self.emit_runtime(
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Checkpoint {
                checkpoint_id: checkpoint.id,
                label: checkpoint.label.clone(),
            },
        );
        Ok(checkpoint)
    }

    /// Makes the checkpoint prerequisite of a claimed fork survive one planned
    /// store append fault. An append error is ambiguous, so the exact
    /// checkpoint id is folded from the authoritative event stream before one
    /// bounded retry; this both advances a before-fault and adopts an
    /// after-fault without a selected-branch projection hiding the commit.
    async fn append_checkpoint_entry_with_reconciliation(
        &self,
        checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
        checkpoint_kind: verlet_history::SessionEntryKind,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionEntryId> {
        let checkpoint_id = checkpoint_id.to_string();
        let mut first_error = None;
        for attempt in 0..2 {
            match self
                .thread
                .services
                .append_session_entry(
                    &self.thread.context.coordinates,
                    None,
                    checkpoint_kind.clone(),
                )
                .await
            {
                Ok(entry) => return Ok(entry.entry_id),
                Err(error) => {
                    let events = self.read_thread_events(None).await.map_err(|read_error| {
                        reconciliation_read_error("checkpoint append", &error, read_error)
                    })?;
                    if let Some(event) = events.iter().rev().find(|event| {
                        event.kind == verlet_history::EventKind::SessionEntryAppended
                            && event
                                .payload
                                .get("runtime_kind")
                                .and_then(serde_json::Value::as_str)
                                == Some("thread_checkpoint")
                            && event
                                .payload
                                .get("runtime_payload")
                                .and_then(|payload| payload.get("checkpoint_id"))
                                .and_then(serde_json::Value::as_str)
                                == Some(checkpoint_id.as_str())
                    }) {
                        let entry_id = event
                            .payload
                            .get("entry_id")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                crate::kernel::runtime_host::VerletError::History(format!(
                                    "checkpoint {checkpoint_id} reconciliation event is missing entry_id"
                                ))
                            })
                            .and_then(|entry_id| {
                                uuid::Uuid::parse_str(entry_id)
                                    .map(verlet_history::SessionEntryId::from_uuid)
                                    .map_err(|parse_error| {
                                        crate::kernel::runtime_host::VerletError::History(format!(
                                            "checkpoint {checkpoint_id} reconciliation entry_id is invalid: {parse_error}"
                                        ))
                                    })
                            })?;
                        return Ok(entry_id);
                    }
                    if attempt == 1 {
                        return Err(first_error.unwrap_or(error));
                    }
                    first_error = Some(error);
                }
            }
        }
        unreachable!("checkpoint append reconciliation has exactly two attempts")
    }

    pub fn emit_runtime(
        &self,
        kind: crate::kernel::runtime_host::runtime_events::RuntimeEventKind,
    ) {
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            &self.thread.event_tx,
            &self.thread.context.coordinates,
            kind,
        );
    }

    pub async fn cancel(
        &self,
        reason: impl Into<String>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.send(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Cancel {
                reason: reason.into(),
            },
        )
        .await
    }

    pub async fn wait(&self) {
        if let Some(join_handle) = self.thread.join_handle.lock().await.take() {
            let _ = join_handle.await;
        }
    }

    pub async fn wait_timeout_or_abort(&self, timeout: std::time::Duration) -> bool {
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

fn thread_start_identity_matches_context(
    event: &verlet_history::EventRecord,
    context: &verlet_runtime_contracts::ThreadContext,
) -> bool {
    if event.kind != verlet_history::EventKind::SessionEntryAppended
        || event
            .payload
            .get("runtime_kind")
            .and_then(serde_json::Value::as_str)
            != Some("thread_started")
    {
        return false;
    }
    let Some(payload) = event
        .payload
        .get("runtime_payload")
        .and_then(serde_json::Value::as_object)
    else {
        return false;
    };
    let Ok(topology) = serde_json::from_value::<verlet_runtime_contracts::ThreadTopology>(
        payload["topology"].clone(),
    ) else {
        return false;
    };
    let Ok(metadata) = serde_json::from_value::<std::collections::BTreeMap<String, String>>(
        payload["metadata"].clone(),
    ) else {
        return false;
    };
    topology == context.topology && metadata == context.metadata
}

fn reconciliation_read_error(
    operation: &str,
    append_error: &crate::kernel::runtime_host::VerletError,
    read_error: crate::kernel::runtime_host::VerletError,
) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::History(format!(
        "{operation} failed ambiguously: {append_error}; reconciliation read failed: {read_error}"
    ))
}
