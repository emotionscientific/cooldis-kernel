#[derive(Clone)]
pub struct RuntimeKernelControl {
    inner: std::sync::Weak<crate::kernel::runtime_host::RuntimeHostInner>,
}

impl RuntimeKernelControl {
    pub(super) fn new(
        inner: std::sync::Weak<crate::kernel::runtime_host::RuntimeHostInner>,
    ) -> Self {
        Self { inner }
    }
}

async fn append_thread_spawned_event(
    parent: &crate::kernel::runtime_host::RuntimeThreadHandle,
    caller: &verlet_runtime_contracts::ThreadContext,
    child: &verlet_runtime_contracts::ThreadContext,
    witness: ThreadSpawnWitness,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    let metadata = &child.metadata;
    let child_manifest_hash = metadata
        .get(crate::kernel::runtime_host::THREAD_AGENT_MANIFEST_HASH_METADATA)
        .cloned()
        .unwrap_or_else(|| "unbound".to_string());
    let inputs_hash = metadata
        .get(crate::kernel::runtime_host::THREAD_SPAWN_INPUTS_HASH_METADATA)
        .cloned()
        .unwrap_or_else(|| {
            format!(
                "sha256:{}",
                verlet_agent::contracts::sha256_hex(
                    child.coordinates.thread_id.to_string().as_bytes()
                )
            )
        });
    let child_policy_hash = metadata
        .get(crate::kernel::runtime_host::THREAD_BOUND_COUPLING_SET_METADATA)
        .map(|raw| {
            serde_json::from_str::<crate::agent::manifest_bind::BoundCouplingSet>(raw)
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "thread bound coupling set is invalid: {err}"
                    ))
                })
                .and_then(|coupling_set| {
                    crate::agent::manifest_bind::coupling_set_content_hash(&coupling_set)
                })
        })
        .transpose()?;
    let payload = verlet_history::ThreadSpawnedPayload {
        parent_thread_id: caller.coordinates.thread_id,
        parent_turn_id: witness.parent_turn_id.clone(),
        child_thread_id: child.coordinates.thread_id,
        child_manifest_hash,
        granted: Vec::new(),
        child_policy_hash,
        inputs_hash,
        fork: None,
    };
    let mut value = serde_json::to_value(payload).map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "thread.spawned payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::ThreadSpawned.payload_schema_id()),
        );
        if let Some(correlation_id) = &witness.correlation_id {
            object.insert(
                "correlation_id".to_string(),
                serde_json::json!(correlation_id),
            );
        }
    }
    let mut record = verlet_history::NewEventRecord::witnessed(
        caller.coordinates.clone(),
        verlet_history::EventKind::ThreadSpawned,
        value,
    );
    if let (Some(stream_id), Some(event_id)) = (witness.request_stream_id, witness.request_event_id)
    {
        record.provenance = verlet_history::EventProvenance {
            source_streams: vec![stream_id],
            source_event_ids: vec![event_id],
            ..verlet_history::EventProvenance::default()
        };
    }
    parent.append_control_event(record).await
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ThreadSpawnWitness {
    pub parent_turn_id: Option<String>,
    pub correlation_id: Option<String>,
    pub request_stream_id: Option<verlet_history::EventStreamId>,
    pub request_event_id: Option<verlet_history::EventRecordId>,
    pub submitted_turn_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentProcessSpawnReceipt {
    pub operation: String,
    pub caller_thread_id: verlet_runtime_contracts::ThreadId,
    pub thread_id: verlet_runtime_contracts::ThreadId,
    pub parent_thread_id: verlet_runtime_contracts::ThreadId,
    pub status: verlet_runtime_contracts::ThreadStatus,
    pub task_name: Option<String>,
    pub submitted_turn_id: String,
    pub handle: verlet_runtime_contracts::handle::HandleId,
    pub dispatch_id: verlet_runtime_contracts::handle::DispatchId,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentProcessSubmitReceipt {
    pub operation: String,
    pub caller_thread_id: verlet_runtime_contracts::ThreadId,
    pub target_thread_id: verlet_runtime_contracts::ThreadId,
    pub interaction_id: verlet_runtime_contracts::RuntimeEventId,
    pub status: verlet_runtime_contracts::ThreadStatus,
    pub turn_id: String,
    pub dispatch_id: verlet_runtime_contracts::handle::DispatchId,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentProcessWaitReceipt {
    pub operation: String,
    pub caller_thread_id: verlet_runtime_contracts::ThreadId,
    pub target_thread_id: verlet_runtime_contracts::ThreadId,
    pub status: verlet_runtime_contracts::ThreadStatus,
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_interaction_id: Option<verlet_runtime_contracts::RuntimeEventId>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentProcessLifecycleReceipt {
    pub operation: String,
    pub caller_thread_id: verlet_runtime_contracts::ThreadId,
    pub target_thread_id: verlet_runtime_contracts::ThreadId,
    pub status: verlet_runtime_contracts::ThreadLifecycleStatus,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentProcessCheckpointReceipt {
    pub operation: String,
    pub caller_thread_id: verlet_runtime_contracts::ThreadId,
    pub target_thread_id: verlet_runtime_contracts::ThreadId,
    pub checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentProcessStatusReceipt {
    pub operation: String,
    pub caller_thread_id: verlet_runtime_contracts::ThreadId,
    pub target_thread_id: verlet_runtime_contracts::ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<verlet_runtime_contracts::ThreadId>,
    pub status: verlet_runtime_contracts::ThreadStatus,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentProcessChildRef {
    pub thread_id: verlet_runtime_contracts::ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<verlet_runtime_contracts::ThreadId>,
    pub status: verlet_runtime_contracts::ThreadStatus,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentProcessChildrenReceipt {
    pub operation: String,
    pub caller_thread_id: verlet_runtime_contracts::ThreadId,
    pub target_thread_id: verlet_runtime_contracts::ThreadId,
    pub children: Vec<AgentProcessChildRef>,
}

fn compile_declaration_contract(
    reference: &verlet_agent::contracts::ThreadContractReference,
) -> crate::kernel::runtime_host::VerletResult<verlet_agent::contracts::CompiledThreadContract> {
    if let Some(compiled) = &reference.compiled {
        compiled.validate()?;
        return Ok(compiled.clone());
    }
    if let Some(source) = &reference.inline {
        return Ok(verlet_agent::contracts::ThreadContractCompiler::compile(
            &verlet_agent::contracts::ThreadContractSource::markdown(source.clone()),
        )?);
    }
    if let Some(ref_path) = &reference.ref_path {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!(
                "thread contract ref {ref_path:?} must be resolved before RuntimeKernelControl::declare_thread"
            ),
        ));
    }
    Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
        "thread contract reference is empty".to_string(),
    ))
}

fn declaration_turn_input(
    contract: &verlet_agent::contracts::CompiledThreadContract,
    initial_turn: &verlet_agent::contracts::ThreadInitialTurn,
    inputs: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::turn::TurnInput> {
    let mut input =
        crate::kernel::runtime_host::turn::TurnInput::text(initial_turn.content.clone())
            .with_metadata("thread_contract_name", contract.name.clone())
            .with_metadata("thread_contract_hash", contract.contract_hash()?)
            .with_metadata("thread_contract_source_hash", contract.source_hash.clone())
            .with_metadata("agent_contract_name", contract.name.clone())
            .with_metadata("agent_contract_hash", contract.contract_hash()?)
            .with_metadata("agent_contract_source_hash", contract.source_hash.clone());
    if !inputs.as_object().is_some_and(|object| object.is_empty()) {
        let input_json = serde_json::to_string(inputs).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "thread declaration inputs could not be encoded: {err}"
            ))
        })?;
        input = input.with_metadata("thread_contract_inputs_json", input_json.clone());
        input = input.with_metadata("agent_contract_inputs_json", input_json);
    }
    Ok(input)
}

impl RuntimeKernelControl {
    fn host(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeHost> {
        let inner = self.inner.upgrade().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(
                "runtime host is no longer available".to_string(),
            )
        })?;
        Ok(crate::kernel::runtime_host::RuntimeHost { inner })
    }

    pub async fn dispatch_thread_spawn(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        dispatch_id: verlet_runtime_contracts::handle::DispatchId,
        task_name: String,
        message: String,
        agent_ref: Option<String>,
        agent_resolver: Option<
            std::sync::Arc<dyn crate::agent::agent_process::KernelThreadSpawnAgentResolver>,
        >,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSpawnReceipt> {
        let host = self.host()?;
        let child_agent_ref = agent_ref
            .or_else(|| {
                agent_resolver
                    .as_ref()
                    .and_then(|resolver| resolver.default_agent_ref(caller))
            })
            .unwrap_or_else(|| "unbound".to_string());
        let mut projector = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host);
        if let Some(agent_resolver) = agent_resolver {
            projector = projector.with_agent_resolver(agent_resolver);
        }
        let submitted_turn_id = format!("thread-spawn-{dispatch_id}");
        let dispatched = projector
            .dispatch_request_with_authority(
                &caller.coordinates,
                verlet_history::ThreadSpawnRequestedPayload {
                    parent_thread_id: caller.coordinates.thread_id,
                    parent_turn_id: None,
                    task_name: Some(task_name.clone()),
                    submitted_turn_id: Some(submitted_turn_id.clone()),
                    child_agent_ref,
                    initial_submission: message,
                    correlation_id: dispatch_id.to_string(),
                    block_parent: false,
                },
                false,
            )
            .await?;
        let thread_id = verlet_runtime_contracts::ThreadId::parse_str(&dispatched.handle.id)
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "thread dispatch returned invalid handle: {err}"
                ))
            })?;
        let host = self.host()?;
        let status = match host.get_thread(thread_id).await {
            Ok(thread) => thread.status(),
            Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(_)) => {
                let executor = host.remote_thread_executor().await.ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::ThreadNotFound(thread_id)
                })?;
                executor.observe(thread_id).await?.status
            }
            Err(error) => return Err(error),
        };
        Ok(AgentProcessSpawnReceipt {
            operation: "cooldis.spawn_subagent".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            thread_id,
            parent_thread_id: caller.coordinates.thread_id,
            status,
            task_name: dispatched.task_name,
            submitted_turn_id: dispatched.submitted_turn_id.unwrap_or(submitted_turn_id),
            handle: dispatched.handle,
            dispatch_id: dispatched.dispatch_id,
        })
    }

    /// Resolve a child handle from the caller's model-facing `task_name` by
    /// folding the caller's durable control-stream spawn records. The returned
    /// runtime receipt carries the raw handle for kernel dispatch; callers must
    /// keep that identity out of model-visible projections.
    pub async fn resolve_child_task_name(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        task_name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::thread_spawn_projector::ThreadTaskNameResolutionReceipt,
    > {
        if task_name.trim().is_empty() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                "thread task_name must not be empty".to_string(),
            ));
        }
        let parent = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        let events = parent.read_control_events().await?;
        crate::kernel::thread_spawn_projector::fold_thread_task_name_resolution(
            &events,
            &caller.coordinates,
            task_name,
        )?
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "thread task_name {task_name:?} was not found under this parent"
            ))
        })
    }

    pub async fn spawn_child_with_witness(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        task_name: Option<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        metadata: std::collections::BTreeMap<String, String>,
        witness: ThreadSpawnWitness,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSpawnReceipt> {
        self.spawn_child_cancellation_safe(caller, task_name, input, metadata, witness, None)
            .await
    }

    pub(crate) async fn spawn_bound_child_with_witness(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        task_name: Option<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        metadata: std::collections::BTreeMap<String, String>,
        witness: ThreadSpawnWitness,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
        principal_id: String,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSpawnReceipt> {
        self.spawn_child_cancellation_safe(
            caller,
            task_name,
            input,
            metadata,
            witness,
            Some((compile_payload, bind_payload, principal_id)),
        )
        .await
    }

    async fn spawn_child_cancellation_safe(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        task_name: Option<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        metadata: std::collections::BTreeMap<String, String>,
        witness: ThreadSpawnWitness,
        manifest_payloads: Option<(serde_json::Value, serde_json::Value, String)>,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSpawnReceipt> {
        let control = self.clone();
        let caller = caller.clone();
        tokio::spawn(async move {
            control
                .spawn_child_inner(
                    &caller,
                    task_name,
                    input,
                    metadata,
                    witness,
                    manifest_payloads,
                )
                .await
        })
        .await
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "child spawn witness task failed: {err}"
            ))
        })?
    }

    async fn spawn_child_inner(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        task_name: Option<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        mut metadata: std::collections::BTreeMap<String, String>,
        witness: ThreadSpawnWitness,
        manifest_payloads: Option<(serde_json::Value, serde_json::Value, String)>,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSpawnReceipt> {
        let host = self.host()?;
        let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
            caller.coordinates.tenant_id.clone(),
            caller.coordinates.user_id.clone(),
            caller.coordinates.session_id.clone(),
        );
        let task_name = task_name.filter(|name| !name.trim().is_empty());
        metadata.insert("agent_process_v1".to_string(), "true".to_string());
        metadata.insert(
            "spawned_by_thread_id".to_string(),
            caller.coordinates.thread_id.to_string(),
        );
        if let Some(task_name) = &task_name {
            metadata.insert("task_name".to_string(), task_name.clone());
        }

        let child = host
            .start_thread_with_topology_and_metadata(
                coordinates,
                verlet_runtime_contracts::ThreadTopology::spawned_from(
                    caller.coordinates.thread_id,
                ),
                metadata,
            )
            .await?;
        let child_thread_id = child.context().coordinates.thread_id;
        let parent = host.get_thread(caller.coordinates.thread_id).await?;
        let dispatch_id = verlet_runtime_contracts::handle::DispatchId::new(
            witness.correlation_id.clone().unwrap_or_else(|| {
                format!(
                    "thread-spawn-{}",
                    verlet_runtime_contracts::ThreadSignalId::new()
                )
            }),
        );
        let turn_id = witness
            .submitted_turn_id
            .clone()
            .filter(|turn_id| !turn_id.trim().is_empty())
            .unwrap_or_else(|| {
                format!(
                    "agent-process-v1-{}",
                    verlet_runtime_contracts::ThreadSignalId::new()
                )
            });
        if let Err(err) =
            append_thread_spawned_event(&parent, caller, child.context(), witness).await
        {
            let _ = host.shutdown_thread(child_thread_id).await;
            return Err(err);
        }
        if let Some((compile_payload, bind_payload, principal_id)) = manifest_payloads
            && let Err(err) = child
                .record_manifest_receipts_for_principal(
                    compile_payload,
                    bind_payload,
                    &principal_id,
                )
                .await
        {
            let _ = host.shutdown_thread(child_thread_id).await;
            return Err(err);
        }
        if let Err(err) = host
            .submit_turn(child_thread_id, turn_id.clone(), input)
            .await
        {
            let _ = host.shutdown_thread(child_thread_id).await;
            return Err(err);
        }

        Ok(AgentProcessSpawnReceipt {
            operation: "cooldis.spawn_subagent".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            thread_id: child_thread_id,
            parent_thread_id: caller.coordinates.thread_id,
            status: child.status(),
            task_name,
            submitted_turn_id: turn_id,
            handle: verlet_runtime_contracts::handle::HandleId::thread(child_thread_id),
            dispatch_id,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn spawn_remote_child_with_witness(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        task_name: Option<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        mut metadata: std::collections::BTreeMap<String, String>,
        witness: ThreadSpawnWitness,
        compile_payload: Option<serde_json::Value>,
        bind_payload: Option<serde_json::Value>,
        binding_principal_id: Option<String>,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSpawnReceipt> {
        let host = self.host()?;
        let executor = host.remote_thread_executor().await.ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "placement target remote requires a served remote thread executor".to_string(),
            )
        })?;
        let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
            caller.coordinates.tenant_id.clone(),
            caller.coordinates.user_id.clone(),
            caller.coordinates.session_id.clone(),
        );
        let task_name = task_name.filter(|name| !name.trim().is_empty());
        metadata.insert("agent_process_v1".to_string(), "true".to_string());
        metadata.insert(
            "spawned_by_thread_id".to_string(),
            caller.coordinates.thread_id.to_string(),
        );
        if let Some(task_name) = &task_name {
            metadata.insert("task_name".to_string(), task_name.clone());
        }
        let child = verlet_runtime_contracts::ThreadContext::with_topology_and_metadata(
            coordinates,
            verlet_runtime_contracts::ThreadTopology::spawned_from(caller.coordinates.thread_id),
            metadata,
        );
        let parent = host.get_thread(caller.coordinates.thread_id).await?;
        let dispatch_id = verlet_runtime_contracts::handle::DispatchId::new(
            witness.correlation_id.clone().unwrap_or_else(|| {
                format!(
                    "thread-spawn-{}",
                    verlet_runtime_contracts::ThreadSignalId::new()
                )
            }),
        );
        let turn_id = witness
            .submitted_turn_id
            .clone()
            .filter(|turn_id| !turn_id.trim().is_empty())
            .unwrap_or_else(|| {
                format!(
                    "agent-process-v1-{}",
                    verlet_runtime_contracts::ThreadSignalId::new()
                )
            });
        let spawned = append_thread_spawned_event(&parent, caller, &child, witness).await?;
        let request = crate::daemon::remote_store::placement::RemoteThreadSpawnRequest {
            child: child.clone(),
            task_name: task_name.clone(),
            turn_id: turn_id.clone(),
            dispatch_id: dispatch_id.clone(),
            input,
            spawned_event_id: spawned.id,
            compile_payload,
            bind_payload,
            binding_principal_id,
        };
        if let Err(error) = executor.spawn(request).await {
            let _ = crate::kernel::runtime_host::runtime_services::append_thread_joined_first_wins(
                host.runtime_store().as_ref(),
                caller.coordinates.clone(),
                child.coordinates.clone(),
                spawned.id,
                verlet_history::ThreadTerminalState::Failed,
                Some(error.to_string()),
                Some("remote child process failed to start".to_string()),
                None,
                "executor:remote-thread",
                "remote_thread_spawn/v1",
            )
            .await;
            return Err(error);
        }
        Ok(AgentProcessSpawnReceipt {
            operation: "cooldis.spawn_subagent".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            thread_id: child.coordinates.thread_id,
            parent_thread_id: caller.coordinates.thread_id,
            status: verlet_runtime_contracts::ThreadStatus::Starting,
            task_name,
            submitted_turn_id: turn_id,
            handle: verlet_runtime_contracts::handle::HandleId::thread(child.coordinates.thread_id),
            dispatch_id,
        })
    }

    pub async fn declare_thread(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        declaration: verlet_agent::contracts::ThreadDeclaration,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_agent::contracts::ThreadHandle> {
        declaration.validate()?;
        let contract = compile_declaration_contract(&declaration.contract)?;
        let contract_hash = contract.contract_hash()?;
        let compile_receipt = contract_hash.clone();
        let host = self.host()?;
        let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
            caller.coordinates.tenant_id.clone(),
            caller.coordinates.user_id.clone(),
            caller.coordinates.session_id.clone(),
        );
        let parent_thread_id = declaration
            .topology
            .as_ref()
            .and_then(|topology| topology.spawned_from)
            .unwrap_or(caller.coordinates.thread_id);
        if parent_thread_id != caller.coordinates.thread_id {
            self.scoped_thread(caller, parent_thread_id).await?;
        }
        let topology = verlet_runtime_contracts::ThreadTopology::spawned_from(parent_thread_id);
        let propagator = declaration.propagator.clone().unwrap_or_else(|| {
            verlet_agent::contracts::ThreadPropagatorSelection::from_runtime_hint(
                contract.runtime.get("propagator"),
            )
        });
        let mut metadata = declaration.metadata.clone();
        metadata.insert("thread_contract_v0".to_string(), "true".to_string());
        metadata.insert("agent_contract_v0".to_string(), "true".to_string());
        metadata.insert(
            "spawned_by_thread_id".to_string(),
            parent_thread_id.to_string(),
        );
        metadata.insert("thread_contract_name".to_string(), contract.name.clone());
        metadata.insert("thread_contract_hash".to_string(), contract_hash.clone());
        metadata.insert(
            "thread_contract_source_hash".to_string(),
            contract.source_hash.clone(),
        );
        metadata.insert(
            "thread_propagator_kind".to_string(),
            propagator.kind.clone(),
        );
        if let Some(name) = &propagator.name {
            metadata.insert("thread_propagator_name".to_string(), name.clone());
        }
        metadata.insert("agent_contract_name".to_string(), contract.name.clone());
        metadata.insert("agent_contract_hash".to_string(), contract_hash.clone());
        metadata.insert(
            "agent_contract_source_hash".to_string(),
            contract.source_hash.clone(),
        );

        let child = host
            .start_thread_with_topology_and_metadata(coordinates, topology, metadata)
            .await?;
        let child_thread_id = child.context().coordinates.thread_id;
        let turn_id = format!(
            "thread-contract-v0-{}",
            verlet_runtime_contracts::ThreadSignalId::new()
        );
        let input =
            declaration_turn_input(&contract, &declaration.initial_turn, &declaration.inputs)?;
        if let Err(err) = host
            .submit_turn(child_thread_id, turn_id.clone(), input)
            .await
        {
            let _ = host.shutdown_thread(child_thread_id).await;
            return Err(err);
        }
        let spawn_receipt = verlet_agent::contracts::sha256_hex(
            serde_json::json!({
                "kind": "cooldis.thread-spawn-receipt",
                "version": 0,
                "caller_thread_id": caller.coordinates.thread_id,
                "parent_thread_id": parent_thread_id,
                "thread_id": child_thread_id,
                "propagator": propagator.clone(),
                "contract_hash": contract_hash,
                "submitted_turn_id": turn_id,
            })
            .to_string()
            .as_bytes(),
        );

        Ok(verlet_agent::contracts::ThreadHandle {
            kind: verlet_agent::contracts::THREAD_HANDLE_KIND.to_string(),
            version: 0,
            thread_id: child_thread_id,
            status: child.status(),
            propagator,
            contract_hash,
            submitted_turn_id: turn_id,
            receipts: verlet_agent::contracts::ThreadReceiptSet {
                compile: compile_receipt,
                spawn: spawn_receipt,
            },
        })
    }

    pub async fn declare_agent_thread(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        declaration: verlet_agent::contracts::ThreadDeclaration,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_agent::contracts::ThreadHandle> {
        self.declare_thread(caller, declaration).await
    }

    pub async fn submit_to_thread(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: Option<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSubmitReceipt> {
        let turn_id = turn_id
            .filter(|turn_id| !turn_id.trim().is_empty())
            .unwrap_or_else(|| {
                format!(
                    "agent-process-v1-{}",
                    verlet_runtime_contracts::ThreadSignalId::new()
                )
            });
        self.submit_to_thread_with_identities(
            caller,
            target_thread_id,
            verlet_runtime_contracts::handle::DispatchId::new(format!(
                "thread-submit-{}",
                verlet_runtime_contracts::ThreadSignalId::new()
            )),
            turn_id,
            verlet_runtime_contracts::RuntimeEventId::new(),
            input,
        )
        .await
    }

    /// Submits through the target-scoped local dispatch fold. The reserved
    /// turn id is derived solely from the dispatch id; callers cannot replace
    /// the fold key with an organic turn identity.
    pub async fn submit_to_thread_with_dispatch(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        dispatch_id: verlet_runtime_contracts::handle::DispatchId,
        input: crate::kernel::runtime_host::turn::TurnInput,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSubmitReceipt> {
        let turn_id = format!("thread-submit-{dispatch_id}");
        let interaction_id = submit_dispatch_interaction_id(target_thread_id, &dispatch_id);
        self.submit_to_thread_with_identities(
            caller,
            target_thread_id,
            dispatch_id,
            turn_id,
            interaction_id,
            input,
        )
        .await
    }

    async fn submit_to_thread_with_identities(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        dispatch_id: verlet_runtime_contracts::handle::DispatchId,
        turn_id: String,
        interaction_id: verlet_runtime_contracts::RuntimeEventId,
        input: crate::kernel::runtime_host::turn::TurnInput,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessSubmitReceipt> {
        let host = self.host()?;
        let caller_thread = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        if let Some((executor, _)) = self
            .scoped_remote_thread_executor(caller, target_thread_id)
            .await?
        {
            let status = executor
                .submit(
                    crate::daemon::remote_store::placement::RemoteThreadSubmitRequest {
                        target_thread_id,
                        turn_id: turn_id.clone(),
                        dispatch_id: dispatch_id.clone(),
                        input,
                    },
                )
                .await?;
            let metadata = std::collections::BTreeMap::from([
                (
                    "operation".to_string(),
                    "cooldis.submit_to_thread".to_string(),
                ),
                (
                    "mode".to_string(),
                    verlet_runtime_contracts::TurnSubmissionMode::Queue.to_string(),
                ),
            ]);
            crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
                &caller_thread,
                interaction_id,
                verlet_runtime_contracts::ThreadInteractionKind::PromptSubmitted,
                caller.coordinates.thread_id,
                target_thread_id,
                None,
                Some(turn_id.clone()),
                None,
                metadata,
            );
            return Ok(AgentProcessSubmitReceipt {
                operation: "cooldis.submit_to_thread".to_string(),
                caller_thread_id: caller.coordinates.thread_id,
                target_thread_id,
                interaction_id,
                status,
                turn_id,
                dispatch_id,
            });
        }
        let target = self.scoped_thread(caller, target_thread_id).await?;
        let admission = crate::kernel::admission::AdmissionGateContext::surface_default(
            crate::kernel::admission::KERNEL_THREAD_SUBMIT_SURFACE,
            Vec::new(),
        )?;
        let reserved = crate::kernel::admission::reserve_turn(
            &host,
            target_thread_id,
            turn_id.clone(),
            input,
            verlet_runtime_contracts::TurnSubmissionMode::Queue,
            Some(admission),
        )
        .await?;
        let submitted = crate::kernel::admission::submit_reserved(reserved).await;
        let metadata = std::collections::BTreeMap::from([
            (
                "operation".to_string(),
                "cooldis.submit_to_thread".to_string(),
            ),
            (
                "mode".to_string(),
                verlet_runtime_contracts::TurnSubmissionMode::Queue.to_string(),
            ),
        ]);
        if submitted {
            crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
                &caller_thread,
                interaction_id,
                verlet_runtime_contracts::ThreadInteractionKind::PromptSubmitted,
                caller.coordinates.thread_id,
                target_thread_id,
                None,
                Some(turn_id.clone()),
                None,
                metadata.clone(),
            );
            crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
                &target,
                interaction_id,
                verlet_runtime_contracts::ThreadInteractionKind::PromptReceived,
                caller.coordinates.thread_id,
                target_thread_id,
                None,
                Some(turn_id.clone()),
                None,
                metadata,
            );
        }
        Ok(AgentProcessSubmitReceipt {
            operation: "cooldis.submit_to_thread".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            target_thread_id,
            interaction_id,
            status: target.status(),
            turn_id,
            dispatch_id,
        })
    }

    pub async fn wait_thread(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        timeout_ms: Option<u64>,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessWaitReceipt> {
        if target_thread_id == caller.coordinates.thread_id {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                "Agent Process V1 cannot wait on the invoking thread".to_string(),
            ));
        }
        let caller_thread = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        if let Some((executor, _)) = self
            .scoped_remote_thread_executor(caller, target_thread_id)
            .await?
        {
            let waited = executor.wait(target_thread_id, timeout_ms).await?;
            let result_interaction_id = if !waited.timed_out {
                waited
                    .observation
                    .latest_output
                    .as_ref()
                    .map(|latest_output| {
                        let interaction_id = verlet_runtime_contracts::RuntimeEventId::new();
                        crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
                            &caller_thread,
                            interaction_id,
                            verlet_runtime_contracts::ThreadInteractionKind::ResultAttached,
                            target_thread_id,
                            caller.coordinates.thread_id,
                            None,
                            None,
                            Some(crate::kernel::runtime_host::runtime_utils::thread_interaction_preview(latest_output)),
                            std::collections::BTreeMap::from([(
                                "operation".to_string(),
                                "cooldis.wait_thread".to_string(),
                            )]),
                        );
                        interaction_id
                    })
            } else {
                None
            };
            return Ok(AgentProcessWaitReceipt {
                operation: "cooldis.wait_thread".to_string(),
                caller_thread_id: caller.coordinates.thread_id,
                target_thread_id,
                status: waited.observation.status,
                timed_out: waited.timed_out,
                latest_output: waited.observation.latest_output,
                result_interaction_id,
            });
        }
        let target = self.scoped_thread(caller, target_thread_id).await?;
        let timed_out = match timeout_ms {
            Some(timeout_ms) => tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                crate::kernel::runtime_host::runtime_utils::wait_until_thread_settled(&target),
            )
            .await
            .is_err(),
            None => {
                crate::kernel::runtime_host::runtime_utils::wait_until_thread_settled(&target)
                    .await;
                false
            }
        };
        let latest_output = target.session_context().await.ok().and_then(|context| {
            crate::kernel::runtime_host::runtime_utils::latest_message_text(&context.messages)
        });
        let result_interaction_id = if !timed_out {
            latest_output.as_ref().map(|latest_output| {
                let interaction_id = verlet_runtime_contracts::RuntimeEventId::new();
                crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
                    &caller_thread,
                    interaction_id,
                    verlet_runtime_contracts::ThreadInteractionKind::ResultAttached,
                    target_thread_id,
                    caller.coordinates.thread_id,
                    None,
                    None,
                    Some(
                        crate::kernel::runtime_host::runtime_utils::thread_interaction_preview(
                            latest_output,
                        ),
                    ),
                    std::collections::BTreeMap::from([(
                        "operation".to_string(),
                        "cooldis.wait_thread".to_string(),
                    )]),
                );
                interaction_id
            })
        } else {
            None
        };
        Ok(AgentProcessWaitReceipt {
            operation: "cooldis.wait_thread".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            target_thread_id,
            status: target.status(),
            timed_out,
            latest_output,
            result_interaction_id,
        })
    }

    pub async fn cancel_thread(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        reason: String,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessLifecycleReceipt> {
        if target_thread_id == caller.coordinates.thread_id {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                "Agent Process V1 cannot cancel the invoking thread through its own control call"
                    .to_string(),
            ));
        }
        let host = self.host()?;
        let caller_thread = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        let interaction_id = verlet_runtime_contracts::RuntimeEventId::new();
        let metadata = std::collections::BTreeMap::from([
            ("operation".to_string(), "cooldis.cancel_thread".to_string()),
            ("reason".to_string(), reason.clone()),
        ]);
        crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
            &caller_thread,
            interaction_id,
            verlet_runtime_contracts::ThreadInteractionKind::ControlRequested,
            caller.coordinates.thread_id,
            target_thread_id,
            None,
            None,
            None,
            metadata.clone(),
        );
        crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
            &target,
            interaction_id,
            verlet_runtime_contracts::ThreadInteractionKind::ControlRequested,
            caller.coordinates.thread_id,
            target_thread_id,
            None,
            None,
            None,
            metadata,
        );
        host.cancel(target_thread_id, reason).await?;
        host.shutdown_thread(target_thread_id).await?;
        Ok(AgentProcessLifecycleReceipt {
            operation: "cooldis.cancel_thread".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            target_thread_id,
            status: verlet_runtime_contracts::ThreadLifecycleStatus::from(target.status()),
        })
    }

    pub async fn shutdown_thread(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessLifecycleReceipt> {
        if target_thread_id == caller.coordinates.thread_id {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                "Agent Process V1 cannot shut down the invoking thread through its own control call"
                    .to_string(),
            ));
        }
        let host = self.host()?;
        let caller_thread = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        let interaction_id = verlet_runtime_contracts::RuntimeEventId::new();
        let metadata = std::collections::BTreeMap::from([(
            "operation".to_string(),
            "cooldis.shutdown_thread".to_string(),
        )]);
        crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
            &caller_thread,
            interaction_id,
            verlet_runtime_contracts::ThreadInteractionKind::ControlRequested,
            caller.coordinates.thread_id,
            target_thread_id,
            None,
            None,
            None,
            metadata.clone(),
        );
        crate::kernel::runtime_host::runtime_utils::emit_thread_interaction(
            &target,
            interaction_id,
            verlet_runtime_contracts::ThreadInteractionKind::ControlRequested,
            caller.coordinates.thread_id,
            target_thread_id,
            None,
            None,
            None,
            metadata,
        );
        host.shutdown_thread(target_thread_id).await?;
        Ok(AgentProcessLifecycleReceipt {
            operation: "cooldis.shutdown_thread".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            target_thread_id,
            status: verlet_runtime_contracts::ThreadLifecycleStatus::Stopped,
        })
    }

    pub async fn create_checkpoint(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        parent_checkpoint_id: Option<verlet_runtime_contracts::ThreadCheckpointId>,
        label: Option<String>,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessCheckpointReceipt> {
        let host = self.host()?;
        self.scoped_thread(caller, target_thread_id).await?;
        let checkpoint = host
            .create_checkpoint(target_thread_id, parent_checkpoint_id, label, metadata)
            .await?;
        Ok(AgentProcessCheckpointReceipt {
            operation: "cooldis.create_checkpoint".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            target_thread_id,
            checkpoint_id: checkpoint.id,
            label: checkpoint.label,
        })
    }

    pub async fn thread_status(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessStatusReceipt> {
        if let Some((executor, remote_context)) = self
            .scoped_remote_thread_executor(caller, target_thread_id)
            .await?
        {
            let observation = executor.observe(target_thread_id).await?;
            return Ok(AgentProcessStatusReceipt {
                operation: "cooldis.thread_status".to_string(),
                caller_thread_id: caller.coordinates.thread_id,
                target_thread_id,
                parent_thread_id: remote_context.parent_thread_id,
                status: observation.status,
            });
        }
        let target = self.scoped_thread(caller, target_thread_id).await?;
        Ok(AgentProcessStatusReceipt {
            operation: "cooldis.thread_status".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            target_thread_id,
            parent_thread_id: target.context().parent_thread_id,
            status: target.status(),
        })
    }

    pub async fn children_of(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<AgentProcessChildrenReceipt> {
        let host = self.host()?;
        self.scoped_thread(caller, target_thread_id).await?;
        let mut children = Vec::new();
        for child in host.children_of(target_thread_id).await {
            let actual_scope = child.context().coordinates.scope();
            if actual_scope == caller.coordinates.scope() {
                children.push(AgentProcessChildRef {
                    thread_id: child.context().coordinates.thread_id,
                    parent_thread_id: child.context().parent_thread_id,
                    status: child.status(),
                });
            }
        }
        children.sort_by_key(|child| child.thread_id.to_string());
        Ok(AgentProcessChildrenReceipt {
            operation: "cooldis.children_of".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            target_thread_id,
            children,
        })
    }

    pub async fn record_manifest_receipts_for_thread(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<(
        verlet_history::EventRecord,
        verlet_history::EventRecord,
    )> {
        let target = self.scoped_thread(caller, target_thread_id).await?;
        target
            .record_manifest_receipts(compile_payload, bind_payload)
            .await
    }

    pub async fn start_mandate(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        mut request: crate::kernel::mandate_lifecycle::MandateStartRequest,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::mandate_lifecycle::MandateStartReceipt,
    > {
        let host = self.host()?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        if request.snapshot_id.is_none() {
            request.snapshot_id = target
                .context()
                .metadata
                .get("cooldis.agent.manifest_hash")
                .cloned();
        }
        crate::kernel::mandate_lifecycle::start_mandate(
            host.runtime_store().as_ref(),
            &target.context().coordinates,
            request,
            chrono::Utc::now(),
        )
        .await
    }

    pub async fn revoke_mandate(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        mandate_event_id: verlet_history::EventRecordId,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::mandate_lifecycle::MandateRevokeReceipt,
    > {
        let host = self.host()?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        crate::kernel::mandate_lifecycle::revoke_mandate(
            host.runtime_store().as_ref(),
            &target.context().coordinates,
            mandate_event_id,
        )
        .await
    }

    pub async fn list_mandates(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<
        Vec<crate::kernel::mandate_lifecycle::ActiveMandate>,
    > {
        let host = self.host()?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        crate::kernel::mandate_lifecycle::list_active_mandates(
            host.runtime_store().as_ref(),
            &target.context().coordinates,
        )
        .await
    }

    pub async fn caller_session_context(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::SessionContext> {
        let target = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        target.session_context().await
    }

    pub async fn caller_thread_events(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        from_sequence: Option<verlet_history::EventSequence>,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
        let target = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        target.read_thread_events(from_sequence).await
    }

    pub async fn caller_control_events(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
        let target = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        target.read_control_events().await
    }

    pub async fn append_caller_thread_event(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        record: verlet_history::NewEventRecord,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
        let target = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        target.append_thread_event_record(record).await
    }

    async fn scoped_thread(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        let host = self.host()?;
        let target = host.get_thread(target_thread_id).await?;
        ensure_thread_scope(caller, &target.context().coordinates)?;
        Ok(target)
    }

    async fn scoped_remote_thread_executor(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        target_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<(
            std::sync::Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>,
            verlet_runtime_contracts::ThreadContext,
        )>,
    > {
        let Some(executor) = self.host()?.remote_thread_executor().await else {
            return Ok(None);
        };
        let Some(context) = executor.context(target_thread_id).await else {
            return Ok(None);
        };
        ensure_thread_scope(caller, &context.coordinates)?;
        Ok(Some((executor, context)))
    }
}

fn ensure_thread_scope(
    caller: &verlet_runtime_contracts::ThreadContext,
    target: &verlet_runtime_contracts::ThreadCoordinates,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let requested = caller.coordinates.scope();
    let actual = target.scope();
    if requested != actual {
        return Err(
            crate::kernel::runtime_host::VerletError::ThreadScopeMismatch {
                thread_id: target.thread_id,
                requested: Box::new(requested),
                actual: Box::new(actual),
            },
        );
    }
    Ok(())
}

/// Derives the target-scoped interaction identity for a local submit fold.
/// Version 8 keeps deterministic dispatch interactions disjoint from organic
/// runtime event ids, which are generated as UUIDv7.
fn submit_dispatch_interaction_id(
    target_thread_id: verlet_runtime_contracts::ThreadId,
    dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
) -> verlet_runtime_contracts::RuntimeEventId {
    let digest =
        verlet_agent::contracts::sha256_hex(format!("{target_thread_id}:{dispatch_id}").as_bytes());
    let digest = digest.strip_prefix("sha256:").unwrap_or(&digest);
    let mut value =
        u128::from_str_radix(&digest[..32], 16).expect("sha256 hex prefix is always a valid u128");
    value = (value & !(0xf_u128 << 76)) | (8_u128 << 76);
    value = (value & !(0b11_u128 << 62)) | (0b10_u128 << 62);
    verlet_runtime_contracts::RuntimeEventId::from_uuid(uuid::Uuid::from_u128(value))
}

#[cfg(test)]
mod remote_scope_tests {

    #[test]
    fn remote_execution_preserves_the_local_thread_scope_fence() {
        let caller = verlet_runtime_contracts::ThreadContext::root(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session-a"),
        );
        let target = verlet_runtime_contracts::ThreadContext::with_topology(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session-b"),
            verlet_runtime_contracts::ThreadTopology::spawned_from(caller.coordinates.thread_id),
        );
        let error = crate::kernel::runtime_host::kernel_control::ensure_thread_scope(
            &caller,
            &target.coordinates,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            crate::kernel::runtime_host::VerletError::ThreadScopeMismatch { thread_id, .. }
                if thread_id == target.coordinates.thread_id
        ));
    }
}
