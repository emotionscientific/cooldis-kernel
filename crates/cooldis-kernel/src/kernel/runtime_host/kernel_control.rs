use super::runtime_utils::{
    emit_thread_interaction, latest_message_text, thread_interaction_preview,
    wait_until_thread_settled,
};
use super::{
    CooldisError, CooldisResult, RuntimeHost, RuntimeHostInner, RuntimeThreadHandle, TurnInput,
};
use crate::agent::contracts::{
    CompiledThreadContract, THREAD_HANDLE_KIND, ThreadContractCompiler, ThreadContractReference,
    ThreadContractSource, ThreadDeclaration, ThreadHandle, ThreadInitialTurn,
    ThreadPropagatorSelection, ThreadReceiptSet, sha256_hex,
};
use crate::agent::manifest_bind::{BoundCouplingSet, coupling_set_content_hash};
use crate::kernel::history::{
    EventKind, EventRecord, EventRecordId, NewEventRecord, ThreadSpawnedPayload,
};
use crate::kernel::mandate_lifecycle::{
    ActiveMandate, MandateRevokeReceipt, MandateStartReceipt, MandateStartRequest,
    list_active_mandates, revoke_mandate, start_mandate,
};
use crate::kernel::runtime_host::{
    THREAD_AGENT_MANIFEST_HASH_METADATA, THREAD_BOUND_COUPLING_SET_METADATA,
    THREAD_SPAWN_GRANTED_METADATA, THREAD_SPAWN_INPUTS_HASH_METADATA,
};
use cooldis_runtime_contracts::{
    RuntimeEventId, ThreadCheckpointId, ThreadContext, ThreadCoordinates, ThreadId,
    ThreadInteractionKind, ThreadLifecycleStatus, ThreadSignalId, ThreadStatus, ThreadTopology,
    TurnSubmissionMode,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Weak;
use std::time::Duration;

#[derive(Clone)]
pub struct RuntimeKernelControl {
    inner: Weak<RuntimeHostInner>,
}

impl RuntimeKernelControl {
    pub(super) fn new(inner: Weak<RuntimeHostInner>) -> Self {
        Self { inner }
    }
}

async fn append_thread_spawned_event(
    parent: &RuntimeThreadHandle,
    caller: &ThreadContext,
    child: &RuntimeThreadHandle,
) -> CooldisResult<EventRecord> {
    let metadata = &child.context().metadata;
    let child_manifest_hash = metadata
        .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
        .cloned()
        .unwrap_or_else(|| "unbound".to_string());
    let granted = metadata
        .get(THREAD_SPAWN_GRANTED_METADATA)
        .map(|raw| {
            serde_json::from_str::<Vec<String>>(raw).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "thread_spawn granted metadata is invalid: {err}"
                ))
            })
        })
        .transpose()?
        .unwrap_or_default();
    let inputs_hash = metadata
        .get(THREAD_SPAWN_INPUTS_HASH_METADATA)
        .cloned()
        .unwrap_or_else(|| {
            format!(
                "sha256:{}",
                sha256_hex(child.context().coordinates.thread_id.to_string().as_bytes())
            )
        });
    let child_policy_hash = metadata
        .get(THREAD_BOUND_COUPLING_SET_METADATA)
        .map(|raw| {
            serde_json::from_str::<BoundCouplingSet>(raw)
                .map_err(|err| {
                    CooldisError::RuntimeFactory(format!(
                        "thread bound coupling set is invalid: {err}"
                    ))
                })
                .and_then(|coupling_set| coupling_set_content_hash(&coupling_set))
        })
        .transpose()?;
    let payload = ThreadSpawnedPayload {
        parent_thread_id: caller.coordinates.thread_id,
        parent_turn_id: None,
        child_thread_id: child.context().coordinates.thread_id,
        child_manifest_hash,
        child_policy_hash,
        granted,
        inputs_hash,
    };
    let mut value = serde_json::to_value(payload).map_err(|err| {
        CooldisError::History(format!("thread.spawned payload codec failed: {err}"))
    })?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(EventKind::ThreadSpawned.payload_schema_id()),
        );
    }
    parent
        .append_control_event(NewEventRecord::witnessed(
            caller.coordinates.clone(),
            EventKind::ThreadSpawned,
            value,
        ))
        .await
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentProcessSpawnReceipt {
    pub operation: String,
    pub caller_thread_id: ThreadId,
    pub thread_id: ThreadId,
    pub parent_thread_id: ThreadId,
    pub status: ThreadStatus,
    pub task_name: Option<String>,
    pub submitted_turn_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentProcessSubmitReceipt {
    pub operation: String,
    pub caller_thread_id: ThreadId,
    pub target_thread_id: ThreadId,
    pub interaction_id: RuntimeEventId,
    pub status: ThreadStatus,
    pub turn_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentProcessWaitReceipt {
    pub operation: String,
    pub caller_thread_id: ThreadId,
    pub target_thread_id: ThreadId,
    pub status: ThreadStatus,
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_output: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_interaction_id: Option<RuntimeEventId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentProcessLifecycleReceipt {
    pub operation: String,
    pub caller_thread_id: ThreadId,
    pub target_thread_id: ThreadId,
    pub status: ThreadLifecycleStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentProcessCheckpointReceipt {
    pub operation: String,
    pub caller_thread_id: ThreadId,
    pub target_thread_id: ThreadId,
    pub checkpoint_id: ThreadCheckpointId,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentProcessStatusReceipt {
    pub operation: String,
    pub caller_thread_id: ThreadId,
    pub target_thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<ThreadId>,
    pub status: ThreadStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentProcessChildRef {
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_thread_id: Option<ThreadId>,
    pub status: ThreadStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentProcessChildrenReceipt {
    pub operation: String,
    pub caller_thread_id: ThreadId,
    pub target_thread_id: ThreadId,
    pub children: Vec<AgentProcessChildRef>,
}

fn compile_declaration_contract(
    reference: &ThreadContractReference,
) -> CooldisResult<CompiledThreadContract> {
    if let Some(compiled) = &reference.compiled {
        compiled.validate()?;
        return Ok(compiled.clone());
    }
    if let Some(source) = &reference.inline {
        return Ok(ThreadContractCompiler::compile(
            &ThreadContractSource::markdown(source.clone()),
        )?);
    }
    if let Some(ref_path) = &reference.ref_path {
        return Err(CooldisError::RuntimeExecution(format!(
            "thread contract ref {ref_path:?} must be resolved before RuntimeKernelControl::declare_thread"
        )));
    }
    Err(CooldisError::RuntimeExecution(
        "thread contract reference is empty".to_string(),
    ))
}

fn declaration_turn_input(
    contract: &CompiledThreadContract,
    initial_turn: &ThreadInitialTurn,
    inputs: &serde_json::Value,
) -> CooldisResult<TurnInput> {
    let mut input = TurnInput::text(initial_turn.content.clone())
        .with_metadata("thread_contract_name", contract.name.clone())
        .with_metadata("thread_contract_hash", contract.contract_hash()?)
        .with_metadata("thread_contract_source_hash", contract.source_hash.clone())
        .with_metadata("agent_contract_name", contract.name.clone())
        .with_metadata("agent_contract_hash", contract.contract_hash()?)
        .with_metadata("agent_contract_source_hash", contract.source_hash.clone());
    if !inputs.as_object().is_some_and(|object| object.is_empty()) {
        let input_json = serde_json::to_string(inputs).map_err(|err| {
            CooldisError::RuntimeExecution(format!(
                "thread declaration inputs could not be encoded: {err}"
            ))
        })?;
        input = input.with_metadata("thread_contract_inputs_json", input_json.clone());
        input = input.with_metadata("agent_contract_inputs_json", input_json);
    }
    Ok(input)
}

impl RuntimeKernelControl {
    fn host(&self) -> CooldisResult<RuntimeHost> {
        let inner = self.inner.upgrade().ok_or_else(|| {
            CooldisError::RuntimeExecution("runtime host is no longer available".to_string())
        })?;
        Ok(RuntimeHost { inner })
    }

    // lexicon-allow: subagent - public compatibility method for existing agent-process callers.
    pub async fn spawn_subagent(
        &self,
        caller: &ThreadContext,
        task_name: Option<String>,
        input: TurnInput,
        mut metadata: BTreeMap<String, String>,
    ) -> CooldisResult<AgentProcessSpawnReceipt> {
        let host = self.host()?;
        let coordinates = ThreadCoordinates::new(
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
                ThreadTopology::spawned_from(caller.coordinates.thread_id),
                metadata,
            )
            .await?;
        let child_thread_id = child.context().coordinates.thread_id;
        let parent = host.get_thread(caller.coordinates.thread_id).await?;
        if let Err(err) = append_thread_spawned_event(&parent, caller, &child).await {
            let _ = host.shutdown_thread(child_thread_id).await;
            return Err(err);
        }
        let turn_id = format!("agent-process-v1-{}", ThreadSignalId::new());
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
        })
    }

    pub async fn declare_thread(
        &self,
        caller: &ThreadContext,
        declaration: ThreadDeclaration,
    ) -> CooldisResult<ThreadHandle> {
        declaration.validate()?;
        let contract = compile_declaration_contract(&declaration.contract)?;
        let contract_hash = contract.contract_hash()?;
        let compile_receipt = contract_hash.clone();
        let host = self.host()?;
        let coordinates = ThreadCoordinates::new(
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
        let topology = ThreadTopology::spawned_from(parent_thread_id);
        let propagator = declaration.propagator.clone().unwrap_or_else(|| {
            ThreadPropagatorSelection::from_runtime_hint(contract.runtime.get("propagator"))
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
        let turn_id = format!("thread-contract-v0-{}", ThreadSignalId::new());
        let input =
            declaration_turn_input(&contract, &declaration.initial_turn, &declaration.inputs)?;
        if let Err(err) = host
            .submit_turn(child_thread_id, turn_id.clone(), input)
            .await
        {
            let _ = host.shutdown_thread(child_thread_id).await;
            return Err(err);
        }
        let spawn_receipt = sha256_hex(
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

        Ok(ThreadHandle {
            kind: THREAD_HANDLE_KIND.to_string(),
            version: 0,
            thread_id: child_thread_id,
            status: child.status(),
            propagator,
            contract_hash,
            submitted_turn_id: turn_id,
            receipts: ThreadReceiptSet {
                compile: compile_receipt,
                spawn: spawn_receipt,
            },
        })
    }

    pub async fn declare_agent_thread(
        &self,
        caller: &ThreadContext,
        declaration: ThreadDeclaration,
    ) -> CooldisResult<ThreadHandle> {
        self.declare_thread(caller, declaration).await
    }

    pub async fn submit_to_thread(
        &self,
        caller: &ThreadContext,
        target_thread_id: ThreadId,
        turn_id: Option<String>,
        input: TurnInput,
    ) -> CooldisResult<AgentProcessSubmitReceipt> {
        let host = self.host()?;
        let caller_thread = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        let turn_id = turn_id
            .filter(|turn_id| !turn_id.trim().is_empty())
            .unwrap_or_else(|| format!("agent-process-v1-{}", ThreadSignalId::new()));
        let interaction_id = RuntimeEventId::new();
        host.submit_turn(target_thread_id, turn_id.clone(), input)
            .await?;
        let metadata = BTreeMap::from([
            (
                "operation".to_string(),
                "cooldis.submit_to_thread".to_string(),
            ),
            (
                "mode".to_string(),
                TurnSubmissionMode::Queue.as_str().to_string(),
            ),
        ]);
        emit_thread_interaction(
            &caller_thread,
            interaction_id,
            ThreadInteractionKind::PromptSubmitted,
            caller.coordinates.thread_id,
            target_thread_id,
            None,
            Some(turn_id.clone()),
            None,
            metadata.clone(),
        );
        emit_thread_interaction(
            &target,
            interaction_id,
            ThreadInteractionKind::PromptReceived,
            caller.coordinates.thread_id,
            target_thread_id,
            None,
            Some(turn_id.clone()),
            None,
            metadata,
        );
        Ok(AgentProcessSubmitReceipt {
            operation: "cooldis.submit_to_thread".to_string(),
            caller_thread_id: caller.coordinates.thread_id,
            target_thread_id,
            interaction_id,
            status: target.status(),
            turn_id,
        })
    }

    pub async fn wait_thread(
        &self,
        caller: &ThreadContext,
        target_thread_id: ThreadId,
        timeout_ms: Option<u64>,
    ) -> CooldisResult<AgentProcessWaitReceipt> {
        if target_thread_id == caller.coordinates.thread_id {
            return Err(CooldisError::RuntimeExecution(
                "Agent Process V1 cannot wait on the invoking thread".to_string(),
            ));
        }
        let caller_thread = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        let timed_out = match timeout_ms {
            Some(timeout_ms) => tokio::time::timeout(
                Duration::from_millis(timeout_ms),
                wait_until_thread_settled(&target),
            )
            .await
            .is_err(),
            None => {
                wait_until_thread_settled(&target).await;
                false
            }
        };
        let latest_output = target
            .session_context()
            .await
            .ok()
            .and_then(|context| latest_message_text(&context.messages));
        let result_interaction_id = if !timed_out {
            latest_output.as_ref().map(|latest_output| {
                let interaction_id = RuntimeEventId::new();
                emit_thread_interaction(
                    &caller_thread,
                    interaction_id,
                    ThreadInteractionKind::ResultAttached,
                    target_thread_id,
                    caller.coordinates.thread_id,
                    None,
                    None,
                    Some(thread_interaction_preview(latest_output)),
                    BTreeMap::from([("operation".to_string(), "cooldis.wait_thread".to_string())]),
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
        caller: &ThreadContext,
        target_thread_id: ThreadId,
        reason: String,
    ) -> CooldisResult<AgentProcessLifecycleReceipt> {
        if target_thread_id == caller.coordinates.thread_id {
            return Err(CooldisError::RuntimeExecution(
                "Agent Process V1 cannot cancel the invoking thread through its own control call"
                    .to_string(),
            ));
        }
        let host = self.host()?;
        let caller_thread = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        let interaction_id = RuntimeEventId::new();
        let metadata = BTreeMap::from([
            ("operation".to_string(), "cooldis.cancel_thread".to_string()),
            ("reason".to_string(), reason.clone()),
        ]);
        emit_thread_interaction(
            &caller_thread,
            interaction_id,
            ThreadInteractionKind::ControlRequested,
            caller.coordinates.thread_id,
            target_thread_id,
            None,
            None,
            None,
            metadata.clone(),
        );
        emit_thread_interaction(
            &target,
            interaction_id,
            ThreadInteractionKind::ControlRequested,
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
            status: ThreadLifecycleStatus::from(target.status()),
        })
    }

    pub async fn shutdown_thread(
        &self,
        caller: &ThreadContext,
        target_thread_id: ThreadId,
    ) -> CooldisResult<AgentProcessLifecycleReceipt> {
        if target_thread_id == caller.coordinates.thread_id {
            return Err(CooldisError::RuntimeExecution(
                "Agent Process V1 cannot shut down the invoking thread through its own control call"
                    .to_string(),
            ));
        }
        let host = self.host()?;
        let caller_thread = self
            .scoped_thread(caller, caller.coordinates.thread_id)
            .await?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        let interaction_id = RuntimeEventId::new();
        let metadata = BTreeMap::from([(
            "operation".to_string(),
            "cooldis.shutdown_thread".to_string(),
        )]);
        emit_thread_interaction(
            &caller_thread,
            interaction_id,
            ThreadInteractionKind::ControlRequested,
            caller.coordinates.thread_id,
            target_thread_id,
            None,
            None,
            None,
            metadata.clone(),
        );
        emit_thread_interaction(
            &target,
            interaction_id,
            ThreadInteractionKind::ControlRequested,
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
            status: ThreadLifecycleStatus::Stopped,
        })
    }

    pub async fn create_checkpoint(
        &self,
        caller: &ThreadContext,
        target_thread_id: ThreadId,
        parent_checkpoint_id: Option<ThreadCheckpointId>,
        label: Option<String>,
        metadata: BTreeMap<String, String>,
    ) -> CooldisResult<AgentProcessCheckpointReceipt> {
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
        caller: &ThreadContext,
        target_thread_id: ThreadId,
    ) -> CooldisResult<AgentProcessStatusReceipt> {
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
        caller: &ThreadContext,
        target_thread_id: ThreadId,
    ) -> CooldisResult<AgentProcessChildrenReceipt> {
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
        caller: &ThreadContext,
        target_thread_id: ThreadId,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
    ) -> CooldisResult<(EventRecord, EventRecord)> {
        let target = self.scoped_thread(caller, target_thread_id).await?;
        target
            .record_manifest_receipts(compile_payload, bind_payload)
            .await
    }

    pub async fn start_mandate(
        &self,
        caller: &ThreadContext,
        target_thread_id: ThreadId,
        mut request: MandateStartRequest,
    ) -> CooldisResult<MandateStartReceipt> {
        let host = self.host()?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        if request.snapshot_id.is_none() {
            request.snapshot_id = target
                .context()
                .metadata
                .get("cooldis.agent.manifest_hash")
                .cloned();
        }
        start_mandate(
            host.runtime_store().as_ref(),
            &target.context().coordinates,
            request,
            chrono::Utc::now(),
        )
        .await
    }

    pub async fn revoke_mandate(
        &self,
        caller: &ThreadContext,
        target_thread_id: ThreadId,
        mandate_event_id: EventRecordId,
    ) -> CooldisResult<MandateRevokeReceipt> {
        let host = self.host()?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        revoke_mandate(
            host.runtime_store().as_ref(),
            &target.context().coordinates,
            mandate_event_id,
        )
        .await
    }

    pub async fn list_mandates(
        &self,
        caller: &ThreadContext,
        target_thread_id: ThreadId,
    ) -> CooldisResult<Vec<ActiveMandate>> {
        let host = self.host()?;
        let target = self.scoped_thread(caller, target_thread_id).await?;
        list_active_mandates(host.runtime_store().as_ref(), &target.context().coordinates).await
    }

    async fn scoped_thread(
        &self,
        caller: &ThreadContext,
        target_thread_id: ThreadId,
    ) -> CooldisResult<RuntimeThreadHandle> {
        let host = self.host()?;
        let target = host.get_thread(target_thread_id).await?;
        let requested = caller.coordinates.scope();
        let actual = target.context().coordinates.scope();
        if requested != actual {
            return Err(CooldisError::ThreadScopeMismatch {
                thread_id: target_thread_id,
                requested: Box::new(requested),
                actual: Box::new(actual),
            });
        }
        Ok(target)
    }
}
