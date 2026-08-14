use verlet_history::{EventStore as _, SessionStore as _};
use verlet_metadata::provider_store::ThreadMetadataStore as _;

const THREAD_REMOTE_PLACEMENT_PROJECTION_METADATA: &str = "cooldis.remote_placement_projection";

#[derive(Default)]
pub(crate) struct AppServerState {
    pub(crate) threads: std::collections::HashMap<String, AppServerThreadState>,
}

#[derive(Clone)]
pub(crate) struct AppServerThreadState {
    pub(crate) thread_id: String,
    pub(crate) session_id: String,
    pub(crate) parent_thread_id: Option<String>,
    pub(crate) topology: verlet_runtime_contracts::ThreadTopology,
    pub(crate) cwd: std::path::PathBuf,
    pub(crate) model_provider: String,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
    pub(crate) status: verlet_runtime_contracts::ThreadStatus,
    pub(crate) preview: String,
    pub(crate) ephemeral: bool,
    pub(crate) name: Option<String>,
    pub(crate) thinking: Option<verlet_provider::ThinkingConfig>,
    pub(crate) turns: std::collections::BTreeMap<String, AppServerTurnState>,
    pub(crate) active_turn_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct AppServerTurnState {
    pub(crate) id: String,
    pub(crate) items: Vec<serde_json::Value>,
    pub(crate) status: AppServerTurnStatus,
    pub(crate) started_at_ms: u64,
    pub(crate) completed_at_ms: Option<u64>,
    pub(crate) error: Option<serde_json::Value>,
    pub(crate) assistant_item_id: String,
    pub(crate) assistant_text: String,
    pub(crate) assistant_started: bool,
    pub(crate) assistant_completed: bool,
    pub(crate) thinking_item_id: String,
    pub(crate) thinking_text: String,
    pub(crate) thinking_started: bool,
    pub(crate) thinking_completed: bool,
    pub(crate) observed_running: bool,
    pub(crate) completion_scheduled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::AsRefStr, strum::Display)]
#[strum(serialize_all = "camelCase")]
pub(crate) enum AppServerTurnStatus {
    InProgress,
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone)]
pub(crate) struct AppServerThreadLifecycleSink {
    inner: std::sync::Weak<crate::adapters::app_server::VerletAppServerInner>,
}

impl AppServerThreadLifecycleSink {
    pub(crate) fn new(app: &crate::adapters::app_server::VerletAppServer) -> Self {
        Self {
            inner: std::sync::Arc::downgrade(&app.inner),
        }
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::ThreadLifecycleSink
    for AppServerThreadLifecycleSink
{
    async fn thread_started(
        &self,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if handle.context().parent_thread_id.is_none() {
            return Ok(());
        }
        let Some(inner) = self.inner.upgrade() else {
            return Ok(());
        };
        crate::adapters::app_server::VerletAppServer { inner }
            .register_runtime_thread(handle)
            .await
    }
}

pub(crate) async fn validate_loaded_manifest_thread_history(
    supervisor: &crate::kernel::supervisor::VerletSupervisor,
    record: &mut verlet_runtime_contracts::ThreadLifecycleRecord,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let runtime_store = supervisor
        .runtime_store(&record.coordinates.tenant_id)
        .await?;
    let witnessed_receipt = resume_manifest_bind_receipt(runtime_store.as_ref(), record).await?;
    let Some(raw) = record
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA)
    else {
        return Ok(());
    };
    let stored = match serde_json::from_str::<
        crate::agent::manifest_bind::AgentManifestResolvedWorkspaceMount,
    >(raw)
    {
        Ok(stored) => stored,
        Err(err) => {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread {} has invalid persisted workspace metadata: {err}; start a new thread",
                record.coordinates.thread_id
            )));
        }
    };
    let Some(witnessed) = witnessed_receipt.and_then(|receipt| receipt.workspace) else {
        return Err(crate::kernel::runtime_host::VerletError::History(format!(
            "thread {} has persisted workspace metadata but no durable workspace binding witness; start a new thread",
            record.coordinates.thread_id
        )));
    };
    if witnessed != stored {
        return Err(crate::kernel::runtime_host::VerletError::History(format!(
            "thread {} persisted workspace metadata disagrees with its durable binding witness; start a new thread",
            record.coordinates.thread_id
        )));
    }
    Ok(())
}

fn resume_manifest_receipt_matches_metadata(
    event: &verlet_history::EventRecord,
    receipt: &crate::agent::manifest_bind::AgentManifestBindReceipt,
    events: &[verlet_history::EventRecord],
    metadata: &std::collections::BTreeMap<String, String>,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    for (key, recorded) in [
        (
            crate::adapters::app_server::THREAD_AGENT_REF_METADATA,
            receipt.ref_uri.as_str(),
        ),
        (
            crate::adapters::app_server::THREAD_AGENT_MODEL_PROFILE_ID_METADATA,
            receipt.model_profile_id.as_str(),
        ),
        (
            crate::adapters::app_server::THREAD_AGENT_PROVIDER_ID_METADATA,
            receipt.provider_id.as_str(),
        ),
        (
            crate::adapters::app_server::THREAD_AGENT_MODEL_ID_METADATA,
            receipt.model_id.as_str(),
        ),
    ] {
        if metadata.get(key).is_some_and(|stored| stored != recorded) {
            return Ok(false);
        }
    }

    let Some(stored_source_hash) =
        metadata.get(crate::adapters::app_server::THREAD_AGENT_SOURCE_HASH_METADATA)
    else {
        return Ok(true);
    };
    let Some(compile_event) = event
        .provenance
        .source_event_ids
        .iter()
        .find_map(|event_id| events.iter().find(|candidate| candidate.id == *event_id))
        .filter(|event| event.kind == verlet_history::EventKind::ManifestCompileCompleted)
    else {
        return Ok(false);
    };
    let compile_receipt =
        crate::agent::manifest_bind::decode_manifest_compile_receipt_event(compile_event)?;
    Ok(&compile_receipt.source_hash == stored_source_hash)
}

async fn resume_manifest_bind_receipt<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    record: &verlet_runtime_contracts::ThreadLifecycleRecord,
) -> crate::kernel::runtime_host::VerletResult<
    Option<crate::agent::manifest_bind::AgentManifestBindReceipt>,
> {
    let events = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&record.coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    validate_manifest_binding_event_contract(&events)?;
    let mut bind_events = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
        .collect::<Vec<_>>();
    bind_events.sort_by_key(|event| std::cmp::Reverse(event.sequence.get()));
    for event in &bind_events {
        let receipt = crate::agent::manifest_bind::decode_manifest_bind_receipt_event(event)?;
        if resume_manifest_receipt_matches_metadata(event, &receipt, &events, &record.metadata)? {
            return Ok(Some(receipt));
        }
    }
    if bind_events.is_empty() {
        Ok(None)
    } else {
        Err(crate::kernel::runtime_host::VerletError::History(format!(
            "no durable manifest bind receipt matches the persisted identity for thread {}; start a new thread",
            record.coordinates.thread_id
        )))
    }
}

fn set_resume_metadata_json<T: serde::Serialize>(
    metadata: &mut std::collections::BTreeMap<String, String>,
    key: &str,
    value: &T,
    label: &str,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let encoded = serde_json::to_string(value).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode recorded {label} for resume: {err}"
        ))
    })?;
    metadata.insert(key.to_string(), encoded);
    Ok(())
}

fn recorded_runtime_overrides(
    receipt: &crate::agent::manifest_bind::AgentManifestBindReceipt,
) -> crate::agent::manifest_bind::AgentManifestBindOverrides {
    let mut overrides = crate::agent::manifest_bind::AgentManifestBindOverrides::default();
    for key in &receipt.overridden_keys {
        match key.as_str() {
            "default_cwd" => {
                overrides.default_cwd = Some(receipt.effective_runtime.default_cwd.clone())
            }
            "streaming" => overrides.streaming = Some(receipt.effective_runtime.streaming),
            "turn_timeout_ms" => {
                overrides.turn_timeout_ms = receipt.effective_runtime.turn_timeout_ms
            }
            "cancellation_grace_ms" => {
                overrides.cancellation_grace_ms = receipt.effective_runtime.cancellation_grace_ms
            }
            "max_tool_rounds" => {
                overrides.max_tool_rounds = receipt.effective_runtime.max_tool_rounds
            }
            "compaction.auto_at_text_bytes" => {
                overrides.compaction_auto_at_text_bytes =
                    receipt.effective_runtime.compaction.auto_at_text_bytes
            }
            _ => {}
        }
    }
    overrides
}

fn validate_resume_authority_metadata(
    record: &verlet_runtime_contracts::ThreadLifecycleRecord,
    receipt: &crate::agent::manifest_bind::AgentManifestBindReceipt,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let stored_tool_universes = record
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_TOOL_UNIVERSES_METADATA)
        .map(|raw| {
            serde_json::from_str::<Vec<crate::agent::tool_universe::ToolUniverseBinding>>(raw)
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "stored manifest tool universes are invalid: {err}"
                    ))
                })
        })
        .transpose()?
        .unwrap_or_default();
    for binding in &stored_tool_universes {
        binding.validate()?;
    }
    let stored_tool_universe_receipts = stored_tool_universes
        .iter()
        .map(crate::agent::tool_universe::ToolUniverseBindReceipt::from_binding)
        .collect::<Vec<_>>();
    if stored_tool_universe_receipts != receipt.tool_universes {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "stored tool universe metadata disagrees with the durable manifest bind witness for thread {}",
                record.coordinates.thread_id
            ),
        ));
    }

    let stored_couplings = record
        .metadata
        .get(crate::kernel::runtime_host::THREAD_BOUND_COUPLING_SET_METADATA)
        .map(|raw| {
            serde_json::from_str::<crate::agent::manifest_bind::BoundCouplingSet>(raw).map_err(
                |err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "stored manifest coupling set is invalid: {err}"
                    ))
                },
            )
        })
        .transpose()?
        .map(|set| {
            set.couplings
                .iter()
                .map(crate::agent::manifest_bind::AgentManifestCouplingBinding::from_bound)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if stored_couplings != receipt.couplings {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "stored coupling metadata disagrees with the durable manifest bind witness for thread {}",
                record.coordinates.thread_id
            ),
        ));
    }
    Ok(())
}

impl crate::adapters::app_server::VerletAppServer {
    pub(crate) async fn rehydrate_loaded_manifest_thread_metadata(
        &self,
        record: &mut verlet_runtime_contracts::ThreadLifecycleRecord,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let runtime_store = self
            .inner
            .supervisor
            .runtime_store(&record.coordinates.tenant_id)
            .await?;
        let Some(receipt) = resume_manifest_bind_receipt(runtime_store.as_ref(), record).await?
        else {
            if record
                .metadata
                .contains_key(crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA)
            {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
                    "thread {} has persisted workspace metadata but no durable manifest bind receipt; start a new thread",
                    record.coordinates.thread_id
                )));
            }
            return Ok(());
        };
        validate_resume_authority_metadata(record, &receipt)?;

        let metadata = &mut record.metadata;
        metadata
            .entry(crate::adapters::app_server::THREAD_AGENT_REF_METADATA.to_string())
            .or_insert_with(|| receipt.ref_uri.clone());
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_MANIFEST_HASH_METADATA.to_string(),
            receipt.manifest_hash.clone(),
        );
        metadata
            .entry(crate::adapters::app_server::THREAD_AGENT_MODEL_PROFILE_ID_METADATA.to_string())
            .or_insert_with(|| receipt.model_profile_id.clone());
        metadata
            .entry(crate::adapters::app_server::THREAD_AGENT_PROVIDER_ID_METADATA.to_string())
            .or_insert_with(|| receipt.provider_id.clone());
        metadata
            .entry(crate::adapters::app_server::THREAD_AGENT_MODEL_ID_METADATA.to_string())
            .or_insert_with(|| receipt.model_id.clone());
        metadata.insert(
            crate::adapters::app_server::THREAD_APP_SERVER_MODEL_PROVIDER_METADATA.to_string(),
            receipt.provider_id.clone(),
        );
        metadata.insert(
            crate::adapters::app_server::THREAD_APP_SERVER_CWD_METADATA.to_string(),
            crate::adapters::app_server::connection::cwd_string(&resolve_cwd(
                &self.inner.cwd,
                Some(receipt.effective_runtime.default_cwd.as_str()),
            )),
        );
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_RUNTIME_STREAMING_METADATA.to_string(),
            receipt.effective_runtime.streaming.to_string(),
        );
        if let Some(max_tool_rounds) = receipt.effective_runtime.max_tool_rounds {
            let value = match max_tool_rounds {
                verlet_agent::manifest_schema::AgentManifestMaxToolRounds::Limited(rounds) => {
                    rounds.to_string()
                }
                verlet_agent::manifest_schema::AgentManifestMaxToolRounds::Unlimited => {
                    "unlimited".to_string()
                }
            };
            metadata.insert(
                crate::adapters::app_server::THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA
                    .to_string(),
                value,
            );
        } else {
            metadata
                .remove(crate::adapters::app_server::THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA);
        }
        if let Some(auto_at_text_bytes) = receipt.effective_runtime.compaction.auto_at_text_bytes {
            metadata.insert(
                crate::adapters::app_server::THREAD_AGENT_RUNTIME_COMPACTION_AUTO_AT_TEXT_BYTES_METADATA
                    .to_string(),
                auto_at_text_bytes.to_string(),
            );
        } else {
            metadata.remove(
                crate::adapters::app_server::THREAD_AGENT_RUNTIME_COMPACTION_AUTO_AT_TEXT_BYTES_METADATA,
            );
        }
        if !receipt.tool_ids.is_empty() {
            let mut tool_ids = receipt.tool_ids.clone();
            tool_ids.sort();
            metadata.insert(
                crate::adapters::app_server::THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA.to_string(),
                manifest_tool_use_instruction_text(&receipt.ref_uri, Some(&tool_ids.join(", "))),
            );
        } else {
            metadata.remove(crate::adapters::app_server::THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA);
        }
        set_resume_metadata_json(
            metadata,
            crate::adapters::app_server::THREAD_AGENT_OPERATION_BINDINGS_METADATA,
            &receipt.operation_bindings,
            "operation bindings",
        )?;
        let overrides = recorded_runtime_overrides(&receipt);
        if !overrides.is_empty() {
            set_resume_metadata_json(
                metadata,
                crate::adapters::app_server::THREAD_AGENT_RUNTIME_OVERRIDES_METADATA,
                &overrides,
                "runtime overrides",
            )?;
        } else {
            metadata.remove(crate::adapters::app_server::THREAD_AGENT_RUNTIME_OVERRIDES_METADATA);
        }
        let recorded_placement = receipt.placement.clone().unwrap_or_default();
        set_resume_metadata_json(
            metadata,
            crate::adapters::app_server::THREAD_AGENT_PLACEMENT_METADATA,
            &recorded_placement,
            "placement binding",
        )?;
        if let Some(workspace) = &receipt.workspace {
            set_resume_metadata_json(
                metadata,
                crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA,
                workspace,
                "workspace binding",
            )?;
        } else {
            metadata.remove(crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA);
        }
        if !receipt.skill_packages.is_empty() {
            set_resume_metadata_json(
                metadata,
                crate::agent::manifest_bind::THREAD_AGENT_SKILL_PACKAGES_METADATA,
                &receipt.skill_packages,
                "skill package bindings",
            )?;
        } else {
            metadata.remove(crate::agent::manifest_bind::THREAD_AGENT_SKILL_PACKAGES_METADATA);
        }
        if let Some(discovery) = &receipt.skill_discovery {
            set_resume_metadata_json(
                metadata,
                crate::agent::manifest_bind::THREAD_AGENT_SKILL_DISCOVERY_METADATA,
                discovery,
                "skill discovery witness",
            )?;
        } else {
            metadata.remove(crate::agent::manifest_bind::THREAD_AGENT_SKILL_DISCOVERY_METADATA);
        }
        if !receipt.static_context_segments.is_empty() {
            set_resume_metadata_json(
                metadata,
                crate::agent::manifest_bind::THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA,
                &receipt.static_context_segments,
                "static context segments",
            )?;
        } else {
            metadata
                .remove(crate::agent::manifest_bind::THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA);
        }
        if !receipt.skill_packages.is_empty() || receipt.skill_discovery.is_some() {
            let skill_context_segments =
                crate::agent::manifest_bind::skill_context_segments_for_witnesses(
                    &receipt.skill_packages,
                    Some(self.inner.skill_registry_root.as_path()),
                    receipt.skill_discovery.as_ref(),
                )?;
            set_resume_metadata_json(
                metadata,
                crate::agent::manifest_bind::THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA,
                &skill_context_segments,
                "skill context segments",
            )?;
        } else {
            metadata
                .remove(crate::agent::manifest_bind::THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA);
        }
        Ok(())
    }

    pub(crate) async fn load_threads_from_metadata(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let records = self
            .inner
            .metadata_store
            .list_thread_lifecycle_for_user(&self.inner.tenant_id, &self.inner.user_id)
            .await
            .map_err(crate::adapters::app_server::metadata_store_error)?;
        for mut record in records {
            if !is_loadable_lifecycle_status(record.status)
                || record
                    .metadata
                    .get(THREAD_REMOTE_PLACEMENT_PROJECTION_METADATA)
                    .is_some_and(|value| value == "true")
            {
                continue;
            }
            if let Err(err) = self
                .rehydrate_loaded_manifest_thread_metadata(&mut record)
                .await
            {
                eprintln!(
                    "verlet app-server skipped thread {} with invalid durable bind history: {err}",
                    record.coordinates.thread_id
                );
                continue;
            }
            let thread_id = record.coordinates.thread_id.to_string();
            if self
                .inner
                .state
                .read()
                .await
                .threads
                .contains_key(&thread_id)
            {
                continue;
            }
            let handle = self
                .inner
                .supervisor
                .load_thread_from_lifecycle(record.clone())
                .await?;
            crate::adapters::app_server::subscriptions::wait_for_initial_thread_status(&handle)
                .await;
            let thread_state = self
                .thread_state_from_lifecycle(&record, handle.status())
                .await?;
            let mut state = self.inner.state.write().await;
            state.threads.insert(thread_id, thread_state);
        }
        Ok(())
    }

    pub(crate) async fn load_thread_from_metadata(
        &self,
        thread_id: &str,
        parsed: verlet_runtime_contracts::ThreadId,
    ) -> Result<
        crate::kernel::runtime_host::RuntimeThreadHandle,
        crate::adapters::app_server::connection::JsonRpcErrorError,
    > {
        let mut record = self
            .inner
            .metadata_store
            .get_thread_lifecycle(parsed)
            .await
            .map_err(crate::adapters::app_server::metadata_store_jsonrpc_error)?
            .ok_or_else(|| crate::adapters::app_server::connection::thread_not_found(thread_id))?;
        if record.coordinates.tenant_id != self.inner.tenant_id
            || record.coordinates.user_id != self.inner.user_id
            || !is_loadable_lifecycle_status(record.status)
            || record
                .metadata
                .get(THREAD_REMOTE_PLACEMENT_PROJECTION_METADATA)
                .is_some_and(|value| value == "true")
        {
            return Err(crate::adapters::app_server::connection::thread_not_found(
                thread_id,
            ));
        }
        self.rehydrate_loaded_manifest_thread_metadata(&mut record)
            .await
            .map_err(crate::adapters::app_server::connection::internal_error)?;
        let handle = loop {
            match self
                .inner
                .supervisor
                .load_thread_from_lifecycle(record.clone())
                .await
            {
                Ok(handle) => break handle,
                Err(crate::kernel::runtime_host::VerletError::ThreadAlreadyExists(_)) => {
                    self.inner
                        .supervisor
                        .wait_for_thread_start_reservation(
                            &record.coordinates.tenant_id,
                            record.coordinates.thread_id,
                        )
                        .await
                        .map_err(crate::adapters::app_server::connection::internal_error)?;
                    match self
                        .inner
                        .supervisor
                        .get_thread_at(&record.coordinates)
                        .await
                    {
                        Ok(handle) => break handle,
                        Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(_)) => {
                            continue;
                        }
                        Err(err) => {
                            return Err(crate::adapters::app_server::connection::internal_error(
                                err,
                            ));
                        }
                    }
                }
                Err(err) => {
                    return Err(crate::adapters::app_server::connection::internal_error(err));
                }
            }
        };
        crate::adapters::app_server::subscriptions::wait_for_initial_thread_status(&handle).await;
        let thread_state = self
            .thread_state_from_lifecycle(&record, handle.status())
            .await
            .map_err(crate::adapters::app_server::connection::internal_error)?;
        let mut state = self.inner.state.write().await;
        state
            .threads
            .insert(record.coordinates.thread_id.to_string(), thread_state);
        Ok(handle)
    }

    pub(crate) fn agent_registry_root(&self) -> &std::path::Path {
        &self.inner.agent_registry_root
    }

    pub(crate) async fn validate_daemon_route_agent_ref(
        &self,
        agent_ref: &str,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if !agent_ref.starts_with("agent://") {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "daemon route agent_ref must be an agent:// ref".to_string(),
            ));
        }
        crate::agent::manifest::AgentRecordRef::parse(agent_ref)?;
        self.bind_daemon_route_agent(agent_ref).await?;
        Ok(())
    }

    pub(crate) async fn bind_daemon_route_agent(
        &self,
        agent_ref: &str,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::agent::agent_process::KernelThreadSpawnAgentBinding,
    > {
        let bound = self
            .bind_app_server_agent_ref(
                agent_ref,
                &crate::agent::manifest_bind::AgentManifestModelProfileSelection::default(),
                &crate::agent::manifest_bind::AgentManifestBindOverrides::default(),
                None,
                None,
            )
            .await?;
        require_local_binding_surface("daemon route", &bound)?;
        kernel_thread_spawn_agent_binding(
            &bound,
            &self.inner.cwd,
            self.inner.capsule_bindings.registry_root.as_deref(),
            None,
            self.inner.user_id.clone(),
        )
    }

    pub(crate) async fn bind_app_server_agent_ref(
        &self,
        agent_ref: &str,
        model_selection: &crate::agent::manifest_bind::AgentManifestModelProfileSelection,
        overrides: &crate::agent::manifest_bind::AgentManifestBindOverrides,
        placement_override: Option<&crate::agent::manifest_bind::AgentManifestPlacementBinding>,
        workspace_override: Option<&crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::agent::manifest_bind::AgentManifestBoundThread,
    > {
        let registry = self.inner.agent_registry();
        let (record, alias) = registry.load_ref_with_alias_receipt(agent_ref)?;
        let provider_surface = self.agent_manifest_provider_surface().await?;
        let mcp_server_refs = self.configured_mcp_server_refs().await?;
        let tool_universe_discoverer = self.tool_universe_discoverer().await?;
        crate::agent::manifest_bind::bind_published_agent_record_with_placement(
            &record,
            alias,
            &provider_surface,
            self.inner.capsule_bindings.registry_root.as_deref(),
            Some(self.inner.blob_registry_root.as_path()),
            Some(self.inner.skill_registry_root.as_path()),
            &mcp_server_refs,
            Some(&tool_universe_discoverer),
            model_selection,
            overrides,
            Some(&self.inner.default_placement),
            placement_override,
            self.inner.default_workspace.as_ref(),
            workspace_override,
            self.remote_event_store_served(),
        )
        .await
    }

    pub(crate) async fn thread_state_from_lifecycle(
        &self,
        record: &verlet_runtime_contracts::ThreadLifecycleRecord,
        status: verlet_runtime_contracts::ThreadStatus,
    ) -> crate::kernel::runtime_host::VerletResult<AppServerThreadState> {
        let (preview, turns) = self.thread_history_from_lifecycle(record).await?;
        Ok(AppServerThreadState {
            thread_id: record.coordinates.thread_id.to_string(),
            session_id: record.coordinates.session_id.clone(),
            parent_thread_id: record.parent_thread_id.map(|id| id.to_string()),
            topology: record.topology.clone(),
            cwd: thread_lifecycle_cwd(record).unwrap_or_else(|| self.inner.cwd.clone()),
            model_provider: record
                .metadata
                .get(crate::adapters::app_server::THREAD_APP_SERVER_MODEL_PROVIDER_METADATA)
                .cloned()
                .unwrap_or_else(|| self.inner.model_provider.clone()),
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
            status,
            preview,
            ephemeral: record
                .metadata
                .get(crate::adapters::app_server::THREAD_APP_SERVER_EPHEMERAL_METADATA)
                .is_some_and(|value| value == "true"),
            name: record
                .metadata
                .get(crate::adapters::app_server::THREAD_APP_SERVER_NAME_METADATA)
                .filter(|value| !value.trim().is_empty())
                .cloned(),
            thinking: thread_lifecycle_thinking(record)?,
            turns,
            active_turn_id: None,
        })
    }

    /// Rebuild the app-server thread projection from durable session history.
    pub(crate) async fn thread_history_from_lifecycle(
        &self,
        record: &verlet_runtime_contracts::ThreadLifecycleRecord,
    ) -> crate::kernel::runtime_host::VerletResult<(
        String,
        std::collections::BTreeMap<String, AppServerTurnState>,
    )> {
        let store = verlet_history_sqlite::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
            .with_lease_epoch(self.inner.lease_epoch);
        let context = store
            .build_context(&record.coordinates)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        Ok(app_server_turns_from_session_entries(&context.entries))
    }

    pub(crate) async fn persist_thread_lifecycle(
        &self,
        handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
        self.persist_thread_lifecycle_with_metadata(handle, std::collections::BTreeMap::new())
            .await
    }

    pub(crate) async fn persist_thread_lifecycle_with_metadata(
        &self,
        handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
        self.persist_thread_lifecycle_record_with_metadata(handle, metadata)
            .await
            .map(|_| ())
            .map_err(crate::adapters::app_server::connection::internal_error)
    }

    pub(crate) async fn persist_thread_lifecycle_record_with_metadata(
        &self,
        handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_runtime_contracts::ThreadLifecycleRecord>
    {
        let mut record = handle.lifecycle_record().await;
        let handle_metadata = record.metadata.clone();
        if let Some(existing) = self
            .inner
            .metadata_store
            .get_thread_lifecycle(record.coordinates.thread_id)
            .await
            .map_err(crate::adapters::app_server::metadata_store_error)?
        {
            record.metadata = existing.metadata;
            record.metadata.extend(handle_metadata);
        }
        record.metadata.extend(metadata);
        self.inner
            .metadata_store
            .upsert_thread_lifecycle(record.clone())
            .await
            .map_err(crate::adapters::app_server::metadata_store_error)?;
        Ok(record)
    }

    pub(crate) async fn register_runtime_thread(
        &self,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        crate::adapters::app_server::subscriptions::wait_for_initial_thread_status(&handle).await;
        let record = self
            .persist_thread_lifecycle_record_with_metadata(
                &handle,
                std::collections::BTreeMap::new(),
            )
            .await?;
        let thread_id = record.coordinates.thread_id.to_string();
        let thread_state = self
            .thread_state_from_lifecycle(&record, handle.status())
            .await?;
        {
            let mut state = self.inner.state.write().await;
            state
                .threads
                .entry(thread_id.clone())
                .or_insert(thread_state);
        }
        self.spawn_lifecycle_persistence_watcher(thread_id, handle);
        Ok(())
    }

    pub(crate) fn spawn_lifecycle_persistence_watcher(
        &self,
        thread_id: String,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
    ) {
        let app = self.clone();
        let tasks = std::sync::Arc::clone(&self.inner.tasks);
        tasks.spawn_cancellable(async move {
            let mut status = handle.subscribe_status();
            let _ = status.borrow_and_update();
            loop {
                if status.changed().await.is_err() {
                    break;
                }
                let status_value = *status.borrow_and_update();
                if let Err(err) = app
                    .persist_thread_lifecycle_record_with_metadata(
                        &handle,
                        std::collections::BTreeMap::new(),
                    )
                    .await
                {
                    eprintln!(
                        "failed to persist app-server lifecycle status for thread {thread_id}: {err}"
                    );
                    break;
                }
                {
                    let mut state = app.inner.state.write().await;
                    if let Some(thread) = state.threads.get_mut(&thread_id) {
                        thread.status = status_value;
                        thread.updated_at_ms =
                            crate::adapters::app_server::connection::now_ms();
                    }
                }
                if matches!(
                    status_value,
                    verlet_runtime_contracts::ThreadStatus::Stopped
                        | verlet_runtime_contracts::ThreadStatus::Failed
                ) {
                    break;
                }
            }
        });
    }

    pub(crate) async fn thread_json_by_id(
        &self,
        thread_id: &str,
        include_turns: bool,
    ) -> Result<serde_json::Value, crate::adapters::app_server::connection::JsonRpcErrorError> {
        let state = self.inner.state.read().await;
        let thread = state
            .threads
            .get(thread_id)
            .ok_or_else(|| crate::adapters::app_server::connection::thread_not_found(thread_id))?;
        Ok(thread_json(thread, include_turns))
    }

    pub(crate) async fn register_remote_thread_projection(
        &self,
        parent: &crate::kernel::runtime_host::RuntimeThreadHandle,
        receipt: &crate::kernel::runtime_host::kernel_control::AgentProcessSpawnReceipt,
    ) -> Result<serde_json::Value, crate::adapters::app_server::connection::JsonRpcErrorError> {
        let parent_context = parent.context();
        let child_context = verlet_runtime_contracts::ThreadContext::with_topology_and_metadata(
            verlet_runtime_contracts::ThreadCoordinates {
                tenant_id: parent_context.coordinates.tenant_id.clone(),
                user_id: parent_context.coordinates.user_id.clone(),
                session_id: parent_context.coordinates.session_id.clone(),
                thread_id: receipt.thread_id,
            },
            verlet_runtime_contracts::ThreadTopology::spawned_from(
                parent_context.coordinates.thread_id,
            ),
            std::collections::BTreeMap::from([
                ("agent_process_v1".to_string(), "true".to_string()),
                (
                    THREAD_REMOTE_PLACEMENT_PROJECTION_METADATA.to_string(),
                    "true".to_string(),
                ),
            ]),
        );
        let lifecycle = verlet_runtime_contracts::ThreadLifecycleRecord::new(
            &child_context,
            receipt.status.into(),
            child_context.metadata.clone(),
        );
        self.inner
            .metadata_store
            .upsert_thread_lifecycle(lifecycle)
            .await
            .map_err(crate::adapters::app_server::metadata_store_jsonrpc_error)?;
        let now = crate::adapters::app_server::connection::now_ms();
        let state = AppServerThreadState {
            thread_id: receipt.thread_id.to_string(),
            session_id: child_context.coordinates.session_id,
            parent_thread_id: child_context.parent_thread_id.map(|id| id.to_string()),
            topology: child_context.topology,
            cwd: self.inner.cwd.clone(),
            model_provider: self.inner.model_provider.clone(),
            created_at_ms: now,
            updated_at_ms: now,
            status: receipt.status,
            preview: String::new(),
            ephemeral: false,
            name: receipt.task_name.clone(),
            thinking: None,
            turns: std::collections::BTreeMap::new(),
            active_turn_id: Some(receipt.submitted_turn_id.clone()),
        };
        let thread_id = receipt.thread_id.to_string();
        self.inner
            .state
            .write()
            .await
            .threads
            .insert(thread_id.clone(), state);
        self.thread_json_by_id(&thread_id, false).await
    }
}

pub(crate) fn require_local_binding_surface(
    surface: &str,
    bound: &crate::agent::manifest_bind::AgentManifestBoundThread,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let target = bound
        .bind_receipt
        .placement
        .as_ref()
        .map(|placement| placement.target.clone())
        .unwrap_or(crate::kernel::control_decision::PlacementTarget::Local);
    if target != crate::kernel::control_decision::PlacementTarget::Local {
        let target: &str = target.as_ref();
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{surface} does not execute placement target {target}; \
             remote placement is supported by thread/spawn"
            ),
        ));
    }
    Ok(())
}

impl AppServerTurnState {
    pub(crate) fn new(id: String, input: Vec<serde_json::Value>) -> Self {
        let assistant_item_id = format!("{id}:agent-message");
        let thinking_item_id = format!("{id}:agent-thinking");
        Self {
            id: id.clone(),
            items: vec![serde_json::json!({
                "type": "userMessage",
                "id": format!("{id}:user-message"),
                "content": input,
            })],
            status: AppServerTurnStatus::InProgress,
            started_at_ms: crate::adapters::app_server::connection::now_ms(),
            completed_at_ms: None,
            error: None,
            assistant_item_id,
            assistant_text: String::new(),
            assistant_started: false,
            assistant_completed: false,
            thinking_item_id,
            thinking_text: String::new(),
            thinking_started: false,
            thinking_completed: false,
            observed_running: false,
            completion_scheduled: false,
        }
    }

    pub(crate) fn restored(id: String, started_at_ms: u64, items: Vec<serde_json::Value>) -> Self {
        let assistant_item_id = format!("{id}:agent-message");
        let thinking_item_id = format!("{id}:agent-thinking");
        Self {
            id,
            items,
            status: AppServerTurnStatus::InProgress,
            started_at_ms,
            completed_at_ms: None,
            error: None,
            assistant_item_id,
            assistant_text: String::new(),
            assistant_started: false,
            assistant_completed: false,
            thinking_item_id,
            thinking_text: String::new(),
            thinking_started: false,
            thinking_completed: false,
            observed_running: false,
            completion_scheduled: false,
        }
    }
}

pub(crate) fn finalize_turn_payload(
    turn: &mut AppServerTurnState,
) -> (serde_json::Value, Vec<serde_json::Value>) {
    let should_complete_message = turn.assistant_started && !turn.assistant_completed;
    let should_complete_thinking = turn.thinking_started && !turn.thinking_completed;
    finalize_agent_message_item(turn);
    finalize_agent_thinking_item(turn);
    let mut items = Vec::new();
    if should_complete_message
        && let Some(item) = turn.items.iter().find(|item| {
            item.get("id").and_then(serde_json::Value::as_str)
                == Some(turn.assistant_item_id.as_str())
        })
    {
        items.push(item.clone());
    }
    if should_complete_thinking
        && let Some(item) = turn.items.iter().find(|item| {
            item.get("id").and_then(serde_json::Value::as_str)
                == Some(turn.thinking_item_id.as_str())
        })
    {
        items.push(item.clone());
    }
    (turn_json(turn), items)
}

pub(crate) fn finalize_agent_message_item(turn: &mut AppServerTurnState) {
    if !turn.assistant_started || turn.assistant_completed {
        return;
    }
    let item_id = turn.assistant_item_id.clone();
    let final_item = agent_message_item(turn);
    for item in &mut turn.items {
        if item.get("id").and_then(serde_json::Value::as_str) == Some(item_id.as_str()) {
            *item = final_item;
            break;
        }
    }
    turn.assistant_completed = true;
}

pub(crate) fn finalize_agent_thinking_item(turn: &mut AppServerTurnState) {
    if !turn.thinking_started || turn.thinking_completed {
        return;
    }
    let item_id = turn.thinking_item_id.clone();
    let final_item = agent_thinking_item(turn);
    for item in &mut turn.items {
        if item.get("id").and_then(serde_json::Value::as_str) == Some(item_id.as_str()) {
            *item = final_item;
            break;
        }
    }
    turn.thinking_completed = true;
}

pub(crate) fn agent_message_item(turn: &AppServerTurnState) -> serde_json::Value {
    agent_message_item_from_text(&turn.assistant_item_id, &turn.assistant_text)
}

pub(crate) fn agent_message_item_from_text(id: &str, text: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "agentMessage",
        "id": id,
        "text": text,
        "content": [{ "type": "text", "text": text }],
        "phase": null,
        "memoryCitation": null,
    })
}

pub(crate) fn agent_thinking_item(turn: &AppServerTurnState) -> serde_json::Value {
    agent_thinking_item_from_text(&turn.thinking_item_id, &turn.thinking_text)
}

pub(crate) fn agent_thinking_item_from_text(id: &str, text: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "agentThinking",
        "id": id,
        "text": text,
        "content": [{ "type": "text", "text": text }],
        "phase": null,
        "memoryCitation": null,
    })
}

pub(crate) fn command_execution_item(
    id: &str,
    command: &str,
    cwd: &std::path::Path,
    status: &str,
    aggregated_output: Option<String>,
    exit_code: Option<i32>,
    duration_ms: Option<u64>,
) -> serde_json::Value {
    serde_json::json!({
        "type": "commandExecution",
        "id": id,
        "command": command,
        "cwd": crate::adapters::app_server::connection::cwd_string(cwd),
        "processId": null,
        "source": "userShell",
        "status": status,
        "commandActions": [],
        "aggregatedOutput": aggregated_output,
        "exitCode": exit_code,
        "durationMs": duration_ms,
    })
}

pub(crate) fn thread_json(thread: &AppServerThreadState, include_turns: bool) -> serde_json::Value {
    let turns = if include_turns {
        thread.turns.values().map(turn_json).collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    serde_json::json!({
        "id": thread.thread_id,
        "sessionId": thread.session_id,
        "forkedFromId": thread.parent_thread_id,
        "parentThreadId": thread.parent_thread_id,
        "topology": thread.topology.clone(),
        "preview": thread.preview,
        "ephemeral": thread.ephemeral,
        "modelProvider": thread.model_provider,
        "thinking": app_server_thinking_json(&thread.thinking),
        "createdAt": thread.created_at_ms / 1000,
        "updatedAt": thread.updated_at_ms / 1000,
        "status": thread_status_json(thread.status),
        "path": null,
        "cwd": crate::adapters::app_server::connection::cwd_string(&thread.cwd),
        "cliVersion": env!("CARGO_PKG_VERSION"),
        "source": "appServer",
        "threadSource": null,
        "agentNickname": null,
        "agentRole": null,
        "gitInfo": null,
        "name": thread.name,
        "turns": turns,
    })
}

pub(crate) fn turn_json(turn: &AppServerTurnState) -> serde_json::Value {
    let completed_at = turn.completed_at_ms.map(|ms| ms / 1000);
    let duration_ms = turn
        .completed_at_ms
        .map(|completed| completed.saturating_sub(turn.started_at_ms));
    let status: &str = turn.status.as_ref();
    serde_json::json!({
        "id": turn.id,
        "items": turn.items,
        "itemsView": "full",
        "status": status,
        "error": turn.error,
        "startedAt": turn.started_at_ms / 1000,
        "completedAt": completed_at,
        "durationMs": duration_ms,
    })
}

pub(crate) fn thread_status_json(
    status: verlet_runtime_contracts::ThreadStatus,
) -> serde_json::Value {
    match status {
        verlet_runtime_contracts::ThreadStatus::Starting
        | verlet_runtime_contracts::ThreadStatus::Running
        | verlet_runtime_contracts::ThreadStatus::Cancelling => {
            serde_json::json!({ "type": "active", "activeFlags": [] })
        }
        verlet_runtime_contracts::ThreadStatus::Idle => serde_json::json!({ "type": "idle" }),
        verlet_runtime_contracts::ThreadStatus::Stopped => {
            serde_json::json!({ "type": "notLoaded" })
        }
        verlet_runtime_contracts::ThreadStatus::Failed => {
            serde_json::json!({ "type": "systemError" })
        }
    }
}

pub(crate) fn turn_input_from_values(
    input: &[serde_json::Value],
) -> crate::kernel::runtime_host::turn::TurnInput {
    let mut content = Vec::new();
    for item in input {
        match item.get("type").and_then(serde_json::Value::as_str) {
            Some("text") => {
                if let Some(text) = item.get("text").and_then(serde_json::Value::as_str) {
                    content.push(crate::kernel::runtime_host::turn::TurnContent::text(
                        text.to_string(),
                    ));
                }
            }
            Some("localImage") => {
                if let Some(path) = item.get("path").and_then(serde_json::Value::as_str) {
                    content.push(crate::kernel::runtime_host::turn::TurnContent::file_ref(
                        std::path::PathBuf::from(path),
                    ));
                }
            }
            Some("mention") | Some("skill") => {
                if let Some(name) = item.get("name").and_then(serde_json::Value::as_str) {
                    content.push(crate::kernel::runtime_host::turn::TurnContent::text(
                        format!("@{name}"),
                    ));
                }
            }
            Some("image") => {}
            _ => {}
        }
    }
    if content.is_empty() {
        content.push(crate::kernel::runtime_host::turn::TurnContent::text(""));
    }
    crate::kernel::runtime_host::turn::TurnInput::new(content)
}

pub(crate) fn deserialize_optional_thinking<'de, D>(
    deserializer: D,
) -> Result<Option<verlet_provider::ThinkingConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = <Option<serde_json::Value> as serde::Deserialize>::deserialize(deserializer)?;
    value
        .map(|value| thinking_from_app_server_value(&value).map_err(serde::de::Error::custom))
        .transpose()
}

pub(crate) fn thinking_from_app_server_value(
    value: &serde_json::Value,
) -> Result<verlet_provider::ThinkingConfig, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "thinking must be an object".to_string())?;
    let kind = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "thinking.type is required".to_string())?;
    match kind {
        "effort" => {
            let effort = object
                .get("effort")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "thinking.effort is required".to_string())?;
            let effort = match effort {
                "low" => verlet_provider::ThinkingEffort::Low,
                "medium" => verlet_provider::ThinkingEffort::Medium,
                "high" => verlet_provider::ThinkingEffort::High,
                "xhigh" => verlet_provider::ThinkingEffort::XHigh,
                "max" => verlet_provider::ThinkingEffort::Max,
                other => return Err(format!("unsupported thinking effort {other:?}")),
            };
            Ok(verlet_provider::ThinkingConfig::Effort { effort })
        }
        "budget" => {
            let budget_tokens = object
                .get("budgetTokens")
                .and_then(serde_json::Value::as_u64)
                .filter(|value| *value <= u64::from(u32::MAX))
                .ok_or_else(|| "thinking.budgetTokens must be a u32".to_string())?
                as u32;
            Ok(verlet_provider::ThinkingConfig::Budget { budget_tokens })
        }
        "disabled" => Ok(verlet_provider::ThinkingConfig::Disabled),
        other => Err(format!("unsupported thinking type {other:?}")),
    }
}

pub(crate) fn app_server_thinking_json(
    thinking: &Option<verlet_provider::ThinkingConfig>,
) -> serde_json::Value {
    thinking
        .as_ref()
        .map(app_server_thinking_value)
        .unwrap_or(serde_json::Value::Null)
}

pub(crate) fn app_server_thinking_value(
    thinking: &verlet_provider::ThinkingConfig,
) -> serde_json::Value {
    match thinking {
        verlet_provider::ThinkingConfig::Effort { effort } => {
            let effort: &str = effort.as_ref();
            serde_json::json!({
                "type": "effort",
                "effort": effort,
            })
        }
        verlet_provider::ThinkingConfig::Budget { budget_tokens } => serde_json::json!({
            "type": "budget",
            "budgetTokens": budget_tokens,
        }),
        verlet_provider::ThinkingConfig::Disabled => serde_json::json!({ "type": "disabled" }),
    }
}

pub(crate) fn encode_app_server_thinking(
    thinking: &verlet_provider::ThinkingConfig,
) -> Result<String, crate::adapters::app_server::connection::JsonRpcErrorError> {
    serde_json::to_string(&app_server_thinking_value(thinking)).map_err(|err| {
        crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            format!("failed to encode app-server thinking config: {err}"),
        )
    })
}

pub(crate) fn user_input_preview(input: &[serde_json::Value]) -> String {
    input
        .iter()
        .filter_map(|item| item.get("text").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn app_server_turns_from_session_entries(
    entries: &[verlet_history::SessionEntry],
) -> (
    String,
    std::collections::BTreeMap<String, AppServerTurnState>,
) {
    let mut preview = String::new();
    let mut turns = std::collections::BTreeMap::new();
    let mut current_turn_id = None;

    for entry in entries {
        let (verlet_history::SessionEntryKind::Message { message }
        | verlet_history::SessionEntryKind::CustomContextMessage { message }) = &entry.kind
        else {
            continue;
        };
        match message {
            verlet_history::CanonicalMessage::User { content, .. } => {
                let text_content = canonical_text_content_items(content);
                if preview.is_empty() {
                    preview = text_content_preview(&text_content);
                }
                let turn_id = format!("turn-{}", entry.entry_id);
                let item = serde_json::json!({
                    "type": "userMessage",
                    "id": format!("{turn_id}:user-message"),
                    "content": text_content,
                });
                let turn = AppServerTurnState::restored(
                    turn_id.clone(),
                    entry_created_at_ms(entry),
                    vec![item],
                );
                turns.insert(turn_id.clone(), turn);
                current_turn_id = Some(turn_id);
            }
            verlet_history::CanonicalMessage::Assistant { content, .. } => {
                let Some(turn_id) = current_turn_id.clone() else {
                    continue;
                };
                let Some(turn) = turns.get_mut(&turn_id) else {
                    continue;
                };
                let restored = restored_assistant_items_from_canonical_content(
                    content,
                    &turn.assistant_item_id,
                    &turn.thinking_item_id,
                );
                if restored.text.is_empty() && restored.thinking.is_empty() {
                    continue;
                }
                if !restored.text.is_empty() {
                    turn.assistant_started = true;
                    turn.assistant_completed = true;
                    turn.assistant_text = restored.text;
                }
                if !restored.thinking.is_empty() {
                    turn.thinking_started = true;
                    turn.thinking_completed = true;
                    turn.thinking_text = restored.thinking;
                }
                turn.items.extend(restored.items);
                turn.status = AppServerTurnStatus::Completed;
                turn.completed_at_ms = Some(entry_created_at_ms(entry));
            }
            verlet_history::CanonicalMessage::ToolResult { .. } => {}
        }
    }

    (preview, turns)
}

struct RestoredAssistantItems {
    text: String,
    thinking: String,
    items: Vec<serde_json::Value>,
}

fn restored_assistant_items_from_canonical_content(
    content: &[verlet_history::CanonicalContent],
    assistant_item_id: &str,
    thinking_item_id: &str,
) -> RestoredAssistantItems {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum ItemKind {
        Message,
        Thinking,
    }

    let mut text = String::new();
    let mut thinking = String::new();
    let mut order = Vec::new();
    let mut text_seen = false;
    let mut thinking_seen = false;

    for content in content {
        match content {
            verlet_history::CanonicalContent::Text { text: value, .. } => {
                if !value.is_empty() && !text_seen {
                    order.push(ItemKind::Message);
                    text_seen = true;
                }
                text.push_str(value);
            }
            verlet_history::CanonicalContent::Thinking { text: value, .. } => {
                if !value.is_empty() && !thinking_seen {
                    order.push(ItemKind::Thinking);
                    thinking_seen = true;
                }
                thinking.push_str(value);
            }
            verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => {}
        }
    }

    let mut items = Vec::with_capacity(order.len());
    for item in order {
        match item {
            ItemKind::Message if !text.is_empty() => {
                items.push(agent_message_item_from_text(assistant_item_id, &text));
            }
            ItemKind::Thinking if !thinking.is_empty() => {
                items.push(agent_thinking_item_from_text(thinking_item_id, &thinking));
            }
            ItemKind::Message | ItemKind::Thinking => {}
        }
    }

    RestoredAssistantItems {
        text,
        thinking,
        items,
    }
}

pub(crate) fn canonical_text_content_items(
    content: &[verlet_history::CanonicalContent],
) -> Vec<serde_json::Value> {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => {
                Some(serde_json::json!({ "type": "text", "text": text }))
            }
            verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => None,
            verlet_history::CanonicalContent::Thinking { .. } => None,
        })
        .collect()
}

pub(crate) fn text_content_preview(content: &[serde_json::Value]) -> String {
    content
        .iter()
        .filter_map(|item| item.get("text").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn entry_created_at_ms(entry: &verlet_history::SessionEntry) -> u64 {
    entry.created_at_ms.max(0) as u64
}

pub(crate) fn resolve_cwd(default_cwd: &std::path::Path, cwd: Option<&str>) -> std::path::PathBuf {
    match cwd {
        Some(cwd) if !cwd.trim().is_empty() => {
            let path = std::path::PathBuf::from(cwd);
            if path.is_absolute() {
                path
            } else {
                default_cwd.join(path)
            }
        }
        _ => default_cwd.to_path_buf(),
    }
}

pub(crate) fn normalize_registry_roots(
    config: &mut crate::adapters::app_server::VerletAppServerConfig,
) {
    let blob_registry_root_was_default = config.blob_registry_root
        == std::path::Path::new(crate::adapters::app_server::DEFAULT_BLOB_REGISTRY_ROOT);
    config.agent_registry_root = resolve_path_against_cwd(&config.cwd, &config.agent_registry_root);
    config.blob_registry_root = if blob_registry_root_was_default {
        crate::agent::manifest::default_blob_registry_root_for_agent_registry_root(
            &config.agent_registry_root,
        )
    } else {
        resolve_path_against_cwd(&config.cwd, &config.blob_registry_root)
    };
    config.skill_registry_root = resolve_path_against_cwd(&config.cwd, &config.skill_registry_root);
    if let Some(registry_root) = &config.capsule_bindings.registry_root {
        config.capsule_bindings.registry_root =
            Some(resolve_path_against_cwd(&config.cwd, registry_root));
    }
}

pub(crate) fn resolve_path_against_cwd(
    cwd: &std::path::Path,
    path: &std::path::Path,
) -> std::path::PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

pub(crate) fn thread_start_topology(
    params: &crate::adapters::app_server::connection::ThreadStartParams,
) -> Result<
    verlet_runtime_contracts::ThreadTopology,
    crate::adapters::app_server::connection::JsonRpcErrorError,
> {
    match (&params.topology, &params.parent_thread_id) {
        (Some(_), Some(_)) => Err(crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            "thread/start accepts either topology or parentThreadId, not both",
        )),
        (Some(topology), None) => Ok(topology.clone()),
        (None, Some(parent_thread_id)) => {
            let parent_thread_id = verlet_runtime_contracts::ThreadId::parse_str(parent_thread_id)
                .map_err(|err| {
                    crate::adapters::app_server::connection::jsonrpc_error(
                        -32602,
                        format!("invalid parentThreadId: {err}"),
                    )
                })?;
            Ok(verlet_runtime_contracts::ThreadTopology::spawned_from(
                parent_thread_id,
            ))
        }
        (None, None) => Ok(verlet_runtime_contracts::ThreadTopology::root()),
    }
}

pub(crate) fn thread_start_metadata(
    params: &crate::adapters::app_server::connection::ThreadStartParams,
    cwd: &std::path::Path,
    model_provider: &str,
    ephemeral: bool,
) -> Result<
    std::collections::BTreeMap<String, String>,
    crate::adapters::app_server::connection::JsonRpcErrorError,
> {
    let mut metadata = app_server_thread_metadata(cwd, model_provider, ephemeral);
    if let Some(thinking) = &params.thinking {
        insert_app_server_thinking_metadata(&mut metadata, Some(thinking))?;
    }
    Ok(metadata)
}

pub(crate) fn insert_app_server_thinking_metadata(
    metadata: &mut std::collections::BTreeMap<String, String>,
    thinking: Option<&verlet_provider::ThinkingConfig>,
) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
    if let Some(thinking) = thinking {
        metadata.insert(
            crate::adapters::app_server::THREAD_APP_SERVER_THINKING_METADATA.to_string(),
            encode_app_server_thinking(thinking)?,
        );
    }
    Ok(())
}

pub(crate) fn append_bound_agent_metadata(
    metadata: &mut std::collections::BTreeMap<String, String>,
    bound: &crate::agent::manifest_bind::AgentManifestBoundThread,
    overrides: Option<&crate::agent::manifest_bind::AgentManifestBindOverrides>,
    operation_registry_root: Option<&std::path::Path>,
) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
    metadata.insert(
        crate::adapters::app_server::THREAD_AGENT_REF_METADATA.to_string(),
        bound.bind_receipt.ref_uri.clone(),
    );
    metadata.insert(
        crate::adapters::app_server::THREAD_AGENT_MANIFEST_HASH_METADATA.to_string(),
        bound.bind_receipt.manifest_hash.clone(),
    );
    metadata.insert(
        crate::adapters::app_server::THREAD_AGENT_SOURCE_HASH_METADATA.to_string(),
        bound.compile_receipt.source_hash.clone(),
    );
    metadata.insert(
        crate::adapters::app_server::THREAD_AGENT_MODEL_PROFILE_ID_METADATA.to_string(),
        bound.bind_receipt.model_profile_id.clone(),
    );
    metadata.insert(
        crate::adapters::app_server::THREAD_AGENT_PROVIDER_ID_METADATA.to_string(),
        bound.bind_receipt.provider_id.clone(),
    );
    metadata.insert(
        crate::adapters::app_server::THREAD_AGENT_MODEL_ID_METADATA.to_string(),
        bound.bind_receipt.model_id.clone(),
    );
    if let Some(placement) = &bound.bind_receipt.placement {
        let encoded = serde_json::to_string(placement).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest placement binding: {err}"),
            )
        })?;
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_PLACEMENT_METADATA.to_string(),
            encoded,
        );
    }
    if let Some(workspace) = &bound.bind_receipt.workspace {
        let encoded = serde_json::to_string(workspace).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest workspace binding: {err}"),
            )
        })?;
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA.to_string(),
            encoded,
        );
    }
    if let Some(instruction) = manifest_tool_use_system_instruction(bound) {
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA.to_string(),
            instruction,
        );
    } else {
        metadata.remove(crate::adapters::app_server::THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA);
    }
    metadata.insert(
        crate::adapters::app_server::THREAD_AGENT_RUNTIME_STREAMING_METADATA.to_string(),
        bound.bind_receipt.effective_runtime.streaming.to_string(),
    );
    if let Some(max_tool_rounds) = bound.bind_receipt.effective_runtime.max_tool_rounds {
        let value = match max_tool_rounds {
            verlet_agent::manifest_schema::AgentManifestMaxToolRounds::Limited(rounds) => {
                rounds.to_string()
            }
            verlet_agent::manifest_schema::AgentManifestMaxToolRounds::Unlimited => {
                "unlimited".to_string()
            }
        };
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA.to_string(),
            value,
        );
    }
    if let Some(auto_at_text_bytes) = bound
        .bind_receipt
        .effective_runtime
        .compaction
        .auto_at_text_bytes
    {
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_RUNTIME_COMPACTION_AUTO_AT_TEXT_BYTES_METADATA.to_string(),
            auto_at_text_bytes.to_string(),
        );
    }
    let encoded = serde_json::to_string(&bound.operation_bindings).map_err(|err| {
        crate::adapters::app_server::connection::jsonrpc_error(
            -32602,
            format!("failed to encode manifest operation bindings: {err}"),
        )
    })?;
    metadata.insert(
        crate::adapters::app_server::THREAD_AGENT_OPERATION_BINDINGS_METADATA.to_string(),
        encoded,
    );
    if !bound.skill_packages.is_empty() {
        let encoded = serde_json::to_string(&bound.skill_packages).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest skill package bindings: {err}"),
            )
        })?;
        metadata.insert(
            crate::agent::manifest_bind::THREAD_AGENT_SKILL_PACKAGES_METADATA.to_string(),
            encoded,
        );
    }
    if let Some(discovery) = &bound.skill_discovery {
        let encoded = serde_json::to_string(discovery).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest skill discovery witness: {err}"),
            )
        })?;
        metadata.insert(
            crate::agent::manifest_bind::THREAD_AGENT_SKILL_DISCOVERY_METADATA.to_string(),
            encoded,
        );
    }
    if !bound.skill_context_segments.is_empty() {
        let encoded = serde_json::to_string(&bound.skill_context_segments).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest skill context segments: {err}"),
            )
        })?;
        metadata.insert(
            crate::agent::manifest_bind::THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA.to_string(),
            encoded,
        );
    }
    if !bound.static_context_segments.is_empty() {
        let encoded = serde_json::to_string(&bound.static_context_segments).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest static context segments: {err}"),
            )
        })?;
        metadata.insert(
            crate::agent::manifest_bind::THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA.to_string(),
            encoded,
        );
    }
    if !bound.tool_universes.is_empty() {
        let encoded = serde_json::to_string(&bound.tool_universes).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest tool universes: {err}"),
            )
        })?;
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_TOOL_UNIVERSES_METADATA.to_string(),
            encoded,
        );
    }
    if !bound.coupling_set.couplings.is_empty() {
        let encoded = serde_json::to_string(&bound.coupling_set).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest bound coupling set: {err}"),
            )
        })?;
        metadata.insert(
            crate::kernel::runtime_host::THREAD_BOUND_COUPLING_SET_METADATA.to_string(),
            encoded,
        );
        if let Some(root) = operation_registry_root {
            metadata.insert(
                crate::kernel::runtime_host::THREAD_OPERATION_REGISTRY_ROOT_METADATA.to_string(),
                root.display().to_string(),
            );
        }
    }
    if let Some(overrides) = overrides {
        let encoded = serde_json::to_string(overrides).map_err(|err| {
            crate::adapters::app_server::connection::jsonrpc_error(
                -32602,
                format!("failed to encode manifest runtime overrides: {err}"),
            )
        })?;
        metadata.insert(
            crate::adapters::app_server::THREAD_AGENT_RUNTIME_OVERRIDES_METADATA.to_string(),
            encoded,
        );
    }
    Ok(())
}

fn manifest_tool_use_system_instruction(
    bound: &crate::agent::manifest_bind::AgentManifestBoundThread,
) -> Option<String> {
    if bound.bind_receipt.tool_ids.is_empty() {
        return None;
    }
    let mut tools = bound.bind_receipt.tool_ids.clone();
    tools.sort();
    Some(manifest_tool_use_instruction_text(
        &bound.bind_receipt.ref_uri,
        Some(&tools.join(", ")),
    ))
}

fn manifest_tool_use_instruction_text(agent_ref: &str, tool_list: Option<&str>) -> String {
    let tool_sentence = tool_list
        .map(|tools| format!("You have these Verlet tools available: {tools}. "))
        .unwrap_or_else(|| {
            "You have Verlet tools available for this manifest-backed thread. ".to_string()
        });
    format!(
        "You are running as {agent_ref}. {tool_sentence}\
Use the tools when they are the right way to satisfy the user's request. When the user asks you \
to read a file, run a shell command, inspect workspace state, spawn/check/wait for child work, or \
perform any action covered by a listed tool, call the tool immediately instead of only saying you \
will. For shell-style requests, use the bash/operation tool surface when it is available. After a \
tool result returns, report the result briefly. Do not claim that a tool ran unless a tool result \
is present in the conversation.",
    )
}

pub(crate) async fn record_bound_agent_receipts(
    handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
    bound: &crate::agent::manifest_bind::AgentManifestBoundThread,
    principal_id: &str,
) -> crate::kernel::runtime_host::VerletResult<(
    verlet_history::EventRecord,
    verlet_history::EventRecord,
)> {
    let compile_payload = serde_json::to_value(&bound.compile_receipt).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode manifest compile receipt: {err}"
        ))
    })?;
    let bind_payload = serde_json::to_value(&bound.bind_receipt).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode manifest bind receipt: {err}"
        ))
    })?;
    let manifest_events = handle
        .record_manifest_receipts_for_principal(compile_payload, bind_payload, principal_id)
        .await?;
    let discovery_payloads = bound
        .tool_universes
        .iter()
        .map(|binding| {
            serde_json::to_value(
                crate::agent::tool_universe::ToolUniverseDiscoveryReceipt::from_discovery(
                    &binding.discovery,
                ),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to encode tool universe discovery receipt: {err}"
            ))
        })?;
    handle
        .record_tool_universe_discovery_receipts(discovery_payloads)
        .await?;
    Ok(manifest_events)
}

pub(crate) async fn active_manifest_receipt_payloads(
    handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
) -> crate::kernel::runtime_host::VerletResult<Option<(serde_json::Value, serde_json::Value)>> {
    let events = handle.read_thread_events(None).await?;
    validate_manifest_binding_event_contract(&events)?;
    let Some(bind) = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
        .max_by_key(|event| event.sequence.get())
    else {
        return Ok(None);
    };
    let compile_id = bind.provenance.source_event_ids.first().ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::History(
            "active manifest bind receipt does not witness a compile receipt".to_string(),
        )
    })?;
    let compile = events
        .iter()
        .find(|event| {
            event.id == *compile_id
                && event.kind == verlet_history::EventKind::ManifestCompileCompleted
        })
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(
                "active manifest bind receipt references an unavailable compile receipt"
                    .to_string(),
            )
        })?;
    crate::agent::manifest_bind::decode_manifest_compile_receipt_event(compile)?;
    crate::agent::manifest_bind::decode_manifest_bind_receipt_event(bind)?;
    Ok(Some((compile.payload.clone(), bind.payload.clone())))
}

impl crate::adapters::app_server::VerletAppServer {
    pub(crate) async fn witness_bound_agent_and_persist_lifecycle(
        &self,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
        bound: crate::agent::manifest_bind::AgentManifestBoundThread,
        principal_id: String,
    ) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
        let app = self.clone();
        self.witness_and_persist_lifecycle(handle, move |handle| async move {
            record_bound_agent_receipts(&handle, &bound, &principal_id).await?;
            app.persist_thread_lifecycle_record_with_metadata(
                &handle,
                std::collections::BTreeMap::new(),
            )
            .await?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn witness_manifest_payloads_and_persist_lifecycle(
        &self,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
        compile_payload: serde_json::Value,
        bind_payload: serde_json::Value,
        principal_id: String,
    ) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError> {
        let app = self.clone();
        self.witness_and_persist_lifecycle(handle, move |handle| async move {
            handle
                .record_manifest_receipts_for_principal(
                    compile_payload,
                    bind_payload,
                    &principal_id,
                )
                .await?;
            app.persist_thread_lifecycle_record_with_metadata(
                &handle,
                std::collections::BTreeMap::new(),
            )
            .await?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn witness_and_persist_lifecycle<F, Fut>(
        &self,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
        operation: F,
    ) -> Result<(), crate::adapters::app_server::connection::JsonRpcErrorError>
    where
        F: FnOnce(crate::kernel::runtime_host::RuntimeThreadHandle) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = crate::kernel::runtime_host::VerletResult<()>>
            + Send
            + 'static,
    {
        let supervisor = self.inner.supervisor.clone();
        let coordinates = handle.context().coordinates.clone();
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        if !self.inner.tasks.spawn(async move {
            let result = if let Err(err) = operation(handle.clone()).await {
                let _ = supervisor.shutdown_thread_at(&coordinates).await;
                Err(crate::adapters::app_server::connection::internal_error(err))
            } else {
                Ok(())
            };
            let _ = completion_tx.send(result);
        }) {
            return Err(crate::adapters::app_server::connection::internal_error(
                crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "Verlet app-server instance shut down before manifest lifecycle witnessing started"
                        .to_string(),
                ),
            ));
        }
        completion_rx.await.map_err(|err| {
            crate::adapters::app_server::connection::internal_error(
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "manifest lifecycle witness task failed: {err}"
                )),
            )
        })?
    }
}

fn kernel_thread_spawn_agent_binding(
    bound: &crate::agent::manifest_bind::AgentManifestBoundThread,
    cwd_root: &std::path::Path,
    operation_registry_root: Option<&std::path::Path>,
    overrides: Option<&crate::agent::manifest_bind::AgentManifestBindOverrides>,
    principal_id: String,
) -> crate::kernel::runtime_host::VerletResult<
    crate::agent::agent_process::KernelThreadSpawnAgentBinding,
> {
    let cwd = resolve_cwd(
        cwd_root,
        Some(bound.bind_receipt.effective_runtime.default_cwd.as_str()),
    );
    let mut metadata = app_server_thread_metadata(&cwd, &bound.bind_receipt.provider_id, false);
    append_bound_agent_metadata(&mut metadata, bound, overrides, operation_registry_root)
        .map_err(|err| crate::kernel::runtime_host::VerletError::RuntimeFactory(err.message))?;
    let compile_receipt = serde_json::to_value(&bound.compile_receipt).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode manifest compile receipt: {err}"
        ))
    })?;
    let bind_receipt = serde_json::to_value(&bound.bind_receipt).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode manifest bind receipt: {err}"
        ))
    })?;
    Ok(crate::agent::agent_process::KernelThreadSpawnAgentBinding {
        metadata,
        compile_receipt,
        bind_receipt,
        principal_id,
    })
}

pub(crate) fn app_server_thread_metadata(
    cwd: &std::path::Path,
    model_provider: &str,
    ephemeral: bool,
) -> std::collections::BTreeMap<String, String> {
    app_server_thread_metadata_with_name(cwd, model_provider, ephemeral, None)
}

pub(crate) fn app_server_thread_metadata_with_name(
    cwd: &std::path::Path,
    model_provider: &str,
    ephemeral: bool,
    name: Option<&str>,
) -> std::collections::BTreeMap<String, String> {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        crate::adapters::app_server::THREAD_APP_SERVER_CWD_METADATA.to_string(),
        crate::adapters::app_server::connection::cwd_string(cwd),
    );
    metadata.insert(
        crate::adapters::app_server::THREAD_APP_SERVER_MODEL_PROVIDER_METADATA.to_string(),
        model_provider.to_string(),
    );
    metadata.insert(
        crate::adapters::app_server::THREAD_APP_SERVER_EPHEMERAL_METADATA.to_string(),
        ephemeral.to_string(),
    );
    if let Some(name) = name.filter(|name| !name.trim().is_empty()) {
        metadata.insert(
            crate::adapters::app_server::THREAD_APP_SERVER_NAME_METADATA.to_string(),
            name.to_string(),
        );
    }
    metadata
}

pub(crate) fn thread_lifecycle_cwd(
    record: &verlet_runtime_contracts::ThreadLifecycleRecord,
) -> Option<std::path::PathBuf> {
    record
        .metadata
        .get(crate::adapters::app_server::THREAD_APP_SERVER_CWD_METADATA)
        .filter(|cwd| !cwd.trim().is_empty())
        .map(std::path::PathBuf::from)
}

pub(crate) fn thread_lifecycle_thinking(
    record: &verlet_runtime_contracts::ThreadLifecycleRecord,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_provider::ThinkingConfig>> {
    thread_metadata_thinking(&record.metadata)
}

pub(crate) fn thread_metadata_thinking(
    metadata: &std::collections::BTreeMap<String, String>,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_provider::ThinkingConfig>> {
    metadata
        .get(crate::adapters::app_server::THREAD_APP_SERVER_THINKING_METADATA)
        .map(|raw| {
            let value = serde_json::from_str::<serde_json::Value>(raw).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "thread thinking metadata is invalid: {err}"
                ))
            })?;
            thinking_from_app_server_value(&value).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "thread thinking metadata is invalid: {err}"
                ))
            })
        })
        .transpose()
}

pub(crate) fn is_loadable_lifecycle_status(
    status: verlet_runtime_contracts::ThreadLifecycleStatus,
) -> bool {
    !matches!(
        status,
        verlet_runtime_contracts::ThreadLifecycleStatus::Stopped
            | verlet_runtime_contracts::ThreadLifecycleStatus::Failed
    )
}

pub(crate) fn thread_manifest_operation_bindings(
    context: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<
    Vec<crate::agent::manifest_bind::AgentManifestOperationBinding>,
> {
    let Some(raw) = context
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_OPERATION_BINDINGS_METADATA)
    else {
        return Ok(Vec::new());
    };
    serde_json::from_str::<Vec<crate::agent::manifest_bind::AgentManifestOperationBinding>>(raw)
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "thread {} manifest operation binding metadata is invalid: {err}",
                context.coordinates.thread_id
            ))
        })
}

pub(crate) fn validate_manifest_binding_event_contract(
    events: &[verlet_history::EventRecord],
) -> crate::kernel::runtime_host::VerletResult<()> {
    if events.iter().any(|event| {
        matches!(
            event.kind,
            verlet_history::EventKind::BindingAttached | verlet_history::EventKind::BindingDetached
        )
    }) {
        return Ok(());
    }
    for event in events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
    {
        let receipt = crate::agent::manifest_bind::decode_manifest_bind_receipt_event(event)?;
        if !receipt.operation_bindings.is_empty() {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread {} has manifest bind receipt {} declaring operation bindings but no binding events; start a new thread",
                event.coordinates.thread_id, event.id
            )));
        }
    }
    Ok(())
}

pub(crate) fn thread_operation_bindings_from_events(
    events: &[verlet_history::EventRecord],
) -> crate::kernel::runtime_host::VerletResult<Vec<ThreadOperationBinding>> {
    validate_manifest_binding_event_contract(events)?;
    if events.iter().any(|event| {
        matches!(
            event.kind,
            verlet_history::EventKind::BindingAttached | verlet_history::EventKind::BindingDetached
        )
    }) {
        let folded = crate::kernel::binding_projector::fold_thread_bindings(events);
        if let Some(message) = folded.anomaly_message() {
            let thread = events
                .first()
                .map(|event| event.coordinates.thread_id.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread {thread}: {message}"
            )));
        }
        let bindings = folded
            .active
            .into_iter()
            .map(|binding| ThreadOperationBinding {
                binding: crate::agent::manifest_bind::operation_binding_from_attached_payload(
                    binding.payload,
                ),
                attach_event_id: Some(binding.attach_event_id),
            })
            .collect();
        return Ok(bindings);
    }

    Ok(Vec::new())
}

pub(crate) fn thread_manifest_workspace_mount(
    context: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<
    Option<crate::agent::manifest_bind::AgentManifestResolvedWorkspaceMount>,
> {
    context
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA)
        .map(|raw| {
            serde_json::from_str::<crate::agent::manifest_bind::AgentManifestResolvedWorkspaceMount>(raw).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "thread manifest workspace binding is invalid: {err}"
                ))
            })
        })
        .transpose()
}

pub(crate) fn thread_manifest_tool_universes(
    context: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<Vec<crate::agent::tool_universe::ToolUniverseBinding>>
{
    let Some(raw) = context
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_TOOL_UNIVERSES_METADATA)
    else {
        return Ok(Vec::new());
    };
    let bindings =
        serde_json::from_str::<Vec<crate::agent::tool_universe::ToolUniverseBinding>>(raw)
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "thread manifest tool universes are invalid: {err}"
                ))
            })?;
    for binding in &bindings {
        binding.validate()?;
    }
    Ok(bindings)
}

pub(crate) fn thread_manifest_skill_packages(
    context: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<
    Vec<crate::agent::manifest_bind::AgentManifestSkillPackageBinding>,
> {
    let Some(raw) = context
        .metadata
        .get(crate::agent::manifest_bind::THREAD_AGENT_SKILL_PACKAGES_METADATA)
    else {
        return Ok(Vec::new());
    };
    let bindings = serde_json::from_str::<
        Vec<crate::agent::manifest_bind::AgentManifestSkillPackageBinding>,
    >(raw)
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "thread manifest skill package bindings are invalid: {err}"
        ))
    })?;
    Ok(bindings)
}

pub(crate) fn thread_manifest_skill_discovery(
    context: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<
    Option<crate::agent::manifest_bind::AgentManifestSkillDiscovery>,
> {
    context
        .metadata
        .get(crate::agent::manifest_bind::THREAD_AGENT_SKILL_DISCOVERY_METADATA)
        .map(|raw| {
            serde_json::from_str(raw).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "thread manifest skill discovery witness is invalid: {err}"
                ))
            })
        })
        .transpose()
}

pub(crate) fn thread_manifest_skill_context_segments(
    context: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<
    Vec<crate::agent::manifest_bind::AgentManifestStaticContextSegment>,
> {
    let Some(raw) = context
        .metadata
        .get(crate::agent::manifest_bind::THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA)
    else {
        return Ok(Vec::new());
    };
    serde_json::from_str(raw).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "thread manifest skill context segments are invalid: {err}"
        ))
    })
}

pub(crate) struct CapsuleBindingRuntimeFactory {
    pub(crate) config: crate::adapters::agent_loop::AgentLoopConfig,
    pub(crate) client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    pub(crate) capsule_bindings: crate::adapters::app_server::CapsuleBindingsConfig,
    pub(crate) secret_resolver:
        Option<std::sync::Arc<dyn verlet_metadata::secret_store::SecretResolver>>,
    pub(crate) metadata_store_path: Option<std::path::PathBuf>,
    pub(crate) secret_store_path: Option<std::path::PathBuf>,
    pub(crate) session_store_path: Option<std::path::PathBuf>,
    pub(crate) lease_epoch: u64,
    pub(crate) agent_registry_root: Option<std::path::PathBuf>,
    pub(crate) blob_registry_root: Option<std::path::PathBuf>,
    pub(crate) skill_registry_root: Option<std::path::PathBuf>,
    pub(crate) cwd: Option<std::path::PathBuf>,
    pub(crate) hook_shell: Option<String>,
    pub(crate) turn_endpoint_router:
        Option<std::sync::Arc<dyn crate::adapters::agent_loop::TurnEndpointRouter>>,
    pub(crate) default_placement: crate::agent::manifest_bind::AgentManifestPlacementBinding,
    pub(crate) default_workspace:
        Option<crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
    pub(crate) remote_event_store_served: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

struct ThreadOperationCatalog {
    registry: std::sync::Arc<verlet_operations::operation_registry::OperationRegistry>,
    tool_aliases: Vec<crate::agent::agent_tool_router::OperationToolAlias>,
    /// The per-thread workspace VFS installed into catalog-loaded operations and
    /// virtual bash so filesystem surfaces do not drift into separate trees.
    workspace_vfs: std::sync::Arc<verlet_vfs::VerletVfs>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ThreadOperationBinding {
    pub(crate) binding: crate::agent::manifest_bind::AgentManifestOperationBinding,
    pub(crate) attach_event_id: Option<verlet_history::EventRecordId>,
}

fn context_operation_binding_plan(
    context: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<Vec<ThreadOperationBinding>> {
    Ok(thread_manifest_operation_bindings(context)?
        .into_iter()
        .map(|binding| ThreadOperationBinding {
            binding,
            attach_event_id: None,
        })
        .collect())
}

pub(crate) async fn runtime_operation_bindings_for_thread(
    context: &verlet_runtime_contracts::ThreadContext,
    session_store_path: Option<&std::path::Path>,
    metadata_store_path: Option<&std::path::Path>,
    lease_epoch: u64,
) -> crate::kernel::runtime_host::VerletResult<Vec<ThreadOperationBinding>> {
    let Some(session_store_path) = session_store_path else {
        return context_operation_binding_plan(context);
    };
    let store = verlet_history_sqlite::SqliteSessionStore::open(session_store_path)
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
        .with_lease_epoch(lease_epoch);
    let events = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(&context.coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    if !events.is_empty() {
        return thread_operation_bindings_from_events(&events);
    }

    if let Some(metadata_store_path) = metadata_store_path {
        let metadata_store =
            verlet_metadata::provider_store::SqliteMetadataStore::open(metadata_store_path)
                .await
                .map_err(crate::adapters::app_server::metadata_store_error)?;
        if metadata_store
            .get_thread_lifecycle(context.coordinates.thread_id)
            .await
            .map_err(crate::adapters::app_server::metadata_store_error)?
            .is_some()
        {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread {} has persisted lifecycle metadata but an empty event stream; start a new thread",
                context.coordinates.thread_id
            )));
        }
    }

    // A new runtime is constructed before its atomic start and binding event
    // batch is appended. Only that unpersisted boundary may use the context
    // plan; a durable lifecycle with no events is rejected above.
    context_operation_binding_plan(context)
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory
    for CapsuleBindingRuntimeFactory
{
    async fn build(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<
        Box<dyn crate::kernel::runtime_host::runtime_api::AgentRuntime>,
    > {
        let mut config = self.config.clone();
        apply_manifest_runtime_metadata(context, &mut config)?;
        let mut factory = crate::adapters::agent_loop::AgentLoopFactory::new(
            config,
            std::sync::Arc::clone(&self.client),
        )
        .with_hook_shell(self.hook_shell.clone())
        .with_process_dispatcher_cwd(self.cwd.clone());
        if let Some(router) = &self.turn_endpoint_router {
            factory = factory.with_turn_endpoint_router(std::sync::Arc::clone(router));
        }
        if let Some(policy) = manifest_compaction_policy(context)? {
            factory = factory.with_compaction_policy(policy);
        }
        if let Some(resolver) = self.thread_spawn_agent_resolver() {
            factory = factory.with_thread_spawn_agent_resolver(std::sync::Arc::new(resolver));
        }
        let mut tool_router = None;
        let skill_files = self.skill_mount_files_for_thread(context).await?;
        if let Some(catalog) = self.operation_catalog_for_thread(context).await? {
            let ThreadOperationCatalog {
                registry,
                tool_aliases,
                workspace_vfs,
            } = catalog;
            let capability_grants = operation_registry_capability_grants(&registry).await;
            tool_router = Some(
                crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::clone(
                    &registry,
                ))
                .with_tool_aliases(tool_aliases),
            );
            factory = factory.with_bash_tool(bash_config_with_skill_files(
                crate::capabilities::execution::VirtualBashRuntimeConfig::default()
                    .with_operation_registry(registry)
                    .with_workspace_vfs(workspace_vfs)
                    .with_capability_grants(capability_grants),
                &skill_files,
            ));
        } else if !skill_files.is_empty() {
            factory = factory.with_bash_tool(bash_config_with_skill_files(
                crate::capabilities::execution::VirtualBashRuntimeConfig::default(),
                &skill_files,
            ));
        }
        if let Some(tool_universe_surface) = self.tool_universe_search_surface(context).await? {
            let router = tool_router
                .take()
                .unwrap_or_else(|| {
                    crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
                        verlet_operations::operation_registry::OperationRegistry::new(),
                    ))
                })
                .with_kernel_tool_provider(std::sync::Arc::new(tool_universe_surface));
            tool_router = Some(router);
        }
        if let Some(tool_router) = tool_router {
            factory = factory.with_tool_router(std::sync::Arc::new(tool_router));
        }
        factory.build(context).await
    }
}

#[derive(Clone)]
pub(crate) struct AppServerThreadSpawnAgentResolver {
    agent_registry_root: std::path::PathBuf,
    operation_registry_root: Option<std::path::PathBuf>,
    blob_registry_root: Option<std::path::PathBuf>,
    skill_registry_root: Option<std::path::PathBuf>,
    metadata_store_path: Option<std::path::PathBuf>,
    secret_store_path: Option<std::path::PathBuf>,
    cwd: std::path::PathBuf,
    provider_surface: crate::agent::manifest_bind::AgentManifestProviderSurface,
    default_placement: crate::agent::manifest_bind::AgentManifestPlacementBinding,
    default_workspace: Option<crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
    remote_event_store_served: std::sync::Arc<std::sync::atomic::AtomicBool>,
    placement_override: Option<crate::agent::manifest_bind::AgentManifestPlacementBinding>,
    workspace_override: Option<crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
    binding_principal_id: Option<String>,
}

#[async_trait::async_trait]
impl crate::agent::agent_process::KernelThreadSpawnAgentResolver
    for AppServerThreadSpawnAgentResolver
{
    fn default_agent_ref(
        &self,
        _caller: &verlet_runtime_contracts::ThreadContext,
    ) -> Option<String> {
        Some(crate::adapters::app_server::default_manifest::DEFAULT_AGENT_REF.to_string())
    }

    async fn resolve_agent_ref(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        agent_ref: &str,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::agent::agent_process::KernelThreadSpawnAgentBinding,
    > {
        let mut registry =
            crate::agent::manifest::LocalAgentRegistry::new(self.agent_registry_root.clone());
        if let Some(blob_registry_root) = &self.blob_registry_root {
            registry = registry.with_blob_registry_root(blob_registry_root.clone());
        }
        let (record, alias) = registry.load_ref_with_alias_receipt(agent_ref)?;
        let mcp_server_refs = self.configured_mcp_server_refs().await?;
        let tool_universe_discoverer = self.tool_universe_discoverer().await?;
        let bound = crate::agent::manifest_bind::bind_published_agent_record_with_placement(
            &record,
            alias,
            &self.provider_surface,
            self.operation_registry_root.as_deref(),
            self.blob_registry_root.as_deref(),
            self.skill_registry_root.as_deref(),
            &mcp_server_refs,
            tool_universe_discoverer.as_ref().map(|discoverer| {
                discoverer as &dyn crate::agent::tool_universe::ToolUniverseDiscoverer
            }),
            &crate::agent::manifest_bind::AgentManifestModelProfileSelection::default(),
            &crate::agent::manifest_bind::AgentManifestBindOverrides::default(),
            Some(&self.default_placement),
            self.placement_override.as_ref(),
            self.default_workspace.as_ref(),
            self.workspace_override.as_ref(),
            self.remote_event_store_served
                .load(std::sync::atomic::Ordering::Acquire),
        )
        .await?;
        kernel_thread_spawn_agent_binding(
            &bound,
            &self.cwd,
            self.operation_registry_root.as_deref(),
            None,
            self.binding_principal_id
                .clone()
                .unwrap_or_else(|| caller.coordinates.user_id.clone()),
        )
    }
}

impl AppServerThreadSpawnAgentResolver {
    async fn configured_mcp_server_refs(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<std::collections::BTreeSet<String>> {
        let Some(metadata_store_path) = &self.metadata_store_path else {
            return Ok(std::collections::BTreeSet::new());
        };
        let registry =
            crate::adapters::mcp_client::SqliteMcpSourceRegistry::open_async(metadata_store_path)
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
                })?;
        Ok(registry
            .list_sources_async()
            .await
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
            })?
            .into_iter()
            .map(|source| format!("mcp://{}", source.name))
            .collect())
    }

    async fn tool_universe_discoverer(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<crate::adapters::mcp_client::McpToolUniverseDiscoverer>,
    > {
        let Some(metadata_store_path) = &self.metadata_store_path else {
            return Ok(None);
        };
        let registry =
            crate::adapters::mcp_client::SqliteMcpSourceRegistry::open_async(metadata_store_path)
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
                })?;
        let secret_store_path = self
            .secret_store_path
            .as_ref()
            .unwrap_or(metadata_store_path);
        let secret_store =
            verlet_metadata::secret_store::SqliteSecretStore::open(secret_store_path)
                .await
                .map_err(crate::adapters::app_server::secret_store_error)?;
        Ok(Some(
            crate::adapters::mcp_client::McpToolUniverseDiscoverer::new(
                registry,
                Some(std::sync::Arc::new(secret_store)),
            ),
        ))
    }
}

impl crate::adapters::app_server::VerletAppServer {
    pub(crate) async fn app_server_thread_spawn_agent_resolver(
        &self,
        placement_override: Option<crate::agent::manifest_bind::AgentManifestPlacementBinding>,
        workspace_override: Option<crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
        binding_principal_id: String,
    ) -> crate::kernel::runtime_host::VerletResult<AppServerThreadSpawnAgentResolver> {
        Ok(AppServerThreadSpawnAgentResolver {
            agent_registry_root: self.inner.agent_registry_root.clone(),
            operation_registry_root: self.inner.capsule_bindings.registry_root.clone(),
            blob_registry_root: Some(self.inner.blob_registry_root.clone()),
            skill_registry_root: Some(self.inner.skill_registry_root.clone()),
            metadata_store_path: Some(self.inner.metadata_store_path.clone()),
            secret_store_path: Some(self.inner.user_metadata_store_path.clone()),
            cwd: self.inner.cwd.clone(),
            provider_surface: self.agent_manifest_provider_surface().await?,
            default_placement: self.inner.default_placement.clone(),
            default_workspace: self.inner.default_workspace.clone(),
            remote_event_store_served: std::sync::Arc::clone(&self.inner.remote_event_store_served),
            placement_override,
            workspace_override,
            binding_principal_id: Some(binding_principal_id),
        })
    }
}

// lexicon-allow: capsule - current app-server type name
impl CapsuleBindingRuntimeFactory {
    fn thread_spawn_agent_resolver(&self) -> Option<AppServerThreadSpawnAgentResolver> {
        let agent_registry_root = self.agent_registry_root.clone()?;
        let cwd = self.cwd.clone()?;
        Some(AppServerThreadSpawnAgentResolver {
            agent_registry_root,
            // lexicon-allow: capsule - existing app-server config field
            operation_registry_root: self.capsule_bindings.registry_root.clone(),
            blob_registry_root: self.blob_registry_root.clone(),
            skill_registry_root: self.skill_registry_root.clone(),
            metadata_store_path: self.metadata_store_path.clone(),
            secret_store_path: self.secret_store_path.clone(),
            cwd,
            provider_surface: provider_surface_for_runtime_config(&self.config),
            default_placement: self.default_placement.clone(),
            default_workspace: self.default_workspace.clone(),
            remote_event_store_served: std::sync::Arc::clone(&self.remote_event_store_served),
            placement_override: None,
            workspace_override: None,
            binding_principal_id: None,
        })
    }

    async fn skill_mount_files_for_thread(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_vbash::VirtualFile>> {
        let bindings = thread_manifest_skill_packages(context)?;
        if bindings.is_empty() {
            return Ok(Vec::new());
        }
        let registry_root = self.skill_registry_root.as_ref().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "skill package bindings require an app-server skill registry root".to_string(),
            )
        })?;
        let registry = verlet_operations::skill_package::LocalSkillRegistry::new(registry_root);
        let mut files = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        for binding in bindings {
            let record = registry
                .load_version_record(&binding.package_name, &binding.artifact_hash)
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "manifest skill package binding {:?}@sha256:{} was not found: {err}",
                        binding.package_name, binding.artifact_hash
                    ))
                })?;
            if record.ref_uri() != binding.ref_uri {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "manifest skill package binding {:?} ref drift: receipt {}, registry {}",
                        binding.resource_name,
                        binding.ref_uri,
                        record.ref_uri()
                    ),
                ));
            }
            for skill in record.package.skills {
                if !names.insert(skill.name.clone()) {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                        format!(
                            "manifest skill packages contain duplicate /skills/{}.md",
                            skill.name
                        ),
                    ));
                }
                files.push(verlet_vbash::VirtualFile::new(
                    std::path::PathBuf::from(format!("{}.md", skill.name)),
                    skill.body.into_bytes(),
                ));
            }
        }
        Ok(files)
    }

    async fn operation_bindings_for_thread(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<ThreadOperationBinding>> {
        runtime_operation_bindings_for_thread(
            context,
            self.session_store_path.as_deref(),
            self.metadata_store_path.as_deref(),
            self.lease_epoch,
        )
        .await
    }

    async fn operation_catalog_for_thread(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Option<ThreadOperationCatalog>> {
        let manifest_operation_bindings = self.operation_bindings_for_thread(context).await?;
        let workspace = thread_manifest_workspace_mount(context)?;
        if manifest_operation_bindings.is_empty() && workspace.is_none() {
            return Ok(None);
        }
        // Workspace-only threads do not read an operation registry, but the
        // shared catalog remains the single assembly path for their VFS.
        // lexicon-allow: capsule - existing app-server config field
        let registry_root = match &self.capsule_bindings.registry_root {
            Some(registry_root) => registry_root.clone(),
            None if manifest_operation_bindings.is_empty() => self
                .cwd
                .clone()
                .unwrap_or_else(|| std::path::PathBuf::from(".")),
            // lexicon-allow: capsule - existing app-server config field
            None => {
                // lexicon-allow: capsule - existing app-server config error text
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "capsule bindings require a registry root".to_string(), // lexicon-allow: capsule - existing app-server config error text
                ));
            }
        };
        let registry =
            verlet_operations::operation_store::LocalOperationRegistry::new(&registry_root);
        let mut records = Vec::new();
        let mut tool_aliases = Vec::new();
        for thread_binding in manifest_operation_bindings {
            let ThreadOperationBinding {
                binding,
                attach_event_id,
            } = thread_binding;
            let crate::agent::manifest_bind::AgentManifestOperationBinding {
                name,
                artifact_hash,
                effect_class: _,
                attachment_config,
                operations,
                direct_tools,
            } = binding;
            let record = registry
                .load_version_record(&name, &artifact_hash)
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "manifest operation binding {:?}@sha256:{} was not found: {err}",
                        name, artifact_hash
                    ))
                })?;
            let is_kernel = matches!(
                &record.source,
                verlet_operations::operation_store::PublishedOperationSource::Kernel { .. }
            );
            let alias_attach_event_id = if is_kernel { None } else { attach_event_id };
            tool_aliases.extend(direct_tools.into_iter().map(|direct_tool| {
                crate::agent::agent_tool_router::OperationToolAlias {
                    tool_name: direct_tool.tool_name,
                    registered_name: name.clone(),
                    operation_name: direct_tool.operation,
                    attach_event_id: alias_attach_event_id,
                }
            }));
            if !is_kernel {
                tool_aliases.extend(
                    record
                        .manifest
                        .operations
                        .iter()
                        .filter(|operation| {
                            operations.is_empty() || operations.contains(&operation.name)
                        })
                        .map(
                            |operation| crate::agent::agent_tool_router::OperationToolAlias {
                                tool_name: verlet_operations::projection_tool_name(
                                    &name,
                                    &operation.name,
                                ),
                                registered_name: name.clone(),
                                operation_name: operation.name.clone(),
                                attach_event_id: alias_attach_event_id,
                            },
                        ),
                );
            }
            let record = if operations.is_empty() {
                crate::operations::plugins::LocalPluginCatalogRecord::whole_record(record)
            } else {
                crate::operations::plugins::LocalPluginCatalogRecord::selected_operations(
                    record, operations,
                )
            }
            .with_attachment_config(attachment_config);
            records.push(record);
        }
        let mounts = workspace
            .into_iter()
            .map(|workspace| match workspace.mode {
                verlet_agent::manifest_schema::AgentManifestWorkspaceMode::ReadOnly => {
                    crate::operations::plugins::PluginMount::pinned_host_read_only(
                        workspace.guest_path,
                        workspace.host_path,
                    )
                }
                verlet_agent::manifest_schema::AgentManifestWorkspaceMode::ReadWrite => {
                    crate::operations::plugins::PluginMount::pinned_host_read_write(
                        workspace.guest_path,
                        workspace.host_path,
                    )
                }
            })
            .collect();
        let catalog = if let Some(secret_resolver) = &self.secret_resolver {
            crate::operations::plugins::LocalPluginCatalog::load_selected_records_with_secret_resolver(
                registry_root.clone(),
                records,
                mounts,
                std::sync::Arc::clone(secret_resolver),
            )
            .await?
        } else {
            crate::operations::plugins::LocalPluginCatalog::load_selected_records(
                registry_root,
                records,
                mounts,
            )
            .await?
        };
        Ok(Some(ThreadOperationCatalog {
            registry: catalog.operation_registry(),
            tool_aliases,
            workspace_vfs: catalog.vfs(),
        }))
    }

    async fn tool_universe_search_surface(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<crate::agent::tool_universe::ToolUniverseSearchSurface>,
    > {
        let bindings = thread_manifest_tool_universes(context)?;
        if bindings.is_empty() {
            return Ok(None);
        }
        let metadata_store_path = self.metadata_store_path.as_ref().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "tool universe bindings require an app-server metadata store path".to_string(),
            )
        })?;
        let session_store_path = self.session_store_path.as_ref().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "tool universe bindings require an app-server session store path".to_string(),
            )
        })?;
        let registry =
            crate::adapters::mcp_client::SqliteMcpSourceRegistry::open_async(metadata_store_path)
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
                })?;
        let discoverer =
            std::sync::Arc::new(crate::adapters::mcp_client::McpToolUniverseDiscoverer::new(
                registry,
                self.secret_resolver.clone(),
            ));
        let mut universes = Vec::new();
        for binding in bindings {
            let caller: std::sync::Arc<dyn crate::agent::tool_universe::ToolUniverseCaller> =
                discoverer.caller_for(&binding.server_ref).await?;
            universes.push(crate::agent::tool_universe::MountedToolUniverse { binding, caller });
        }
        let event_store: std::sync::Arc<dyn verlet_history::RuntimeStore> = std::sync::Arc::new(
            verlet_history_sqlite::SqliteSessionStore::open(session_store_path)
                .await
                .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
                .with_lease_epoch(self.lease_epoch),
        );
        Ok(Some(
            crate::agent::tool_universe::ToolUniverseSearchSurface::new_with_runtime(
                universes,
                event_store,
                discoverer,
            ),
        ))
    }
}

fn bash_config_with_skill_files(
    mut config: crate::capabilities::execution::VirtualBashRuntimeConfig,
    skill_files: &[verlet_vbash::VirtualFile],
) -> crate::capabilities::execution::VirtualBashRuntimeConfig {
    for file in skill_files {
        config = config.with_readonly_skill_file(file.path.clone(), file.content.clone());
    }
    config
}

fn provider_surface_for_runtime_config(
    config: &crate::adapters::agent_loop::AgentLoopConfig,
) -> crate::agent::manifest_bind::AgentManifestProviderSurface {
    let supports_streaming = !matches!(config.api, verlet_history::ProviderApi::Other(_));
    crate::agent::manifest_bind::AgentManifestProviderSurface::single(
        config.provider.clone(),
        config.model.clone(),
    )
    .with_supports_streaming(supports_streaming)
}

pub(crate) fn apply_manifest_runtime_metadata(
    context: &verlet_runtime_contracts::ThreadContext,
    config: &mut crate::adapters::agent_loop::AgentLoopConfig,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if let Some(thinking) = thread_metadata_thinking(&context.metadata)? {
        config.thinking = Some(thinking);
    }
    if let Some(provider_id) = context
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_PROVIDER_ID_METADATA)
    {
        config.provider = provider_id.clone();
    }
    if let Some(model_id) = context
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_MODEL_ID_METADATA)
    {
        config.model = model_id.clone();
    }
    let tool_instruction = context
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA)
        .filter(|instruction| !instruction.trim().is_empty())
        .cloned();
    if let Some(instruction) = tool_instruction {
        if !config.system.iter().any(|block| block.text == instruction) {
            config
                .system
                .push(verlet_provider::SystemBlock::text(instruction));
        }
    }
    if let Some(streaming) = context
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_RUNTIME_STREAMING_METADATA)
    {
        config.stream = streaming.parse::<bool>().map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "manifest runtime streaming metadata is invalid: {err}"
            ))
        })?;
    }
    Ok(())
}

pub(crate) fn manifest_compaction_policy(
    context: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<Option<crate::kernel::compaction::CompactionPolicy>>
{
    context
        .metadata
        .get(crate::adapters::app_server::THREAD_AGENT_RUNTIME_COMPACTION_AUTO_AT_TEXT_BYTES_METADATA)
        .map(|value| {
            let bytes = value.parse::<usize>().map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "manifest runtime compaction metadata is invalid: {err}"
                ))
            })?;
            Ok(crate::kernel::compaction::CompactionPolicy::auto_at_text_bytes(bytes))
        })
        .transpose()
}

pub(crate) async fn operation_registry_capability_grants(
    registry: &verlet_operations::operation_registry::OperationRegistry,
) -> std::collections::BTreeSet<String> {
    registry
        .list()
        .await
        .into_iter()
        .flat_map(|operation| operation.capability_grants)
        .collect()
}
