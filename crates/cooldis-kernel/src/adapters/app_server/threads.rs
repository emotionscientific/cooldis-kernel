use super::connection::*;
use super::subscriptions::wait_for_initial_thread_status;
use super::*;

#[derive(Default)]
pub(super) struct AppServerState {
    pub(super) threads: HashMap<String, AppServerThreadState>,
}

#[derive(Clone)]
pub(super) struct AppServerThreadState {
    pub(super) thread_id: String,
    pub(super) session_id: String,
    pub(super) parent_thread_id: Option<String>,
    pub(super) topology: ThreadTopology,
    pub(super) cwd: PathBuf,
    pub(super) model_provider: String,
    pub(super) created_at_ms: u64,
    pub(super) updated_at_ms: u64,
    pub(super) status: ThreadStatus,
    pub(super) preview: String,
    pub(super) ephemeral: bool,
    pub(super) name: Option<String>,
    pub(super) thinking: Option<ThinkingConfig>,
    pub(super) turns: BTreeMap<String, AppServerTurnState>,
    pub(super) active_turn_id: Option<String>,
}

#[derive(Clone)]
pub(super) struct AppServerTurnState {
    pub(super) id: String,
    pub(super) items: Vec<Value>,
    pub(super) status: AppServerTurnStatus,
    pub(super) started_at_ms: u64,
    pub(super) completed_at_ms: Option<u64>,
    pub(super) error: Option<Value>,
    pub(super) assistant_item_id: String,
    pub(super) assistant_text: String,
    pub(super) assistant_started: bool,
    pub(super) assistant_completed: bool,
    pub(super) thinking_item_id: String,
    pub(super) thinking_text: String,
    pub(super) thinking_started: bool,
    pub(super) thinking_completed: bool,
    pub(super) observed_running: bool,
    pub(super) completion_scheduled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AppServerTurnStatus {
    InProgress,
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone)]
pub(super) struct AppServerThreadLifecycleSink {
    inner: Weak<CooldisAppServerInner>,
}

impl AppServerThreadLifecycleSink {
    pub(super) fn new(app: &CooldisAppServer) -> Self {
        Self {
            inner: Arc::downgrade(&app.inner),
        }
    }
}

#[async_trait::async_trait]
impl ThreadLifecycleSink for AppServerThreadLifecycleSink {
    async fn thread_started(&self, handle: RuntimeThreadHandle) -> CooldisResult<()> {
        if handle.context().parent_thread_id.is_none() {
            return Ok(());
        }
        let Some(inner) = self.inner.upgrade() else {
            return Ok(());
        };
        CooldisAppServer { inner }
            .register_runtime_thread(handle)
            .await
    }
}

impl CooldisAppServer {
    pub(super) async fn load_threads_from_metadata(&self) -> CooldisResult<()> {
        let records = self
            .inner
            .metadata_store
            .list_thread_lifecycle_for_user(&self.inner.tenant_id, &self.inner.user_id)
            .map_err(metadata_store_error)?;
        for record in records {
            if !is_loadable_lifecycle_status(record.status) {
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
            wait_for_initial_thread_status(&handle).await;
            if let Err(err) = self.rebind_loaded_manifest_thread(&handle).await {
                let _ = self
                    .inner
                    .supervisor
                    .shutdown_thread_at(&handle.context().coordinates)
                    .await;
                eprintln!(
                    "cooldis app-server skipped unavailable thread {}: agent_ref={}, stored_hash={}, error={err}",
                    record.coordinates.thread_id,
                    record
                        .metadata
                        .get(THREAD_AGENT_REF_METADATA)
                        .map(String::as_str)
                        .unwrap_or("<none>"),
                    record
                        .metadata
                        .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
                        .map(String::as_str)
                        .unwrap_or("<none>")
                );
                continue;
            }
            let thread_state = self
                .thread_state_from_lifecycle(&record, handle.status())
                .await?;
            let mut state = self.inner.state.write().await;
            state.threads.insert(thread_id, thread_state);
        }
        Ok(())
    }

    pub(super) async fn load_thread_from_metadata(
        &self,
        thread_id: &str,
        parsed: ThreadId,
    ) -> Result<RuntimeThreadHandle, JsonRpcErrorError> {
        let record = self
            .inner
            .metadata_store
            .get_thread_lifecycle(parsed)
            .map_err(metadata_store_jsonrpc_error)?
            .ok_or_else(|| thread_not_found(thread_id))?;
        if record.coordinates.tenant_id != self.inner.tenant_id
            || record.coordinates.user_id != self.inner.user_id
            || !is_loadable_lifecycle_status(record.status)
        {
            return Err(thread_not_found(thread_id));
        }
        let handle = self
            .inner
            .supervisor
            .load_thread_from_lifecycle(record.clone())
            .await
            .map_err(internal_error)?;
        wait_for_initial_thread_status(&handle).await;
        if let Err(err) = self.rebind_loaded_manifest_thread(&handle).await {
            let _ = self
                .inner
                .supervisor
                .shutdown_thread_at(&handle.context().coordinates)
                .await;
            return Err(internal_error(err));
        }
        let thread_state = self
            .thread_state_from_lifecycle(&record, handle.status())
            .await
            .map_err(internal_error)?;
        let mut state = self.inner.state.write().await;
        state
            .threads
            .insert(record.coordinates.thread_id.to_string(), thread_state);
        Ok(handle)
    }

    pub(super) async fn rebind_loaded_manifest_thread(
        &self,
        handle: &RuntimeThreadHandle,
    ) -> CooldisResult<Option<(crate::EventRecord, crate::EventRecord)>> {
        let metadata = &handle.context().metadata;
        let Some(agent_ref) = metadata.get(THREAD_AGENT_REF_METADATA) else {
            return Ok(None);
        };
        let expected_hash = metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .ok_or_else(|| {
                CooldisError::RuntimeFactory(format!(
                    "manifest thread {agent_ref:?} is missing stored manifest hash"
                ))
            })?;
        let overrides = metadata
            .get(THREAD_AGENT_RUNTIME_OVERRIDES_METADATA)
            .map(|value| {
                serde_json::from_str::<AgentManifestBindOverrides>(value).map_err(|err| {
                    CooldisError::RuntimeFactory(format!(
                        "stored manifest runtime overrides are invalid: {err}"
                    ))
                })
            })
            .transpose()?
            .unwrap_or_default();
        let registry = LocalAgentRegistry::new(self.inner.agent_registry_root.clone());
        let (record, alias) = registry.load_ref_with_alias_receipt(agent_ref)?;
        if &record.manifest_hash != expected_hash {
            return Err(CooldisError::RuntimeFactory(format!(
                "manifest thread stored hash {expected_hash} but {agent_ref:?} loaded {}",
                record.manifest_hash
            )));
        }
        let mut provider_surface = self.agent_manifest_provider_surface()?;
        if record.name == default_manifest::DEFAULT_AGENT_NAME
            && record.namespace.as_deref() == Some(default_manifest::DEFAULT_AGENT_NAMESPACE)
            && metadata
                .get(THREAD_AGENT_PROVIDER_ID_METADATA)
                .is_some_and(|provider_id| provider_id == &provider_surface.provider_id)
            && let Some(model_id) = metadata.get(THREAD_AGENT_MODEL_ID_METADATA)
        {
            provider_surface.model_ids.insert(model_id.clone());
        }
        let mcp_server_refs = self.configured_mcp_server_refs()?;
        let tool_universe_discoverer = self.tool_universe_discoverer()?;
        let model_selection = metadata
            .get(THREAD_AGENT_MODEL_PROFILE_ID_METADATA)
            .map(|profile_id| AgentManifestModelProfileSelection::profile_id(profile_id.clone()))
            .unwrap_or_default();
        let bound = bind_published_agent_record(
            &record,
            alias,
            &provider_surface,
            self.inner.capsule_bindings.registry_root.as_deref(),
            Some(self.inner.blob_registry_root.as_path()),
            Some(self.inner.skill_registry_root.as_path()),
            &mcp_server_refs,
            Some(&tool_universe_discoverer),
            &model_selection,
            &overrides,
        )
        .await?;
        record_bound_agent_receipts(handle, &bound).await.map(Some)
    }

    pub(crate) fn agent_registry_root(&self) -> &Path {
        &self.inner.agent_registry_root
    }

    pub(crate) async fn validate_daemon_route_agent_ref(
        &self,
        agent_ref: &str,
    ) -> CooldisResult<()> {
        if !agent_ref.starts_with("agent://") {
            return Err(CooldisError::RuntimeFactory(
                "daemon route agent_ref must be an agent:// ref".to_string(),
            ));
        }
        AgentRecordRef::parse(agent_ref)?;
        self.bind_daemon_route_agent(agent_ref).await?;
        Ok(())
    }

    pub(crate) async fn bind_daemon_route_agent(
        &self,
        agent_ref: &str,
    ) -> CooldisResult<KernelThreadSpawnAgentBinding> {
        let bound = self
            .bind_app_server_agent_ref(
                agent_ref,
                &AgentManifestModelProfileSelection::default(),
                &AgentManifestBindOverrides::default(),
            )
            .await?;
        kernel_thread_spawn_agent_binding(
            &bound,
            &self.inner.cwd,
            self.inner.capsule_bindings.registry_root.as_deref(),
            None,
        )
    }

    pub(super) async fn bind_app_server_agent_ref(
        &self,
        agent_ref: &str,
        model_selection: &AgentManifestModelProfileSelection,
        overrides: &AgentManifestBindOverrides,
    ) -> CooldisResult<AgentManifestBoundThread> {
        let registry = LocalAgentRegistry::new(self.inner.agent_registry_root.clone());
        let (record, alias) = registry.load_ref_with_alias_receipt(agent_ref)?;
        let provider_surface = self.agent_manifest_provider_surface()?;
        let mcp_server_refs = self.configured_mcp_server_refs()?;
        let tool_universe_discoverer = self.tool_universe_discoverer()?;
        bind_published_agent_record(
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
        )
        .await
    }

    pub(super) async fn thread_state_from_lifecycle(
        &self,
        record: &ThreadLifecycleRecord,
        status: ThreadStatus,
    ) -> CooldisResult<AppServerThreadState> {
        let (preview, turns) = self.thread_history_from_lifecycle(record).await?;
        Ok(AppServerThreadState {
            thread_id: record.coordinates.thread_id.to_string(),
            session_id: record.coordinates.session_id.clone(),
            parent_thread_id: record.parent_thread_id.map(|id| id.to_string()),
            topology: record.topology.clone(),
            cwd: thread_lifecycle_cwd(record).unwrap_or_else(|| self.inner.cwd.clone()),
            model_provider: record
                .metadata
                .get(THREAD_APP_SERVER_MODEL_PROVIDER_METADATA)
                .cloned()
                .unwrap_or_else(|| self.inner.model_provider.clone()),
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
            status,
            preview,
            ephemeral: record
                .metadata
                .get(THREAD_APP_SERVER_EPHEMERAL_METADATA)
                .is_some_and(|value| value == "true"),
            name: record
                .metadata
                .get(THREAD_APP_SERVER_NAME_METADATA)
                .filter(|value| !value.trim().is_empty())
                .cloned(),
            thinking: thread_lifecycle_thinking(record)?,
            turns,
            active_turn_id: None,
        })
    }

    /// Rebuild the app-server thread projection from durable session history.
    pub(super) async fn thread_history_from_lifecycle(
        &self,
        record: &ThreadLifecycleRecord,
    ) -> CooldisResult<(String, BTreeMap<String, AppServerTurnState>)> {
        let store = SqliteSessionStore::open(&self.inner.session_store_path)
            .map_err(|err| CooldisError::History(err.to_string()))?;
        let context = store
            .build_context(&record.coordinates)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        Ok(app_server_turns_from_session_entries(&context.entries))
    }

    pub(super) async fn persist_thread_lifecycle(
        &self,
        handle: &RuntimeThreadHandle,
    ) -> Result<(), JsonRpcErrorError> {
        self.persist_thread_lifecycle_with_metadata(handle, BTreeMap::new())
            .await
    }

    pub(super) async fn persist_thread_lifecycle_with_metadata(
        &self,
        handle: &RuntimeThreadHandle,
        metadata: BTreeMap<String, String>,
    ) -> Result<(), JsonRpcErrorError> {
        self.persist_thread_lifecycle_record_with_metadata(handle, metadata)
            .await
            .map(|_| ())
            .map_err(internal_error)
    }

    pub(super) async fn persist_thread_lifecycle_record_with_metadata(
        &self,
        handle: &RuntimeThreadHandle,
        metadata: BTreeMap<String, String>,
    ) -> CooldisResult<ThreadLifecycleRecord> {
        let mut record = handle.lifecycle_record().await;
        let handle_metadata = record.metadata.clone();
        if let Some(existing) = self
            .inner
            .metadata_store
            .get_thread_lifecycle(record.coordinates.thread_id)
            .map_err(metadata_store_error)?
        {
            record.metadata = existing.metadata;
            record.metadata.extend(handle_metadata);
        }
        record.metadata.extend(metadata);
        self.inner
            .metadata_store
            .upsert_thread_lifecycle(record.clone())
            .map_err(metadata_store_error)?;
        Ok(record)
    }

    pub(super) async fn register_runtime_thread(
        &self,
        handle: RuntimeThreadHandle,
    ) -> CooldisResult<()> {
        wait_for_initial_thread_status(&handle).await;
        let record = self
            .persist_thread_lifecycle_record_with_metadata(&handle, BTreeMap::new())
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

    pub(super) fn spawn_lifecycle_persistence_watcher(
        &self,
        thread_id: String,
        handle: RuntimeThreadHandle,
    ) {
        let app = self.clone();
        tokio::spawn(async move {
            let mut status = handle.subscribe_status();
            let _ = status.borrow_and_update();
            loop {
                if status.changed().await.is_err() {
                    break;
                }
                let status_value = *status.borrow_and_update();
                if let Err(err) = app
                    .persist_thread_lifecycle_record_with_metadata(&handle, BTreeMap::new())
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
                        thread.updated_at_ms = now_ms();
                    }
                }
                if matches!(status_value, ThreadStatus::Stopped | ThreadStatus::Failed) {
                    break;
                }
            }
        });
    }

    pub(super) async fn thread_json_by_id(
        &self,
        thread_id: &str,
        include_turns: bool,
    ) -> Result<Value, JsonRpcErrorError> {
        let state = self.inner.state.read().await;
        let thread = state
            .threads
            .get(thread_id)
            .ok_or_else(|| thread_not_found(thread_id))?;
        Ok(thread_json(thread, include_turns))
    }
}

impl AppServerTurnState {
    pub(super) fn new(id: String, input: Vec<Value>) -> Self {
        let assistant_item_id = format!("{id}:agent-message");
        let thinking_item_id = format!("{id}:agent-thinking");
        Self {
            id: id.clone(),
            items: vec![json!({
                "type": "userMessage",
                "id": format!("{id}:user-message"),
                "content": input,
            })],
            status: AppServerTurnStatus::InProgress,
            started_at_ms: now_ms(),
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

    pub(super) fn restored(id: String, started_at_ms: u64, items: Vec<Value>) -> Self {
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

pub(super) fn finalize_turn_payload(turn: &mut AppServerTurnState) -> (Value, Vec<Value>) {
    let should_complete_message = turn.assistant_started && !turn.assistant_completed;
    let should_complete_thinking = turn.thinking_started && !turn.thinking_completed;
    finalize_agent_message_item(turn);
    finalize_agent_thinking_item(turn);
    let mut items = Vec::new();
    if should_complete_message
        && let Some(item) = turn.items.iter().find(|item| {
            item.get("id").and_then(Value::as_str) == Some(turn.assistant_item_id.as_str())
        })
    {
        items.push(item.clone());
    }
    if should_complete_thinking
        && let Some(item) = turn.items.iter().find(|item| {
            item.get("id").and_then(Value::as_str) == Some(turn.thinking_item_id.as_str())
        })
    {
        items.push(item.clone());
    }
    (turn_json(turn), items)
}

pub(super) fn finalize_agent_message_item(turn: &mut AppServerTurnState) {
    if !turn.assistant_started || turn.assistant_completed {
        return;
    }
    let item_id = turn.assistant_item_id.clone();
    let final_item = agent_message_item(turn);
    for item in &mut turn.items {
        if item.get("id").and_then(Value::as_str) == Some(item_id.as_str()) {
            *item = final_item;
            break;
        }
    }
    turn.assistant_completed = true;
}

pub(super) fn finalize_agent_thinking_item(turn: &mut AppServerTurnState) {
    if !turn.thinking_started || turn.thinking_completed {
        return;
    }
    let item_id = turn.thinking_item_id.clone();
    let final_item = agent_thinking_item(turn);
    for item in &mut turn.items {
        if item.get("id").and_then(Value::as_str) == Some(item_id.as_str()) {
            *item = final_item;
            break;
        }
    }
    turn.thinking_completed = true;
}

pub(super) fn agent_message_item(turn: &AppServerTurnState) -> Value {
    agent_message_item_from_text(&turn.assistant_item_id, &turn.assistant_text)
}

pub(super) fn agent_message_item_from_text(id: &str, text: &str) -> Value {
    json!({
        "type": "agentMessage",
        "id": id,
        "text": text,
        "content": [{ "type": "text", "text": text }],
        "phase": null,
        "memoryCitation": null,
    })
}

pub(super) fn agent_thinking_item(turn: &AppServerTurnState) -> Value {
    agent_thinking_item_from_text(&turn.thinking_item_id, &turn.thinking_text)
}

pub(super) fn agent_thinking_item_from_text(id: &str, text: &str) -> Value {
    json!({
        "type": "agentThinking",
        "id": id,
        "text": text,
        "content": [{ "type": "text", "text": text }],
        "phase": null,
        "memoryCitation": null,
    })
}

pub(super) fn command_execution_item(
    id: &str,
    command: &str,
    cwd: &Path,
    status: &str,
    aggregated_output: Option<String>,
    exit_code: Option<i32>,
    duration_ms: Option<u64>,
) -> Value {
    json!({
        "type": "commandExecution",
        "id": id,
        "command": command,
        "cwd": cwd_string(cwd),
        "processId": null,
        "source": "userShell",
        "status": status,
        "commandActions": [],
        "aggregatedOutput": aggregated_output,
        "exitCode": exit_code,
        "durationMs": duration_ms,
    })
}

pub(super) fn thread_json(thread: &AppServerThreadState, include_turns: bool) -> Value {
    let turns = if include_turns {
        thread.turns.values().map(turn_json).collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    json!({
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
        "cwd": cwd_string(&thread.cwd),
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

pub(super) fn turn_json(turn: &AppServerTurnState) -> Value {
    let completed_at = turn.completed_at_ms.map(|ms| ms / 1000);
    let duration_ms = turn
        .completed_at_ms
        .map(|completed| completed.saturating_sub(turn.started_at_ms));
    json!({
        "id": turn.id,
        "items": turn.items,
        "itemsView": "full",
        "status": turn_status_string(turn.status),
        "error": turn.error,
        "startedAt": turn.started_at_ms / 1000,
        "completedAt": completed_at,
        "durationMs": duration_ms,
    })
}

pub(super) fn thread_status_json(status: ThreadStatus) -> Value {
    match status {
        ThreadStatus::Starting | ThreadStatus::Running | ThreadStatus::Cancelling => {
            json!({ "type": "active", "activeFlags": [] })
        }
        ThreadStatus::Idle => json!({ "type": "idle" }),
        ThreadStatus::Stopped => json!({ "type": "notLoaded" }),
        ThreadStatus::Failed => json!({ "type": "systemError" }),
    }
}

pub(super) fn turn_status_string(status: AppServerTurnStatus) -> &'static str {
    match status {
        AppServerTurnStatus::InProgress => "inProgress",
        AppServerTurnStatus::Completed => "completed",
        AppServerTurnStatus::Interrupted => "interrupted",
        AppServerTurnStatus::Failed => "failed",
    }
}

pub(super) fn turn_input_from_values(input: &[Value]) -> TurnInput {
    let mut content = Vec::new();
    for item in input {
        match item.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    content.push(TurnContent::text(text.to_string()));
                }
            }
            Some("localImage") => {
                if let Some(path) = item.get("path").and_then(Value::as_str) {
                    content.push(TurnContent::file_ref(PathBuf::from(path)));
                }
            }
            Some("mention") | Some("skill") => {
                if let Some(name) = item.get("name").and_then(Value::as_str) {
                    content.push(TurnContent::text(format!("@{name}")));
                }
            }
            Some("image") => {}
            _ => {}
        }
    }
    if content.is_empty() {
        content.push(TurnContent::text(""));
    }
    TurnInput::new(content)
}

pub(super) fn deserialize_optional_thinking<'de, D>(
    deserializer: D,
) -> Result<Option<ThinkingConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = <Option<Value> as serde::Deserialize>::deserialize(deserializer)?;
    value
        .map(|value| thinking_from_app_server_value(&value).map_err(serde::de::Error::custom))
        .transpose()
}

pub(super) fn thinking_from_app_server_value(value: &Value) -> Result<ThinkingConfig, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "thinking must be an object".to_string())?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "thinking.type is required".to_string())?;
    match kind {
        "effort" => {
            let effort = object
                .get("effort")
                .and_then(Value::as_str)
                .ok_or_else(|| "thinking.effort is required".to_string())?;
            let effort = match effort {
                "low" => ThinkingEffort::Low,
                "medium" => ThinkingEffort::Medium,
                "high" => ThinkingEffort::High,
                "xhigh" => ThinkingEffort::XHigh,
                "max" => ThinkingEffort::Max,
                other => return Err(format!("unsupported thinking effort {other:?}")),
            };
            Ok(ThinkingConfig::Effort { effort })
        }
        "budget" => {
            let budget_tokens = object
                .get("budgetTokens")
                .and_then(Value::as_u64)
                .filter(|value| *value <= u64::from(u32::MAX))
                .ok_or_else(|| "thinking.budgetTokens must be a u32".to_string())?
                as u32;
            Ok(ThinkingConfig::Budget { budget_tokens })
        }
        "disabled" => Ok(ThinkingConfig::Disabled),
        other => Err(format!("unsupported thinking type {other:?}")),
    }
}

pub(super) fn app_server_thinking_json(thinking: &Option<ThinkingConfig>) -> Value {
    thinking
        .as_ref()
        .map(app_server_thinking_value)
        .unwrap_or(Value::Null)
}

pub(super) fn app_server_thinking_value(thinking: &ThinkingConfig) -> Value {
    match thinking {
        ThinkingConfig::Effort { effort } => json!({
            "type": "effort",
            "effort": match effort {
                ThinkingEffort::Low => "low",
                ThinkingEffort::Medium => "medium",
                ThinkingEffort::High => "high",
                ThinkingEffort::XHigh => "xhigh",
                ThinkingEffort::Max => "max",
                ThinkingEffort::Other(value) => value.as_str(),
            },
        }),
        ThinkingConfig::Budget { budget_tokens } => json!({
            "type": "budget",
            "budgetTokens": budget_tokens,
        }),
        ThinkingConfig::Disabled => json!({ "type": "disabled" }),
    }
}

pub(super) fn encode_app_server_thinking(
    thinking: &ThinkingConfig,
) -> Result<String, JsonRpcErrorError> {
    serde_json::to_string(&app_server_thinking_value(thinking)).map_err(|err| {
        jsonrpc_error(
            -32602,
            format!("failed to encode app-server thinking config: {err}"),
        )
    })
}

pub(super) fn user_input_preview(input: &[Value]) -> String {
    input
        .iter()
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn app_server_turns_from_session_entries(
    entries: &[SessionEntry],
) -> (String, BTreeMap<String, AppServerTurnState>) {
    let mut preview = String::new();
    let mut turns = BTreeMap::new();
    let mut current_turn_id = None;

    for entry in entries {
        let (SessionEntryKind::Message { message }
        | SessionEntryKind::CustomContextMessage { message }) = &entry.kind
        else {
            continue;
        };
        match message {
            CanonicalMessage::User { content, .. } => {
                let text_content = canonical_text_content_items(content);
                if preview.is_empty() {
                    preview = text_content_preview(&text_content);
                }
                let turn_id = format!("turn-{}", entry.entry_id);
                let item = json!({
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
            CanonicalMessage::Assistant { content, .. } => {
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
            CanonicalMessage::ToolResult { .. } => {}
        }
    }

    (preview, turns)
}

struct RestoredAssistantItems {
    text: String,
    thinking: String,
    items: Vec<Value>,
}

fn restored_assistant_items_from_canonical_content(
    content: &[CanonicalContent],
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
            CanonicalContent::Text { text: value, .. } => {
                if !value.is_empty() && !text_seen {
                    order.push(ItemKind::Message);
                    text_seen = true;
                }
                text.push_str(value);
            }
            CanonicalContent::Thinking { text: value, .. } => {
                if !value.is_empty() && !thinking_seen {
                    order.push(ItemKind::Thinking);
                    thinking_seen = true;
                }
                thinking.push_str(value);
            }
            CanonicalContent::Image { .. } | CanonicalContent::ToolCall { .. } => {}
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

pub(super) fn canonical_text_content_items(content: &[CanonicalContent]) -> Vec<Value> {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(json!({ "type": "text", "text": text })),
            CanonicalContent::Image { .. } | CanonicalContent::ToolCall { .. } => None,
            CanonicalContent::Thinking { .. } => None,
        })
        .collect()
}

pub(super) fn text_content_preview(content: &[Value]) -> String {
    content
        .iter()
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn entry_created_at_ms(entry: &SessionEntry) -> u64 {
    entry.created_at_ms.max(0) as u64
}

pub(super) fn resolve_cwd(default_cwd: &Path, cwd: Option<&str>) -> PathBuf {
    match cwd {
        Some(cwd) if !cwd.trim().is_empty() => {
            let path = PathBuf::from(cwd);
            if path.is_absolute() {
                path
            } else {
                default_cwd.join(path)
            }
        }
        _ => default_cwd.to_path_buf(),
    }
}

pub(super) fn normalize_registry_roots(config: &mut CooldisAppServerConfig) {
    let blob_registry_root_was_default =
        config.blob_registry_root == Path::new(DEFAULT_BLOB_REGISTRY_ROOT);
    config.agent_registry_root = resolve_path_against_cwd(&config.cwd, &config.agent_registry_root);
    config.blob_registry_root = if blob_registry_root_was_default {
        default_blob_registry_root_for_agent_registry_root(&config.agent_registry_root)
    } else {
        resolve_path_against_cwd(&config.cwd, &config.blob_registry_root)
    };
    config.skill_registry_root = resolve_path_against_cwd(&config.cwd, &config.skill_registry_root);
    if let Some(registry_root) = &config.capsule_bindings.registry_root {
        config.capsule_bindings.registry_root =
            Some(resolve_path_against_cwd(&config.cwd, registry_root));
    }
}

pub(super) fn resolve_path_against_cwd(cwd: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

pub(super) fn thread_start_topology(
    params: &ThreadStartParams,
) -> Result<ThreadTopology, JsonRpcErrorError> {
    match (&params.topology, &params.parent_thread_id) {
        (Some(_), Some(_)) => Err(jsonrpc_error(
            -32602,
            "thread/start accepts either topology or parentThreadId, not both",
        )),
        (Some(topology), None) => Ok(topology.clone()),
        (None, Some(parent_thread_id)) => {
            let parent_thread_id = ThreadId::parse_str(parent_thread_id)
                .map_err(|err| jsonrpc_error(-32602, format!("invalid parentThreadId: {err}")))?;
            Ok(ThreadTopology::spawned_from(parent_thread_id))
        }
        (None, None) => Ok(ThreadTopology::root()),
    }
}

pub(super) fn thread_start_metadata(
    params: &ThreadStartParams,
    cwd: &Path,
    model_provider: &str,
    ephemeral: bool,
) -> Result<BTreeMap<String, String>, JsonRpcErrorError> {
    let mut metadata = app_server_thread_metadata(cwd, model_provider, ephemeral);
    if let Some(thinking) = &params.thinking {
        insert_app_server_thinking_metadata(&mut metadata, Some(thinking))?;
    }
    Ok(metadata)
}

pub(super) fn insert_app_server_thinking_metadata(
    metadata: &mut BTreeMap<String, String>,
    thinking: Option<&ThinkingConfig>,
) -> Result<(), JsonRpcErrorError> {
    if let Some(thinking) = thinking {
        metadata.insert(
            THREAD_APP_SERVER_THINKING_METADATA.to_string(),
            encode_app_server_thinking(thinking)?,
        );
    }
    Ok(())
}

pub(super) fn append_bound_agent_metadata(
    metadata: &mut BTreeMap<String, String>,
    bound: &AgentManifestBoundThread,
    overrides: Option<&AgentManifestBindOverrides>,
    operation_registry_root: Option<&Path>,
) -> Result<(), JsonRpcErrorError> {
    metadata.insert(
        THREAD_AGENT_REF_METADATA.to_string(),
        bound.bind_receipt.ref_uri.clone(),
    );
    metadata.insert(
        THREAD_AGENT_MANIFEST_HASH_METADATA.to_string(),
        bound.bind_receipt.manifest_hash.clone(),
    );
    metadata.insert(
        THREAD_AGENT_SOURCE_HASH_METADATA.to_string(),
        bound.compile_receipt.source_hash.clone(),
    );
    metadata.insert(
        THREAD_AGENT_MODEL_PROFILE_ID_METADATA.to_string(),
        bound.bind_receipt.model_profile_id.clone(),
    );
    metadata.insert(
        THREAD_AGENT_PROVIDER_ID_METADATA.to_string(),
        bound.bind_receipt.provider_id.clone(),
    );
    metadata.insert(
        THREAD_AGENT_MODEL_ID_METADATA.to_string(),
        bound.bind_receipt.model_id.clone(),
    );
    if let Some(instruction) = manifest_tool_use_system_instruction(bound) {
        metadata.insert(
            THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA.to_string(),
            instruction,
        );
    }
    metadata.insert(
        THREAD_AGENT_RUNTIME_STREAMING_METADATA.to_string(),
        bound.bind_receipt.effective_runtime.streaming.to_string(),
    );
    if let Some(auto_at_text_bytes) = bound
        .bind_receipt
        .effective_runtime
        .compaction
        .auto_at_text_bytes
    {
        metadata.insert(
            THREAD_AGENT_RUNTIME_COMPACTION_AUTO_AT_TEXT_BYTES_METADATA.to_string(),
            auto_at_text_bytes.to_string(),
        );
    }
    if !bound.operation_bindings.is_empty() {
        let encoded = serde_json::to_string(&bound.operation_bindings).map_err(|err| {
            jsonrpc_error(
                -32602,
                format!("failed to encode manifest operation bindings: {err}"),
            )
        })?;
        metadata.insert(
            THREAD_AGENT_OPERATION_BINDINGS_METADATA.to_string(),
            encoded,
        );
    }
    if !bound.skill_packages.is_empty() {
        let encoded = serde_json::to_string(&bound.skill_packages).map_err(|err| {
            jsonrpc_error(
                -32602,
                format!("failed to encode manifest skill package bindings: {err}"),
            )
        })?;
        metadata.insert(THREAD_AGENT_SKILL_PACKAGES_METADATA.to_string(), encoded);
    }
    if !bound.skill_context_segments.is_empty() {
        let encoded = serde_json::to_string(&bound.skill_context_segments).map_err(|err| {
            jsonrpc_error(
                -32602,
                format!("failed to encode manifest skill context segments: {err}"),
            )
        })?;
        metadata.insert(
            THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA.to_string(),
            encoded,
        );
    }
    if !bound.static_context_segments.is_empty() {
        let encoded = serde_json::to_string(&bound.static_context_segments).map_err(|err| {
            jsonrpc_error(
                -32602,
                format!("failed to encode manifest static context segments: {err}"),
            )
        })?;
        metadata.insert(
            crate::THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA.to_string(),
            encoded,
        );
    }
    if !bound.tool_universes.is_empty() {
        let encoded = serde_json::to_string(&bound.tool_universes).map_err(|err| {
            jsonrpc_error(
                -32602,
                format!("failed to encode manifest tool universes: {err}"),
            )
        })?;
        metadata.insert(THREAD_AGENT_TOOL_UNIVERSES_METADATA.to_string(), encoded);
    }
    if !bound.coupling_set.couplings.is_empty() {
        let encoded = serde_json::to_string(&bound.coupling_set).map_err(|err| {
            jsonrpc_error(
                -32602,
                format!("failed to encode manifest bound coupling set: {err}"),
            )
        })?;
        metadata.insert(THREAD_BOUND_COUPLING_SET_METADATA.to_string(), encoded);
        if let Some(root) = operation_registry_root {
            metadata.insert(
                THREAD_OPERATION_REGISTRY_ROOT_METADATA.to_string(),
                root.display().to_string(),
            );
        }
    }
    if let Some(overrides) = overrides {
        let encoded = serde_json::to_string(overrides).map_err(|err| {
            jsonrpc_error(
                -32602,
                format!("failed to encode manifest runtime overrides: {err}"),
            )
        })?;
        metadata.insert(THREAD_AGENT_RUNTIME_OVERRIDES_METADATA.to_string(), encoded);
    }
    Ok(())
}

fn manifest_tool_use_system_instruction(bound: &AgentManifestBoundThread) -> Option<String> {
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

fn legacy_manifest_tool_use_system_instruction(context: &ThreadContext) -> Option<String> {
    let has_tool_metadata = context
        .metadata
        .get(THREAD_AGENT_OPERATION_BINDINGS_METADATA)
        .is_some_and(|value| !value.trim().is_empty())
        || context
            .metadata
            .get(THREAD_AGENT_TOOL_UNIVERSES_METADATA)
            .is_some_and(|value| !value.trim().is_empty());
    if !has_tool_metadata {
        return None;
    }
    let agent_ref = context.metadata.get(THREAD_AGENT_REF_METADATA)?;
    Some(manifest_tool_use_instruction_text(agent_ref, None))
}

fn manifest_tool_use_instruction_text(agent_ref: &str, tool_list: Option<&str>) -> String {
    let tool_sentence = tool_list
        .map(|tools| format!("You have these Cooldis tools available: {tools}. "))
        .unwrap_or_else(|| {
            "You have Cooldis tools available for this manifest-backed thread. ".to_string()
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

pub(super) async fn record_bound_agent_receipts(
    handle: &RuntimeThreadHandle,
    bound: &AgentManifestBoundThread,
) -> CooldisResult<(crate::EventRecord, crate::EventRecord)> {
    let compile_payload = serde_json::to_value(&bound.compile_receipt).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to encode manifest compile receipt: {err}"))
    })?;
    let bind_payload = serde_json::to_value(&bound.bind_receipt).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to encode manifest bind receipt: {err}"))
    })?;
    let manifest_events = handle
        .record_manifest_receipts(compile_payload, bind_payload)
        .await?;
    let discovery_payloads = bound
        .tool_universes
        .iter()
        .map(|binding| {
            serde_json::to_value(ToolUniverseDiscoveryReceipt::from_discovery(
                &binding.discovery,
            ))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to encode tool universe discovery receipt: {err}"
            ))
        })?;
    handle
        .record_tool_universe_discovery_receipts(discovery_payloads)
        .await?;
    Ok(manifest_events)
}

fn kernel_thread_spawn_agent_binding(
    bound: &AgentManifestBoundThread,
    cwd_root: &Path,
    operation_registry_root: Option<&Path>,
    overrides: Option<&AgentManifestBindOverrides>,
) -> CooldisResult<KernelThreadSpawnAgentBinding> {
    let cwd = resolve_cwd(
        cwd_root,
        Some(bound.bind_receipt.effective_runtime.default_cwd.as_str()),
    );
    let mut metadata = app_server_thread_metadata(&cwd, &bound.bind_receipt.provider_id, false);
    append_bound_agent_metadata(&mut metadata, bound, overrides, operation_registry_root)
        .map_err(|err| CooldisError::RuntimeFactory(err.message))?;
    let compile_receipt = serde_json::to_value(&bound.compile_receipt).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to encode manifest compile receipt: {err}"))
    })?;
    let bind_receipt = serde_json::to_value(&bound.bind_receipt).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to encode manifest bind receipt: {err}"))
    })?;
    Ok(KernelThreadSpawnAgentBinding {
        metadata,
        compile_receipt,
        bind_receipt,
    })
}

pub(super) fn app_server_thread_metadata(
    cwd: &Path,
    model_provider: &str,
    ephemeral: bool,
) -> BTreeMap<String, String> {
    app_server_thread_metadata_with_name(cwd, model_provider, ephemeral, None)
}

pub(super) fn app_server_thread_metadata_with_name(
    cwd: &Path,
    model_provider: &str,
    ephemeral: bool,
    name: Option<&str>,
) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    metadata.insert(THREAD_APP_SERVER_CWD_METADATA.to_string(), cwd_string(cwd));
    metadata.insert(
        THREAD_APP_SERVER_MODEL_PROVIDER_METADATA.to_string(),
        model_provider.to_string(),
    );
    metadata.insert(
        THREAD_APP_SERVER_EPHEMERAL_METADATA.to_string(),
        ephemeral.to_string(),
    );
    if let Some(name) = name.filter(|name| !name.trim().is_empty()) {
        metadata.insert(
            THREAD_APP_SERVER_NAME_METADATA.to_string(),
            name.to_string(),
        );
    }
    metadata
}

pub(super) fn thread_lifecycle_cwd(record: &ThreadLifecycleRecord) -> Option<PathBuf> {
    record
        .metadata
        .get(THREAD_APP_SERVER_CWD_METADATA)
        .filter(|cwd| !cwd.trim().is_empty())
        .map(PathBuf::from)
}

pub(super) fn thread_lifecycle_thinking(
    record: &ThreadLifecycleRecord,
) -> CooldisResult<Option<ThinkingConfig>> {
    thread_metadata_thinking(&record.metadata)
}

pub(super) fn thread_metadata_thinking(
    metadata: &BTreeMap<String, String>,
) -> CooldisResult<Option<ThinkingConfig>> {
    metadata
        .get(THREAD_APP_SERVER_THINKING_METADATA)
        .map(|raw| {
            let value = serde_json::from_str::<Value>(raw).map_err(|err| {
                CooldisError::RuntimeFactory(format!("thread thinking metadata is invalid: {err}"))
            })?;
            thinking_from_app_server_value(&value).map_err(|err| {
                CooldisError::RuntimeFactory(format!("thread thinking metadata is invalid: {err}"))
            })
        })
        .transpose()
}

pub(super) fn is_loadable_lifecycle_status(status: ThreadLifecycleStatus) -> bool {
    !matches!(
        status,
        ThreadLifecycleStatus::Stopped | ThreadLifecycleStatus::Failed
    )
}

pub(super) fn thread_manifest_operation_bindings(
    context: &ThreadContext,
) -> CooldisResult<Vec<AgentManifestOperationBinding>> {
    let Some(raw) = context
        .metadata
        .get(THREAD_AGENT_OPERATION_BINDINGS_METADATA)
    else {
        return Ok(Vec::new());
    };
    serde_json::from_str::<Vec<AgentManifestOperationBinding>>(raw).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "thread manifest operation bindings are invalid: {err}"
        ))
    })
}

pub(super) fn thread_manifest_tool_universes(
    context: &ThreadContext,
) -> CooldisResult<Vec<ToolUniverseBinding>> {
    let Some(raw) = context.metadata.get(THREAD_AGENT_TOOL_UNIVERSES_METADATA) else {
        return Ok(Vec::new());
    };
    let bindings = serde_json::from_str::<Vec<ToolUniverseBinding>>(raw).map_err(|err| {
        CooldisError::RuntimeFactory(format!("thread manifest tool universes are invalid: {err}"))
    })?;
    for binding in &bindings {
        binding.validate()?;
    }
    Ok(bindings)
}

pub(super) fn thread_manifest_skill_packages(
    context: &ThreadContext,
) -> CooldisResult<Vec<AgentManifestSkillPackageBinding>> {
    let Some(raw) = context.metadata.get(THREAD_AGENT_SKILL_PACKAGES_METADATA) else {
        return Ok(Vec::new());
    };
    let bindings =
        serde_json::from_str::<Vec<AgentManifestSkillPackageBinding>>(raw).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "thread manifest skill package bindings are invalid: {err}"
            ))
        })?;
    Ok(bindings)
}

pub(super) struct CapsuleBindingRuntimeFactory {
    pub(super) config: CanonicalProviderRuntimeConfig,
    pub(super) client: Arc<dyn ProviderClient>,
    pub(super) capsule_bindings: CapsuleBindingsConfig,
    pub(super) secret_resolver: Option<Arc<dyn SecretResolver>>,
    pub(super) metadata_store_path: Option<PathBuf>,
    pub(super) secret_store_path: Option<PathBuf>,
    pub(super) session_store_path: Option<PathBuf>,
    pub(super) agent_registry_root: Option<PathBuf>,
    pub(super) blob_registry_root: Option<PathBuf>,
    pub(super) skill_registry_root: Option<PathBuf>,
    pub(super) cwd: Option<PathBuf>,
}

struct ThreadOperationCatalog {
    registry: Arc<crate::OperationRegistry>,
    tool_aliases: Vec<OperationToolAlias>,
    /// The per-thread workspace VFS installed into catalog-loaded operations and
    /// virtual bash so filesystem surfaces do not drift into separate trees.
    workspace_vfs: Arc<crate::CooldisVfs>,
}

#[async_trait::async_trait]
impl AgentRuntimeFactory for CapsuleBindingRuntimeFactory {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        let mut config = self.config.clone();
        apply_manifest_runtime_metadata(context, &mut config)?;
        let mut factory = CanonicalProviderRuntimeFactory::new(config, Arc::clone(&self.client));
        if let Some(policy) = manifest_compaction_policy(context)? {
            factory = factory.with_compaction_policy(policy);
        }
        if let Some(resolver) = self.thread_spawn_agent_resolver() {
            factory = factory.with_thread_spawn_agent_resolver(Arc::new(resolver));
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
                AgentToolRouter::new(Arc::clone(&registry))
                    .with_tool_aliases(tool_aliases)
                    .with_capability_grants(capability_grants.clone()),
            );
            factory = factory.with_bash_tool(bash_config_with_skill_files(
                VirtualBashRuntimeConfig::default()
                    .with_operation_registry(registry)
                    .with_workspace_vfs(workspace_vfs)
                    .with_capability_grants(capability_grants),
                &skill_files,
            ));
        } else if !skill_files.is_empty() {
            factory = factory.with_bash_tool(bash_config_with_skill_files(
                VirtualBashRuntimeConfig::default(),
                &skill_files,
            ));
        }
        if let Some(tool_universe_surface) = self.tool_universe_search_surface(context).await? {
            let router = tool_router
                .take()
                .unwrap_or_else(|| AgentToolRouter::new(Arc::new(OperationRegistry::new())))
                .with_kernel_tool_provider(Arc::new(tool_universe_surface));
            tool_router = Some(router);
        }
        if let Some(tool_router) = tool_router {
            factory = factory.with_tool_router(Arc::new(tool_router));
        }
        factory.build(context).await
    }
}

#[derive(Clone)]
struct AppServerThreadSpawnAgentResolver {
    agent_registry_root: PathBuf,
    operation_registry_root: Option<PathBuf>,
    blob_registry_root: Option<PathBuf>,
    skill_registry_root: Option<PathBuf>,
    metadata_store_path: Option<PathBuf>,
    secret_store_path: Option<PathBuf>,
    cwd: PathBuf,
    provider_surface: AgentManifestProviderSurface,
}

#[async_trait::async_trait]
impl KernelThreadSpawnAgentResolver for AppServerThreadSpawnAgentResolver {
    async fn resolve_agent_ref(
        &self,
        _caller: &ThreadContext,
        agent_ref: &str,
    ) -> CooldisResult<KernelThreadSpawnAgentBinding> {
        let registry = LocalAgentRegistry::new(self.agent_registry_root.clone());
        let (record, alias) = registry.load_ref_with_alias_receipt(agent_ref)?;
        let mcp_server_refs = self.configured_mcp_server_refs()?;
        let tool_universe_discoverer = self.tool_universe_discoverer()?;
        let bound = bind_published_agent_record(
            &record,
            alias,
            &self.provider_surface,
            self.operation_registry_root.as_deref(),
            self.blob_registry_root.as_deref(),
            self.skill_registry_root.as_deref(),
            &mcp_server_refs,
            tool_universe_discoverer
                .as_ref()
                .map(|discoverer| discoverer as &dyn crate::ToolUniverseDiscoverer),
            &AgentManifestModelProfileSelection::default(),
            &AgentManifestBindOverrides::default(),
        )
        .await?;
        kernel_thread_spawn_agent_binding(
            &bound,
            &self.cwd,
            self.operation_registry_root.as_deref(),
            None,
        )
    }
}

impl AppServerThreadSpawnAgentResolver {
    fn configured_mcp_server_refs(&self) -> CooldisResult<BTreeSet<String>> {
        let Some(metadata_store_path) = &self.metadata_store_path else {
            return Ok(BTreeSet::new());
        };
        let registry = SqliteMcpSourceRegistry::open(metadata_store_path)
            .map_err(|err| CooldisError::RuntimeFactory(err.to_string()))?;
        Ok(registry
            .list_sources()
            .map_err(|err| CooldisError::RuntimeFactory(err.to_string()))?
            .into_iter()
            .map(|source| format!("mcp://{}", source.name))
            .collect())
    }

    fn tool_universe_discoverer(&self) -> CooldisResult<Option<McpToolUniverseDiscoverer>> {
        let Some(metadata_store_path) = &self.metadata_store_path else {
            return Ok(None);
        };
        let registry = SqliteMcpSourceRegistry::open(metadata_store_path)
            .map_err(|err| CooldisError::RuntimeFactory(err.to_string()))?;
        let secret_store_path = self
            .secret_store_path
            .as_ref()
            .unwrap_or(metadata_store_path);
        let secret_store =
            SqliteSecretStore::open(secret_store_path).map_err(secret_store_error)?;
        Ok(Some(McpToolUniverseDiscoverer::new(
            registry,
            Some(Arc::new(secret_store)),
        )))
    }
}

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
        })
    }

    async fn skill_mount_files_for_thread(
        &self,
        context: &ThreadContext,
    ) -> CooldisResult<Vec<VirtualFile>> {
        let bindings = thread_manifest_skill_packages(context)?;
        if bindings.is_empty() {
            return Ok(Vec::new());
        }
        let registry_root = self.skill_registry_root.as_ref().ok_or_else(|| {
            CooldisError::RuntimeFactory(
                "skill package bindings require an app-server skill registry root".to_string(),
            )
        })?;
        let registry = LocalSkillRegistry::new(registry_root);
        let mut files = Vec::new();
        let mut names = BTreeSet::new();
        for binding in bindings {
            let record = registry
                .load_version_record(&binding.package_name, &binding.artifact_hash)
                .map_err(|err| {
                    CooldisError::RuntimeFactory(format!(
                        "manifest skill package binding {:?}@sha256:{} was not found: {err}",
                        binding.package_name, binding.artifact_hash
                    ))
                })?;
            if record.ref_uri() != binding.ref_uri {
                return Err(CooldisError::RuntimeFactory(format!(
                    "manifest skill package binding {:?} ref drift: receipt {}, registry {}",
                    binding.resource_name,
                    binding.ref_uri,
                    record.ref_uri()
                )));
            }
            for skill in record.package.skills {
                if !names.insert(skill.name.clone()) {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "manifest skill packages contain duplicate /skills/{}.md",
                        skill.name
                    )));
                }
                files.push(VirtualFile::new(
                    PathBuf::from(format!("{}.md", skill.name)),
                    skill.body.into_bytes(),
                ));
            }
        }
        Ok(files)
    }

    async fn operation_catalog_for_thread(
        &self,
        context: &ThreadContext,
    ) -> CooldisResult<Option<ThreadOperationCatalog>> {
        let manifest_operation_bindings = thread_manifest_operation_bindings(context)?;
        if manifest_operation_bindings.is_empty() {
            return Ok(None);
        }
        // lexicon-allow: capsule - existing app-server config field
        let Some(registry_root) = &self.capsule_bindings.registry_root else {
            // lexicon-allow: capsule - existing app-server config error text
            return Err(CooldisError::RuntimeFactory(
                "capsule bindings require a registry root".to_string(), // lexicon-allow: capsule - existing app-server config error text
            ));
        };
        let registry = LocalOperationRegistry::new(registry_root);
        let mut records = Vec::new();
        let mut tool_aliases = Vec::new();
        for binding in manifest_operation_bindings {
            let AgentManifestOperationBinding {
                name,
                artifact_hash,
                grants,
                operations,
                direct_tools,
            } = binding;
            tool_aliases.extend(
                direct_tools
                    .into_iter()
                    .map(|direct_tool| OperationToolAlias {
                        tool_name: direct_tool.tool_name,
                        registered_name: name.clone(),
                        operation_name: direct_tool.operation,
                    }),
            );
            let mut record = registry
                .load_version_record(&name, &artifact_hash)
                .map_err(|err| {
                    CooldisError::RuntimeFactory(format!(
                        "manifest operation binding {:?}@sha256:{} was not found: {err}",
                        name, artifact_hash
                    ))
                })?;
            apply_manifest_operation_grants(&mut record, grants);
            let record = if operations.is_empty() {
                LocalPluginCatalogRecord::whole_record(record)
            } else {
                LocalPluginCatalogRecord::selected_operations(record, operations)
            };
            records.push(record);
        }
        let catalog = if let Some(secret_resolver) = &self.secret_resolver {
            LocalPluginCatalog::load_selected_records_with_secret_resolver(
                registry_root.clone(),
                records,
                Vec::new(),
                Arc::clone(secret_resolver),
            )
            .await?
        } else {
            LocalPluginCatalog::load_selected_records(registry_root.clone(), records, Vec::new())
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
        context: &ThreadContext,
    ) -> CooldisResult<Option<ToolUniverseSearchSurface>> {
        let bindings = thread_manifest_tool_universes(context)?;
        if bindings.is_empty() {
            return Ok(None);
        }
        let metadata_store_path = self.metadata_store_path.as_ref().ok_or_else(|| {
            CooldisError::RuntimeFactory(
                "tool universe bindings require an app-server metadata store path".to_string(),
            )
        })?;
        let session_store_path = self.session_store_path.as_ref().ok_or_else(|| {
            CooldisError::RuntimeFactory(
                "tool universe bindings require an app-server session store path".to_string(),
            )
        })?;
        let registry = SqliteMcpSourceRegistry::open(metadata_store_path)
            .map_err(|err| CooldisError::RuntimeFactory(err.to_string()))?;
        let discoverer = Arc::new(McpToolUniverseDiscoverer::new(
            registry,
            self.secret_resolver.clone(),
        ));
        let mut universes = Vec::new();
        for binding in bindings {
            let caller: Arc<dyn ToolUniverseCaller> =
                discoverer.caller_for(&binding.server_ref).await?;
            universes.push(MountedToolUniverse { binding, caller });
        }
        let event_store: Arc<dyn RuntimeStore> = Arc::new(
            SqliteSessionStore::open(session_store_path)
                .map_err(|err| CooldisError::History(err.to_string()))?,
        );
        Ok(Some(ToolUniverseSearchSurface::new_with_runtime(
            universes,
            event_store,
            discoverer,
        )))
    }
}

fn bash_config_with_skill_files(
    mut config: VirtualBashRuntimeConfig,
    skill_files: &[VirtualFile],
) -> VirtualBashRuntimeConfig {
    for file in skill_files {
        config = config.with_readonly_skill_file(file.path.clone(), file.content.clone());
    }
    config
}

fn provider_surface_for_runtime_config(
    config: &CanonicalProviderRuntimeConfig,
) -> AgentManifestProviderSurface {
    let supports_streaming = !matches!(config.api, ProviderApi::Other(_));
    AgentManifestProviderSurface::single(config.provider.clone(), config.model.clone())
        .with_supports_streaming(supports_streaming)
}

pub(super) fn apply_manifest_runtime_metadata(
    context: &ThreadContext,
    config: &mut CanonicalProviderRuntimeConfig,
) -> CooldisResult<()> {
    if let Some(thinking) = thread_metadata_thinking(&context.metadata)? {
        config.thinking = Some(thinking);
    }
    if let Some(provider_id) = context.metadata.get(THREAD_AGENT_PROVIDER_ID_METADATA) {
        config.provider = provider_id.clone();
    }
    if let Some(model_id) = context.metadata.get(THREAD_AGENT_MODEL_ID_METADATA) {
        config.model = model_id.clone();
    }
    let tool_instruction = context
        .metadata
        .get(THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA)
        .filter(|instruction| !instruction.trim().is_empty())
        .cloned()
        .or_else(|| legacy_manifest_tool_use_system_instruction(context));
    if let Some(instruction) = tool_instruction {
        if !config.system.iter().any(|block| block.text == instruction) {
            config.system.push(SystemBlock::text(instruction));
        }
    }
    if let Some(streaming) = context
        .metadata
        .get(THREAD_AGENT_RUNTIME_STREAMING_METADATA)
    {
        config.stream = streaming.parse::<bool>().map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "manifest runtime streaming metadata is invalid: {err}"
            ))
        })?;
    }
    Ok(())
}

pub(super) fn manifest_compaction_policy(
    context: &ThreadContext,
) -> CooldisResult<Option<crate::CompactionPolicy>> {
    context
        .metadata
        .get(THREAD_AGENT_RUNTIME_COMPACTION_AUTO_AT_TEXT_BYTES_METADATA)
        .map(|value| {
            let bytes = value.parse::<usize>().map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "manifest runtime compaction metadata is invalid: {err}"
                ))
            })?;
            Ok(crate::CompactionPolicy::auto_at_text_bytes(bytes))
        })
        .transpose()
}

pub(super) fn apply_manifest_operation_grants(
    record: &mut crate::PublishedOperationRecord,
    grants: impl IntoIterator<Item = String>,
) {
    record.capability_grants.extend(grants);
}

pub(super) async fn operation_registry_capability_grants(
    registry: &crate::OperationRegistry,
) -> BTreeSet<String> {
    registry
        .list()
        .await
        .into_iter()
        .flat_map(|operation| operation.capability_grants)
        .collect()
}
