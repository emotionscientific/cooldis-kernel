#[derive(Clone)]
pub struct VerletSupervisor {
    inner: std::sync::Arc<SupervisorInner>,
}

struct SupervisorInner {
    tenants: tokio::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<TenantRuntime>>>,
}

struct TenantRuntime {
    tenant_id: String,
    context: TenantRuntimeContext,
    host: crate::kernel::runtime_host::RuntimeHost,
}

pub struct TenantRegistration {
    pub context: TenantRuntimeContext,
    pub runtime_factory:
        std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory>,
}

#[derive(Clone)]
pub struct TenantRuntimeContext {
    tenant_id: String,
    config: TenantRuntimeConfig,
    session_store: Option<std::sync::Arc<dyn verlet_history::RuntimeStore>>,
    execution_policy: crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy,
}

impl TenantRuntimeContext {
    pub fn local(
        tenant_id: impl Into<String>,
        runtime_home: impl Into<std::path::PathBuf>,
        state_home: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            config: TenantRuntimeConfig::local(runtime_home, state_home),
            session_store: None,
            execution_policy:
                crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
        }
    }

    pub fn with_session_store(
        mut self,
        session_store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
    ) -> Self {
        self.session_store = Some(session_store);
        self
    }

    pub fn with_execution_policy(
        mut self,
        execution_policy: crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy,
    ) -> Self {
        self.execution_policy = execution_policy;
        self
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn config(&self) -> &TenantRuntimeConfig {
        &self.config
    }

    pub fn execution_policy(
        &self,
    ) -> &crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy {
        &self.execution_policy
    }

    pub fn runtime_home(&self) -> &std::path::Path {
        &self.config.runtime_home
    }

    pub fn state_home(&self) -> &std::path::Path {
        &self.config.state_home
    }

    pub fn codex_home(&self) -> std::path::PathBuf {
        self.config.runtime_home.join("codex-home")
    }

    pub fn session_history_path(&self) -> std::path::PathBuf {
        self.config.state_home.join("session_history.sqlite3")
    }

    pub fn descriptor(&self) -> TenantRuntimeContextDescriptor {
        TenantRuntimeContextDescriptor {
            tenant_id: self.tenant_id.clone(),
            runtime_home: self.config.runtime_home.clone(),
            state_home: self.config.state_home.clone(),
            codex_home: self.codex_home(),
            session_history_path: self.session_history_path(),
            execution_policy: self.execution_policy.clone(),
        }
    }

    fn take_session_store(
        self,
    ) -> (
        TenantRuntimeContext,
        Option<std::sync::Arc<dyn verlet_history::RuntimeStore>>,
    ) {
        let session_store = self.session_store.clone();
        (self, session_store)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TenantRuntimeContextDescriptor {
    pub tenant_id: String,
    pub runtime_home: std::path::PathBuf,
    pub state_home: std::path::PathBuf,
    pub codex_home: std::path::PathBuf,
    pub session_history_path: std::path::PathBuf,
    pub execution_policy: crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TenantRuntimeConfig {
    pub runtime_home: std::path::PathBuf,
    pub state_home: std::path::PathBuf,
}

impl TenantRuntimeConfig {
    pub fn local(
        runtime_home: impl Into<std::path::PathBuf>,
        state_home: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            runtime_home: runtime_home.into(),
            state_home: state_home.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadStartRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    #[serde(default)]
    pub topology: verlet_runtime_contracts::ThreadTopology,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SupervisorSnapshot {
    pub tenants: Vec<TenantSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SupervisorLifecycleSnapshot {
    pub tenants: Vec<TenantLifecycleSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TenantLifecycleSnapshot {
    pub tenant_id: String,
    pub records: Vec<verlet_runtime_contracts::ThreadLifecycleRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TenantSnapshot {
    pub tenant_id: String,
    pub config: TenantRuntimeConfig,
    pub context: TenantRuntimeContextDescriptor,
    pub sessions: Vec<SessionSnapshot>,
    pub runtime: crate::kernel::runtime_host::runtime_api::RuntimeHostSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionSnapshot {
    pub user_id: String,
    pub session_id: String,
    pub thread_count: usize,
}

impl VerletSupervisor {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(SupervisorInner {
                tenants: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            }),
        }
    }

    pub async fn register_tenant(
        &self,
        registration: TenantRegistration,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let mut tenants = self.inner.tenants.write().await;
        let tenant_id = registration.context.tenant_id().to_string();
        if tenants.contains_key(&tenant_id) {
            return Err(crate::kernel::runtime_host::VerletError::TenantAlreadyExists(tenant_id));
        }
        std::fs::create_dir_all(registration.context.runtime_home()).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
        })?;
        std::fs::create_dir_all(registration.context.state_home()).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
        })?;
        std::fs::create_dir_all(registration.context.codex_home()).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
        })?;

        let (context, provided_session_store) = registration.context.take_session_store();
        let session_store = match provided_session_store {
            Some(store) => store,
            None => std::sync::Arc::new(
                verlet_history_sqlite::SqliteSessionStore::open(context.session_history_path())
                    .await
                    .map_err(|err| {
                        crate::kernel::runtime_host::VerletError::History(err.to_string())
                    })?,
            ) as std::sync::Arc<dyn verlet_history::RuntimeStore>,
        };
        let tenant = TenantRuntime {
            tenant_id: tenant_id.clone(),
            host: crate::kernel::runtime_host::RuntimeHost::with_session_store_and_policy(
                registration.runtime_factory,
                session_store,
                context.execution_policy().clone(),
            ),
            context,
        };
        tenants.insert(tenant_id, std::sync::Arc::new(tenant));
        Ok(())
    }

    pub async fn set_thread_lifecycle_sink(
        &self,
        tenant_id: &str,
        sink: Option<
            std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ThreadLifecycleSink>,
        >,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .set_lifecycle_sink(sink)
            .await;
        Ok(())
    }

    pub(crate) async fn set_process_handle_dispatcher(
        &self,
        tenant_id: &str,
        dispatcher: Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .set_process_handle_dispatcher(dispatcher)
            .await;
        Ok(())
    }

    pub async fn set_process_handle_ingress(
        &self,
        tenant_id: &str,
        sink: Option<
            std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink>,
        >,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .set_process_handle_ingress(sink)
            .await;
        Ok(())
    }

    pub async fn set_remote_thread_executor(
        &self,
        tenant_id: &str,
        executor: Option<
            std::sync::Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>,
        >,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .set_remote_thread_executor(executor)
            .await;
        Ok(())
    }

    pub(crate) async fn kernel_control(
        &self,
        tenant_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::runtime_host::kernel_control::RuntimeKernelControl,
    > {
        Ok(self.tenant(tenant_id).await?.host.kernel_control())
    }

    pub async fn start_thread(
        &self,
        request: ThreadStartRequest,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        let tenant = self.tenant(&request.tenant_id).await?;
        let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
            request.tenant_id.clone(),
            request.user_id.clone(),
            request.session_id.clone(),
        );
        let requested_scope = coordinates.scope();
        let topology = request.topology;
        let metadata = request.metadata;
        Self::validate_thread_topology(&tenant, &requested_scope, &topology).await?;
        tenant
            .host
            .start_thread_with_topology_and_metadata(coordinates, topology, metadata)
            .await
    }

    pub(crate) async fn start_thread_with_id(
        &self,
        request: ThreadStartRequest,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        let tenant = self.tenant(&request.tenant_id).await?;
        let coordinates = verlet_runtime_contracts::ThreadCoordinates {
            tenant_id: request.tenant_id,
            user_id: request.user_id,
            session_id: request.session_id,
            thread_id,
        };
        let requested_scope = coordinates.scope();
        let topology = request.topology;
        let metadata = request.metadata;
        Self::validate_thread_topology(&tenant, &requested_scope, &topology).await?;
        tenant
            .host
            .start_thread_with_topology_and_metadata(coordinates, topology, metadata)
            .await
    }

    pub async fn load_thread_from_lifecycle(
        &self,
        record: verlet_runtime_contracts::ThreadLifecycleRecord,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        let tenant = self.tenant(&record.coordinates.tenant_id).await?;
        let requested_scope = record.coordinates.scope();
        let topology = record.topology;
        let metadata = record.metadata;
        Self::validate_thread_topology(&tenant, &requested_scope, &topology).await?;
        tenant
            .host
            .load_thread_with_topology_and_metadata(record.coordinates, topology, metadata)
            .await
    }

    pub(crate) async fn runtime_store(
        &self,
        tenant_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<std::sync::Arc<dyn verlet_history::RuntimeStore>>
    {
        Ok(self.tenant(tenant_id).await?.host.runtime_store())
    }

    async fn validate_thread_topology(
        tenant: &TenantRuntime,
        requested_scope: &verlet_runtime_contracts::ThreadScope,
        topology: &verlet_runtime_contracts::ThreadTopology,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        for related_thread_id in topology.related_thread_ids() {
            Self::validate_related_thread_scope(tenant, requested_scope, related_thread_id).await?;
        }
        Ok(())
    }

    async fn validate_related_thread_scope(
        tenant: &TenantRuntime,
        requested_scope: &verlet_runtime_contracts::ThreadScope,
        related_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let related = tenant
            .host
            .get_thread(related_thread_id)
            .await
            .map_err(|_| {
                crate::kernel::runtime_host::VerletError::RelatedThreadNotFound(related_thread_id)
            })?;
        let actual_scope = related.context().coordinates.scope();
        if actual_scope != *requested_scope {
            return Err(
                crate::kernel::runtime_host::VerletError::RelatedThreadScopeMismatch {
                    thread_id: related_thread_id,
                    requested: Box::new(requested_scope.clone()),
                    actual: Box::new(actual_scope),
                },
            );
        }
        Ok(())
    }

    pub async fn get_thread(
        &self,
        tenant_id: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        self.tenant(tenant_id)
            .await?
            .host
            .get_thread(thread_id)
            .await
    }

    pub async fn get_thread_at(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        let thread = self
            .get_thread(&coordinates.tenant_id, coordinates.thread_id)
            .await?;
        self.validate_thread_scope(coordinates, &thread)?;
        Ok(thread)
    }

    pub(crate) async fn wait_for_thread_start_reservation(
        &self,
        tenant_id: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .wait_for_thread_start_reservation(thread_id)
            .await;
        Ok(())
    }

    pub async fn submit(
        &self,
        tenant_id: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .submit(thread_id, turn_id, input)
            .await
    }

    pub async fn submit_turn(
        &self,
        tenant_id: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .submit_turn(thread_id, turn_id, input)
            .await
    }

    pub async fn submit_with_mode(
        &self,
        tenant_id: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .submit_with_mode(thread_id, turn_id, input, mode)
            .await
    }

    pub async fn submit_turn_with_mode(
        &self,
        tenant_id: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .submit_turn_with_mode(thread_id, turn_id, input, mode)
            .await
    }

    pub async fn submit_to(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .submit(coordinates.thread_id, turn_id, input)
            .await
    }

    pub async fn submit_turn_to(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: impl Into<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .submit_turn(coordinates.thread_id, turn_id, input)
            .await
    }

    pub async fn submit_to_with_mode(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: impl Into<String>,
        input: impl Into<String>,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .submit_with_mode(coordinates.thread_id, turn_id, input, mode)
            .await
    }

    pub async fn submit_turn_to_with_mode(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: impl Into<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .submit_turn_with_mode(coordinates.thread_id, turn_id, input, mode)
            .await
    }

    pub(crate) async fn submit_admitted_turn_to(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: impl Into<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
        admission: Option<crate::kernel::admission::AdmissionGateContext>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        let tenant = self.tenant(&coordinates.tenant_id).await?;
        crate::kernel::admission::submit_turn(
            &tenant.host,
            coordinates.thread_id,
            turn_id,
            input,
            mode,
            admission,
        )
        .await
    }

    pub(crate) async fn reserve_admitted_turn_to(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: impl Into<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
        admission: Option<crate::kernel::admission::AdmissionGateContext>,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::runtime_host::ReservedTurnSubmission,
    > {
        self.get_thread_at(coordinates).await?;
        let tenant = self.tenant(&coordinates.tenant_id).await?;
        crate::kernel::admission::reserve_turn(
            &tenant.host,
            coordinates.thread_id,
            turn_id,
            input,
            mode,
            admission,
        )
        .await
    }

    pub async fn compact_thread_at(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: impl Into<String>,
        summary: Option<String>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .compact_thread(coordinates.thread_id, turn_id, summary)
            .await
    }

    pub async fn cancel(
        &self,
        tenant_id: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
        reason: impl Into<String>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .cancel(thread_id, reason)
            .await
    }

    pub async fn cancel_at(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        reason: impl Into<String>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .cancel(coordinates.thread_id, reason)
            .await
    }

    pub async fn shutdown_thread(
        &self,
        tenant_id: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .shutdown_thread(thread_id)
            .await
    }

    pub async fn shutdown_thread_at(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.shutdown_thread(&coordinates.tenant_id, coordinates.thread_id)
            .await
    }

    pub async fn shutdown_tenant(
        &self,
        tenant_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_runtime_contracts::ThreadId>> {
        self.tenant(tenant_id).await?.host.shutdown_all().await
    }

    pub async fn shutdown_all(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<
        Vec<(String, Vec<verlet_runtime_contracts::ThreadId>)>,
    > {
        let tenants = {
            let tenants = self.inner.tenants.read().await;
            tenants
                .values()
                .map(|tenant| (tenant.tenant_id.clone(), std::sync::Arc::clone(tenant)))
                .collect::<Vec<_>>()
        };
        let mut stopped = Vec::with_capacity(tenants.len());
        for (tenant_id, tenant) in tenants {
            stopped.push((tenant_id, tenant.host.shutdown_all().await?));
        }
        stopped.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(stopped)
    }

    pub async fn children_of(
        &self,
        tenant_id: &str,
        parent_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<
        Vec<crate::kernel::runtime_host::RuntimeThreadHandle>,
    > {
        Ok(self
            .tenant(tenant_id)
            .await?
            .host
            .children_of(parent_thread_id)
            .await)
    }

    pub async fn children_of_at(
        &self,
        parent_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> crate::kernel::runtime_host::VerletResult<
        Vec<crate::kernel::runtime_host::RuntimeThreadHandle>,
    > {
        self.tenant(&parent_coordinates.tenant_id)
            .await?
            .host
            .children_of_at(parent_coordinates)
            .await
    }

    pub async fn create_checkpoint_at(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_checkpoint_id: Option<verlet_runtime_contracts::ThreadCheckpointId>,
        label: Option<String>,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    > {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .create_checkpoint(coordinates.thread_id, parent_checkpoint_id, label, metadata)
            .await
    }

    pub async fn resume_thread_at(
        &self,
        tenant_id: &str,
        checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        self.tenant(tenant_id)
            .await?
            .host
            .resume_thread(checkpoint_id)
            .await
    }

    pub async fn checkpoint_at(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    > {
        self.get_thread_at(coordinates).await?;
        let checkpoint = self
            .tenant(&coordinates.tenant_id)
            .await?
            .host
            .checkpoint(checkpoint_id)
            .await?;
        if checkpoint.coordinates.thread_id != coordinates.thread_id {
            return Err(
                crate::kernel::runtime_host::VerletError::ThreadScopeMismatch {
                    thread_id: coordinates.thread_id,
                    requested: Box::new(coordinates.scope()),
                    actual: Box::new(checkpoint.coordinates.scope()),
                },
            );
        }
        Ok(checkpoint)
    }

    pub async fn resume_thread_from_checkpoint_at(
        &self,
        checkpoint: crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        self.tenant(&checkpoint.coordinates.tenant_id)
            .await?
            .host
            .resume_thread_from_checkpoint(checkpoint)
            .await
    }

    pub async fn fork_thread_at(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        checkpoint_id: Option<verlet_runtime_contracts::ThreadCheckpointId>,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .fork_thread(coordinates.thread_id, checkpoint_id)
            .await
    }

    pub async fn fork_thread_from_checkpoint_at(
        &self,
        checkpoint: crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        self.tenant(&checkpoint.coordinates.tenant_id)
            .await?
            .host
            .fork_thread_from_checkpoint(checkpoint)
            .await
    }

    pub(crate) async fn fork_thread_from_checkpoint_with_id_at(
        &self,
        checkpoint: crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
        child_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        self.tenant(&checkpoint.coordinates.tenant_id)
            .await?
            .host
            .fork_thread_from_checkpoint_with_id(checkpoint, child_thread_id)
            .await
    }

    pub async fn fork_history_by_reference_at(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        base: verlet_history::ThreadBaseRef,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.get_thread_at(source_coordinates).await?;
        self.get_thread_at(target_coordinates).await?;
        self.tenant(&source_coordinates.tenant_id)
            .await?
            .host
            .fork_history_by_reference(source_coordinates, target_coordinates, base)
            .await
    }

    pub async fn snapshot(&self) -> SupervisorSnapshot {
        let tenants = self.inner.tenants.read().await;
        let mut snapshots = Vec::with_capacity(tenants.len());
        for tenant in tenants.values() {
            let runtime = tenant.host.snapshot().await;
            snapshots.push(TenantSnapshot {
                tenant_id: tenant.tenant_id.clone(),
                config: tenant.context.config().clone(),
                context: tenant.context.descriptor(),
                sessions: sessions_from_runtime_snapshot(&runtime),
                runtime,
            });
        }
        snapshots.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
        SupervisorSnapshot { tenants: snapshots }
    }

    pub async fn lifecycle_snapshot(&self) -> SupervisorLifecycleSnapshot {
        let tenants = self.inner.tenants.read().await;
        let mut snapshots = Vec::with_capacity(tenants.len());
        for tenant in tenants.values() {
            snapshots.push(TenantLifecycleSnapshot {
                tenant_id: tenant.tenant_id.clone(),
                records: tenant.host.lifecycle_snapshot().await.records,
            });
        }
        snapshots.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
        SupervisorLifecycleSnapshot { tenants: snapshots }
    }

    async fn tenant(
        &self,
        tenant_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<std::sync::Arc<TenantRuntime>> {
        self.inner
            .tenants
            .read()
            .await
            .get(tenant_id)
            .cloned()
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::TenantNotFound(tenant_id.to_string())
            })
    }

    fn validate_thread_scope(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        thread: &crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let requested = coordinates.scope();
        let actual = thread.context().coordinates.scope();
        if requested != actual {
            return Err(
                crate::kernel::runtime_host::VerletError::ThreadScopeMismatch {
                    thread_id: coordinates.thread_id,
                    requested: Box::new(requested),
                    actual: Box::new(actual),
                },
            );
        }
        Ok(())
    }
}

fn sessions_from_runtime_snapshot(
    runtime: &crate::kernel::runtime_host::runtime_api::RuntimeHostSnapshot,
) -> Vec<SessionSnapshot> {
    let mut session_counts = std::collections::BTreeMap::<(String, String), usize>::new();
    for thread in &runtime.threads {
        let coordinates = &thread.context.coordinates;
        *session_counts
            .entry((coordinates.user_id.clone(), coordinates.session_id.clone()))
            .or_default() += 1;
    }
    session_counts
        .into_iter()
        .map(|((user_id, session_id), thread_count)| SessionSnapshot {
            user_id,
            session_id,
            thread_count,
        })
        .collect()
}

impl Default for VerletSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
