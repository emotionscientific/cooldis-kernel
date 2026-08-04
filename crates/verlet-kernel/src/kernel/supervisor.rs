use crate::kernel::admission::AdmissionGateContext;
use crate::kernel::runtime_host::ReservedTurnSubmission;
use crate::{
    AgentRuntimeFactory, RuntimeExecutionPolicy, RuntimeHost, RuntimeHostSnapshot, RuntimeStore,
    RuntimeThreadHandle, SqliteSessionStore, ThreadBaseRef, ThreadCheckpoint, ThreadCheckpointId,
    ThreadCoordinates, ThreadId, ThreadLifecycleRecord, ThreadLifecycleSink, ThreadScope,
    ThreadTopology, TurnInput, TurnSubmissionMode, VerletError, VerletResult,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct VerletSupervisor {
    inner: Arc<SupervisorInner>,
}

struct SupervisorInner {
    tenants: RwLock<HashMap<String, Arc<TenantRuntime>>>,
}

struct TenantRuntime {
    tenant_id: String,
    context: TenantRuntimeContext,
    host: RuntimeHost,
}

pub struct TenantRegistration {
    pub context: TenantRuntimeContext,
    pub runtime_factory: Arc<dyn AgentRuntimeFactory>,
}

#[derive(Clone)]
pub struct TenantRuntimeContext {
    tenant_id: String,
    config: TenantRuntimeConfig,
    session_store: Option<Arc<dyn RuntimeStore>>,
    execution_policy: RuntimeExecutionPolicy,
}

impl TenantRuntimeContext {
    pub fn local(
        tenant_id: impl Into<String>,
        runtime_home: impl Into<PathBuf>,
        state_home: impl Into<PathBuf>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            config: TenantRuntimeConfig::local(runtime_home, state_home),
            session_store: None,
            execution_policy: RuntimeExecutionPolicy::default(),
        }
    }

    pub fn with_session_store(mut self, session_store: Arc<dyn RuntimeStore>) -> Self {
        self.session_store = Some(session_store);
        self
    }

    pub fn with_execution_policy(mut self, execution_policy: RuntimeExecutionPolicy) -> Self {
        self.execution_policy = execution_policy;
        self
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn config(&self) -> &TenantRuntimeConfig {
        &self.config
    }

    pub fn execution_policy(&self) -> &RuntimeExecutionPolicy {
        &self.execution_policy
    }

    pub fn runtime_home(&self) -> &std::path::Path {
        &self.config.runtime_home
    }

    pub fn state_home(&self) -> &std::path::Path {
        &self.config.state_home
    }

    pub fn codex_home(&self) -> PathBuf {
        self.config.runtime_home.join("codex-home")
    }

    pub fn session_history_path(&self) -> PathBuf {
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

    fn take_session_store(self) -> (TenantRuntimeContext, Option<Arc<dyn RuntimeStore>>) {
        let session_store = self.session_store.clone();
        (self, session_store)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantRuntimeContextDescriptor {
    pub tenant_id: String,
    pub runtime_home: PathBuf,
    pub state_home: PathBuf,
    pub codex_home: PathBuf,
    pub session_history_path: PathBuf,
    pub execution_policy: RuntimeExecutionPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantRuntimeConfig {
    pub runtime_home: PathBuf,
    pub state_home: PathBuf,
}

impl TenantRuntimeConfig {
    pub fn local(runtime_home: impl Into<PathBuf>, state_home: impl Into<PathBuf>) -> Self {
        Self {
            runtime_home: runtime_home.into(),
            state_home: state_home.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadStartRequest {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    #[serde(default)]
    pub topology: ThreadTopology,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SupervisorSnapshot {
    pub tenants: Vec<TenantSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SupervisorLifecycleSnapshot {
    pub tenants: Vec<TenantLifecycleSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantLifecycleSnapshot {
    pub tenant_id: String,
    pub records: Vec<ThreadLifecycleRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantSnapshot {
    pub tenant_id: String,
    pub config: TenantRuntimeConfig,
    pub context: TenantRuntimeContextDescriptor,
    pub sessions: Vec<SessionSnapshot>,
    pub runtime: RuntimeHostSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub user_id: String,
    pub session_id: String,
    pub thread_count: usize,
}

impl VerletSupervisor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SupervisorInner {
                tenants: RwLock::new(HashMap::new()),
            }),
        }
    }

    pub async fn register_tenant(&self, registration: TenantRegistration) -> VerletResult<()> {
        let mut tenants = self.inner.tenants.write().await;
        let tenant_id = registration.context.tenant_id().to_string();
        if tenants.contains_key(&tenant_id) {
            return Err(VerletError::TenantAlreadyExists(tenant_id));
        }
        std::fs::create_dir_all(registration.context.runtime_home())
            .map_err(|err| VerletError::RuntimeFactory(err.to_string()))?;
        std::fs::create_dir_all(registration.context.state_home())
            .map_err(|err| VerletError::RuntimeFactory(err.to_string()))?;
        std::fs::create_dir_all(registration.context.codex_home())
            .map_err(|err| VerletError::RuntimeFactory(err.to_string()))?;

        let (context, provided_session_store) = registration.context.take_session_store();
        let session_store = match provided_session_store {
            Some(store) => store,
            None => Arc::new(
                SqliteSessionStore::open(context.session_history_path())
                    .await
                    .map_err(|err| VerletError::History(err.to_string()))?,
            ) as Arc<dyn RuntimeStore>,
        };
        let tenant = TenantRuntime {
            tenant_id: tenant_id.clone(),
            host: RuntimeHost::with_session_store_and_policy(
                registration.runtime_factory,
                session_store,
                context.execution_policy().clone(),
            ),
            context,
        };
        tenants.insert(tenant_id, Arc::new(tenant));
        Ok(())
    }

    pub async fn set_thread_lifecycle_sink(
        &self,
        tenant_id: &str,
        sink: Option<Arc<dyn ThreadLifecycleSink>>,
    ) -> VerletResult<()> {
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
    ) -> VerletResult<()> {
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
        sink: Option<Arc<dyn crate::ProcessHandleIngressSink>>,
    ) -> VerletResult<()> {
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
        executor: Option<Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>>,
    ) -> VerletResult<()> {
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
    ) -> VerletResult<crate::RuntimeKernelControl> {
        Ok(self.tenant(tenant_id).await?.host.kernel_control())
    }

    pub async fn start_thread(
        &self,
        request: ThreadStartRequest,
    ) -> VerletResult<RuntimeThreadHandle> {
        let tenant = self.tenant(&request.tenant_id).await?;
        let coordinates = ThreadCoordinates::new(
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
        thread_id: ThreadId,
    ) -> VerletResult<RuntimeThreadHandle> {
        let tenant = self.tenant(&request.tenant_id).await?;
        let coordinates = ThreadCoordinates {
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
        record: ThreadLifecycleRecord,
    ) -> VerletResult<RuntimeThreadHandle> {
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
    ) -> VerletResult<Arc<dyn RuntimeStore>> {
        Ok(self.tenant(tenant_id).await?.host.runtime_store())
    }

    async fn validate_thread_topology(
        tenant: &TenantRuntime,
        requested_scope: &ThreadScope,
        topology: &ThreadTopology,
    ) -> VerletResult<()> {
        for related_thread_id in topology.related_thread_ids() {
            Self::validate_related_thread_scope(tenant, requested_scope, related_thread_id).await?;
        }
        Ok(())
    }

    async fn validate_related_thread_scope(
        tenant: &TenantRuntime,
        requested_scope: &ThreadScope,
        related_thread_id: ThreadId,
    ) -> VerletResult<()> {
        let related = tenant
            .host
            .get_thread(related_thread_id)
            .await
            .map_err(|_| VerletError::RelatedThreadNotFound(related_thread_id))?;
        let actual_scope = related.context().coordinates.scope();
        if actual_scope != *requested_scope {
            return Err(VerletError::RelatedThreadScopeMismatch {
                thread_id: related_thread_id,
                requested: Box::new(requested_scope.clone()),
                actual: Box::new(actual_scope),
            });
        }
        Ok(())
    }

    pub async fn get_thread(
        &self,
        tenant_id: &str,
        thread_id: ThreadId,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.tenant(tenant_id)
            .await?
            .host
            .get_thread(thread_id)
            .await
    }

    pub async fn get_thread_at(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> VerletResult<RuntimeThreadHandle> {
        let thread = self
            .get_thread(&coordinates.tenant_id, coordinates.thread_id)
            .await?;
        self.validate_thread_scope(coordinates, &thread)?;
        Ok(thread)
    }

    pub(crate) async fn wait_for_thread_start_reservation(
        &self,
        tenant_id: &str,
        thread_id: ThreadId,
    ) -> VerletResult<()> {
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
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .submit(thread_id, turn_id, input)
            .await
    }

    pub async fn submit_turn(
        &self,
        tenant_id: &str,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: TurnInput,
    ) -> VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .submit_turn(thread_id, turn_id, input)
            .await
    }

    pub async fn submit_with_mode(
        &self,
        tenant_id: &str,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
        mode: TurnSubmissionMode,
    ) -> VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .submit_with_mode(thread_id, turn_id, input, mode)
            .await
    }

    pub async fn submit_turn_with_mode(
        &self,
        tenant_id: &str,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: TurnInput,
        mode: TurnSubmissionMode,
    ) -> VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .submit_turn_with_mode(thread_id, turn_id, input, mode)
            .await
    }

    pub async fn submit_to(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .submit(coordinates.thread_id, turn_id, input)
            .await
    }

    pub async fn submit_turn_to(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: impl Into<String>,
        input: TurnInput,
    ) -> VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .submit_turn(coordinates.thread_id, turn_id, input)
            .await
    }

    pub async fn submit_to_with_mode(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: impl Into<String>,
        input: impl Into<String>,
        mode: TurnSubmissionMode,
    ) -> VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .submit_with_mode(coordinates.thread_id, turn_id, input, mode)
            .await
    }

    pub async fn submit_turn_to_with_mode(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: impl Into<String>,
        input: TurnInput,
        mode: TurnSubmissionMode,
    ) -> VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .submit_turn_with_mode(coordinates.thread_id, turn_id, input, mode)
            .await
    }

    pub(crate) async fn submit_admitted_turn_to(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: impl Into<String>,
        input: TurnInput,
        mode: TurnSubmissionMode,
        admission: Option<AdmissionGateContext>,
    ) -> VerletResult<()> {
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
        coordinates: &ThreadCoordinates,
        turn_id: impl Into<String>,
        input: TurnInput,
        mode: TurnSubmissionMode,
        admission: Option<AdmissionGateContext>,
    ) -> VerletResult<ReservedTurnSubmission> {
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
        coordinates: &ThreadCoordinates,
        turn_id: impl Into<String>,
        summary: Option<String>,
    ) -> VerletResult<()> {
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
        thread_id: ThreadId,
        reason: impl Into<String>,
    ) -> VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .cancel(thread_id, reason)
            .await
    }

    pub async fn cancel_at(
        &self,
        coordinates: &ThreadCoordinates,
        reason: impl Into<String>,
    ) -> VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .cancel(coordinates.thread_id, reason)
            .await
    }

    pub async fn shutdown_thread(&self, tenant_id: &str, thread_id: ThreadId) -> VerletResult<()> {
        self.tenant(tenant_id)
            .await?
            .host
            .shutdown_thread(thread_id)
            .await
    }

    pub async fn shutdown_thread_at(&self, coordinates: &ThreadCoordinates) -> VerletResult<()> {
        self.get_thread_at(coordinates).await?;
        self.shutdown_thread(&coordinates.tenant_id, coordinates.thread_id)
            .await
    }

    pub async fn shutdown_tenant(&self, tenant_id: &str) -> VerletResult<Vec<ThreadId>> {
        self.tenant(tenant_id).await?.host.shutdown_all().await
    }

    pub async fn shutdown_all(&self) -> VerletResult<Vec<(String, Vec<ThreadId>)>> {
        let tenants = {
            let tenants = self.inner.tenants.read().await;
            tenants
                .values()
                .map(|tenant| (tenant.tenant_id.clone(), Arc::clone(tenant)))
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
        parent_thread_id: ThreadId,
    ) -> VerletResult<Vec<RuntimeThreadHandle>> {
        Ok(self
            .tenant(tenant_id)
            .await?
            .host
            .children_of(parent_thread_id)
            .await)
    }

    pub async fn children_of_at(
        &self,
        parent_coordinates: &ThreadCoordinates,
    ) -> VerletResult<Vec<RuntimeThreadHandle>> {
        self.tenant(&parent_coordinates.tenant_id)
            .await?
            .host
            .children_of_at(parent_coordinates)
            .await
    }

    pub async fn create_checkpoint_at(
        &self,
        coordinates: &ThreadCoordinates,
        parent_checkpoint_id: Option<ThreadCheckpointId>,
        label: Option<String>,
        metadata: BTreeMap<String, String>,
    ) -> VerletResult<ThreadCheckpoint> {
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
        checkpoint_id: ThreadCheckpointId,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.tenant(tenant_id)
            .await?
            .host
            .resume_thread(checkpoint_id)
            .await
    }

    pub async fn checkpoint_at(
        &self,
        coordinates: &ThreadCoordinates,
        checkpoint_id: ThreadCheckpointId,
    ) -> VerletResult<ThreadCheckpoint> {
        self.get_thread_at(coordinates).await?;
        let checkpoint = self
            .tenant(&coordinates.tenant_id)
            .await?
            .host
            .checkpoint(checkpoint_id)
            .await?;
        if checkpoint.coordinates.thread_id != coordinates.thread_id {
            return Err(VerletError::ThreadScopeMismatch {
                thread_id: coordinates.thread_id,
                requested: Box::new(coordinates.scope()),
                actual: Box::new(checkpoint.coordinates.scope()),
            });
        }
        Ok(checkpoint)
    }

    pub async fn resume_thread_from_checkpoint_at(
        &self,
        checkpoint: ThreadCheckpoint,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.tenant(&checkpoint.coordinates.tenant_id)
            .await?
            .host
            .resume_thread_from_checkpoint(checkpoint)
            .await
    }

    pub async fn fork_thread_at(
        &self,
        coordinates: &ThreadCoordinates,
        checkpoint_id: Option<ThreadCheckpointId>,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.get_thread_at(coordinates).await?;
        self.tenant(&coordinates.tenant_id)
            .await?
            .host
            .fork_thread(coordinates.thread_id, checkpoint_id)
            .await
    }

    pub async fn fork_thread_from_checkpoint_at(
        &self,
        checkpoint: ThreadCheckpoint,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.tenant(&checkpoint.coordinates.tenant_id)
            .await?
            .host
            .fork_thread_from_checkpoint(checkpoint)
            .await
    }

    pub(crate) async fn fork_thread_from_checkpoint_with_id_at(
        &self,
        checkpoint: ThreadCheckpoint,
        child_thread_id: ThreadId,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.tenant(&checkpoint.coordinates.tenant_id)
            .await?
            .host
            .fork_thread_from_checkpoint_with_id(checkpoint, child_thread_id)
            .await
    }

    pub async fn fork_history_by_reference_at(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> VerletResult<()> {
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

    async fn tenant(&self, tenant_id: &str) -> VerletResult<Arc<TenantRuntime>> {
        self.inner
            .tenants
            .read()
            .await
            .get(tenant_id)
            .cloned()
            .ok_or_else(|| VerletError::TenantNotFound(tenant_id.to_string()))
    }

    fn validate_thread_scope(
        &self,
        coordinates: &ThreadCoordinates,
        thread: &RuntimeThreadHandle,
    ) -> VerletResult<()> {
        let requested = coordinates.scope();
        let actual = thread.context().coordinates.scope();
        if requested != actual {
            return Err(VerletError::ThreadScopeMismatch {
                thread_id: coordinates.thread_id,
                requested: Box::new(requested),
                actual: Box::new(actual),
            });
        }
        Ok(())
    }
}

fn sessions_from_runtime_snapshot(runtime: &RuntimeHostSnapshot) -> Vec<SessionSnapshot> {
    let mut session_counts = BTreeMap::<(String, String), usize>::new();
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
