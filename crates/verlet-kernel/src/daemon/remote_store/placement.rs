//! Remote placement execution seam.
//!
//! Placement is resolved before this seam. The kernel invokes exactly one
//! generation-local executor for a `Remote` binding; an absent executor is a
//! hard error, never permission to fall back to local execution.

/// Everything the process-backed executor needs after the parent has
/// durably witnessed `thread.spawned`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RemoteThreadSpawnRequest {
    pub child: crate::ThreadContext,
    pub task_name: Option<String>,
    pub turn_id: String,
    pub dispatch_id: verlet_runtime_contracts::DispatchId,
    pub input: crate::TurnInput,
    pub spawned_event_id: crate::EventRecordId,
    pub compile_payload: Option<serde_json::Value>,
    pub bind_payload: Option<serde_json::Value>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RemoteThreadSubmitRequest {
    pub target_thread_id: crate::ThreadId,
    pub turn_id: String,
    pub dispatch_id: verlet_runtime_contracts::DispatchId,
    pub input: crate::TurnInput,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteThreadObservation {
    pub status: crate::ThreadStatus,
    pub latest_output: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteThreadWaitObservation {
    pub observation: RemoteThreadObservation,
    pub timed_out: bool,
}

#[async_trait::async_trait]
pub trait RemoteThreadExecutor: Send + Sync {
    /// Return the immutable child context for an executor-owned projection.
    /// Kernel callers use it to preserve the same scope and topology checks as
    /// resident local threads before dispatching any operation.
    async fn context(&self, thread_id: crate::ThreadId) -> Option<crate::ThreadContext>;

    async fn spawn(&self, request: RemoteThreadSpawnRequest) -> crate::VerletResult<()>;

    async fn submit(
        &self,
        request: RemoteThreadSubmitRequest,
    ) -> crate::VerletResult<crate::ThreadStatus>;

    async fn observe(
        &self,
        thread_id: crate::ThreadId,
    ) -> crate::VerletResult<RemoteThreadObservation>;

    async fn wait(
        &self,
        thread_id: crate::ThreadId,
        timeout_ms: Option<u64>,
    ) -> crate::VerletResult<RemoteThreadWaitObservation>;
}
