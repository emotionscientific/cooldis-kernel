//! Remote placement execution seam.
//!
//! Placement is resolved before this seam. The kernel invokes exactly one
//! generation-local executor for a `Remote` binding; an absent executor is a
//! hard error, never permission to fall back to local execution.

use crate::{EventRecordId, ThreadContext, ThreadId, ThreadStatus, TurnInput, VerletResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use verlet_runtime_contracts::DispatchId;

/// Everything the process-backed executor needs after the parent has
/// durably witnessed `thread.spawned`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteThreadSpawnRequest {
    pub child: ThreadContext,
    pub task_name: Option<String>,
    pub turn_id: String,
    pub dispatch_id: DispatchId,
    pub input: TurnInput,
    pub spawned_event_id: EventRecordId,
    pub compile_payload: Option<Value>,
    pub bind_payload: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteThreadSubmitRequest {
    pub target_thread_id: ThreadId,
    pub turn_id: String,
    pub dispatch_id: DispatchId,
    pub input: TurnInput,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteThreadObservation {
    pub status: ThreadStatus,
    pub latest_output: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteThreadWaitObservation {
    pub observation: RemoteThreadObservation,
    pub timed_out: bool,
}

#[async_trait]
pub trait RemoteThreadExecutor: Send + Sync {
    /// Return the immutable child context for an executor-owned projection.
    /// Kernel callers use it to preserve the same scope and topology checks as
    /// resident local threads before dispatching any operation.
    async fn context(&self, thread_id: ThreadId) -> Option<ThreadContext>;

    async fn spawn(&self, request: RemoteThreadSpawnRequest) -> VerletResult<()>;

    async fn submit(&self, request: RemoteThreadSubmitRequest) -> VerletResult<ThreadStatus>;

    async fn observe(&self, thread_id: ThreadId) -> VerletResult<RemoteThreadObservation>;

    async fn wait(
        &self,
        thread_id: ThreadId,
        timeout_ms: Option<u64>,
    ) -> VerletResult<RemoteThreadWaitObservation>;
}
