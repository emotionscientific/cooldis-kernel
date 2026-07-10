use super::{CooldisResult, RuntimeEvent, RuntimeServices, RuntimeThreadHandle, TurnInput};
use crate::CompactionTrigger;
use crate::kernel::history::{SessionEntry, SessionEntryId};
use async_trait::async_trait;
use cooldis_runtime_contracts::{
    ThreadCheckpointId, ThreadContext, ThreadCoordinates, ThreadId, ThreadLifecycleRecord,
    ThreadSignal, ThreadStatus, TurnSubmissionMode,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Records whether a checkpoint is proven safe for V1 root-only resume.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadCheckpointLineage {
    /// The checkpoint predates explicit lineage recording and cannot be
    /// established as safe for root-only resume.
    #[default]
    Unknown,
    Root,
    Parent {
        parent_thread_id: ThreadId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadCheckpoint {
    pub id: ThreadCheckpointId,
    pub coordinates: ThreadCoordinates,
    #[serde(default)]
    pub lineage: ThreadCheckpointLineage,
    pub parent_checkpoint_id: Option<ThreadCheckpointId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_entry_id: Option<SessionEntryId>,
    pub label: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ThreadCommand {
    Submit {
        turn_id: String,
        input: TurnInput,
        #[serde(default)]
        mode: TurnSubmissionMode,
    },
    Compact {
        turn_id: String,
        trigger: CompactionTrigger,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
    ResumeToolCall {
        turn_id: String,
        call_id: String,
    },
    Cancel {
        reason: String,
    },
    CancelTurn {
        watchdog_token_id: u64,
        reason: String,
    },
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ThreadEvent {
    Runtime {
        thread_id: ThreadId,
        event: RuntimeEvent,
    },
    Started {
        context: ThreadContext,
    },
    CanonicalMirror {
        thread_id: ThreadId,
        entry: SessionEntry,
    },
    Output {
        thread_id: ThreadId,
        text: String,
    },
    Cancelled {
        thread_id: ThreadId,
        reason: String,
    },
    Signal {
        thread_id: ThreadId,
        signal: ThreadSignal,
    },
    Stopped {
        thread_id: ThreadId,
    },
    Failed {
        thread_id: ThreadId,
        message: String,
    },
}

#[async_trait]
pub trait AgentRuntime: Send + Sync + 'static {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    );
}

#[async_trait]
pub trait AgentRuntimeFactory: Send + Sync + 'static {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>>;
}

#[async_trait]
pub trait ThreadLifecycleSink: Send + Sync + 'static {
    async fn thread_started(&self, handle: RuntimeThreadHandle) -> CooldisResult<()>;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeHostSnapshot {
    pub threads: Vec<ThreadSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadSnapshot {
    pub context: ThreadContext,
    pub status: ThreadStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeHostLifecycleSnapshot {
    pub records: Vec<ThreadLifecycleRecord>,
}
