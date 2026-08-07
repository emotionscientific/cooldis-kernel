/// Records whether a checkpoint is proven safe for V1 root-only resume.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadCheckpointLineage {
    /// The checkpoint predates explicit lineage recording and cannot be
    /// established as safe for root-only resume.
    #[default]
    Unknown,
    Root,
    Parent {
        parent_thread_id: verlet_runtime_contracts::ThreadId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadCheckpoint {
    pub id: verlet_runtime_contracts::ThreadCheckpointId,
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    #[serde(default)]
    pub lineage: ThreadCheckpointLineage,
    pub parent_checkpoint_id: Option<verlet_runtime_contracts::ThreadCheckpointId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_entry_id: Option<verlet_history::SessionEntryId>,
    pub label: Option<String>,
    pub metadata: std::collections::BTreeMap<String, String>,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ThreadCommand {
    Submit {
        turn_id: String,
        input: crate::kernel::runtime_host::turn::TurnInput,
        #[serde(default)]
        mode: verlet_runtime_contracts::TurnSubmissionMode,
    },
    Compact {
        turn_id: String,
        trigger: crate::kernel::compaction::CompactionTrigger,
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ThreadEvent {
    Runtime {
        thread_id: verlet_runtime_contracts::ThreadId,
        event: crate::kernel::runtime_host::runtime_events::RuntimeEvent,
    },
    Started {
        context: verlet_runtime_contracts::ThreadContext,
    },
    CanonicalMirror {
        thread_id: verlet_runtime_contracts::ThreadId,
        entry: verlet_history::SessionEntry,
    },
    Output {
        thread_id: verlet_runtime_contracts::ThreadId,
        text: String,
    },
    Cancelled {
        thread_id: verlet_runtime_contracts::ThreadId,
        reason: String,
    },
    Signal {
        thread_id: verlet_runtime_contracts::ThreadId,
        signal: verlet_runtime_contracts::ThreadSignal,
    },
    Stopped {
        thread_id: verlet_runtime_contracts::ThreadId,
    },
    Failed {
        thread_id: verlet_runtime_contracts::ThreadId,
        message: String,
    },
}

#[async_trait::async_trait]
pub trait AgentRuntime: Send + Sync + 'static {
    async fn run(
        self: Box<Self>,
        context: verlet_runtime_contracts::ThreadContext,
        services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
        commands: tokio::sync::mpsc::Receiver<ThreadCommand>,
        events: tokio::sync::broadcast::Sender<ThreadEvent>,
        status: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    );
}

#[async_trait::async_trait]
pub trait AgentRuntimeFactory: Send + Sync + 'static {
    async fn build(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn AgentRuntime>>;
}

#[async_trait::async_trait]
pub trait ThreadLifecycleSink: Send + Sync + 'static {
    async fn thread_started(
        &self,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> crate::kernel::runtime_host::VerletResult<()>;
}

/// Owning-surface ingress for durable process dispatch and settlement facts.
///
/// Implementations acknowledge only after ADR 0003 has durably settled the
/// envelope. Process managers retain terminal entries until this boundary
/// succeeds, so cancellation or a transient bridge failure can be retried.
#[async_trait::async_trait]
pub trait ProcessHandleIngressSink: Send + Sync + 'static {
    async fn submit_process_handle_envelope(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> crate::kernel::runtime_host::VerletResult<()>;
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeHostSnapshot {
    pub threads: Vec<ThreadSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadSnapshot {
    pub context: verlet_runtime_contracts::ThreadContext,
    pub status: verlet_runtime_contracts::ThreadStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeHostLifecycleSnapshot {
    pub records: Vec<verlet_runtime_contracts::ThreadLifecycleRecord>,
}
