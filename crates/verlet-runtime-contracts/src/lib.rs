//! Shared runtime contract types for Verlet.
//!
//! This crate is intentionally low in the dependency graph. It owns stable
//! identity, topology, status, and runtime event support types that projection,
//! history, adapter, and kernel crates can share without depending on the full
//! kernel implementation.

pub mod env_compat;
pub mod handle;
pub mod schema;

pub use handle::{
    DispatchId, HANDLE_DISPATCH_CONTENT_KIND, HANDLE_OUTCOME_CONTENT_KIND, HandleDispatchEnvelope,
    HandleId, HandleKind, HandleTerminalEnvelope, HandleTerminalOutcome,
};
pub use schema::{
    JsonSchemaResult, JsonSchemaValidationError, MAX_JSON_SCHEMA_SUBSET_DEPTH, SchemaRegistry,
    validate_json_schema_subset, validate_json_value_against_schema,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ThreadId(uuid::Uuid);

impl ThreadId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub fn parse_str(value: &str) -> Result<Self, uuid::Error> {
        uuid::Uuid::parse_str(value).map(Self)
    }
}

impl Default for ThreadId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ThreadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ThreadSignalId(uuid::Uuid);

impl ThreadSignalId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for ThreadSignalId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ThreadSignalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ThreadCheckpointId(uuid::Uuid);

impl ThreadCheckpointId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }

    pub fn parse_str(value: &str) -> Result<Self, uuid::Error> {
        uuid::Uuid::parse_str(value).map(Self)
    }
}

impl Default for ThreadCheckpointId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ThreadCheckpointId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RuntimeEventId(uuid::Uuid);

impl RuntimeEventId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for RuntimeEventId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RuntimeEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ThreadCoordinates {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub thread_id: ThreadId,
}

impl ThreadCoordinates {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        session_id: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            session_id: session_id.into(),
            thread_id: ThreadId::new(),
        }
    }

    pub fn scope(&self) -> ThreadScope {
        ThreadScope {
            tenant_id: self.tenant_id.clone(),
            user_id: self.user_id.clone(),
            session_id: self.session_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ThreadScope {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ThreadInitiationSource {
    Root,
    Thread {
        thread_id: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_id: Option<RuntimeEventId>,
    },
}

impl Default for ThreadInitiationSource {
    fn default() -> Self {
        Self::Root
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ThreadLineage {
    Root,
    Branch {
        parent_thread_id: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_id: Option<ThreadCheckpointId>,
    },
}

impl Default for ThreadLineage {
    fn default() -> Self {
        Self::Root
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadSpawnAttribution {
    pub source_thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_id: Option<RuntimeEventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_ref: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadTopology {
    #[serde(default)]
    pub initiation: ThreadInitiationSource,
    #[serde(default)]
    pub lineage: ThreadLineage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawn_attribution: Option<ThreadSpawnAttribution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_thread_id: Option<ThreadId>,
}

impl Default for ThreadTopology {
    fn default() -> Self {
        Self::root()
    }
}

impl ThreadTopology {
    pub fn root() -> Self {
        Self {
            initiation: ThreadInitiationSource::Root,
            lineage: ThreadLineage::Root,
            spawn_attribution: None,
            controller_thread_id: None,
        }
    }

    pub fn spawned_from(source_thread_id: ThreadId) -> Self {
        Self {
            initiation: ThreadInitiationSource::Thread {
                thread_id: source_thread_id,
                turn_id: None,
                event_id: None,
            },
            lineage: ThreadLineage::Root,
            spawn_attribution: Some(ThreadSpawnAttribution {
                source_thread_id,
                source_turn_id: None,
                source_event_id: None,
                prompt_ref: None,
            }),
            controller_thread_id: Some(source_thread_id),
        }
    }

    pub fn branch_from(
        parent_thread_id: ThreadId,
        checkpoint_id: Option<ThreadCheckpointId>,
    ) -> Self {
        Self {
            initiation: ThreadInitiationSource::Root,
            lineage: ThreadLineage::Branch {
                parent_thread_id,
                checkpoint_id,
            },
            spawn_attribution: None,
            controller_thread_id: None,
        }
    }

    pub fn compatibility_parent_thread_id(&self) -> Option<ThreadId> {
        match &self.lineage {
            ThreadLineage::Branch {
                parent_thread_id, ..
            } => Some(*parent_thread_id),
            ThreadLineage::Root => self
                .spawn_attribution
                .as_ref()
                .map(|attribution| attribution.source_thread_id),
        }
    }

    pub fn spawn_source_thread_id(&self) -> Option<ThreadId> {
        self.spawn_attribution
            .as_ref()
            .map(|attribution| attribution.source_thread_id)
    }

    pub fn branch_parent_thread_id(&self) -> Option<ThreadId> {
        match &self.lineage {
            ThreadLineage::Branch {
                parent_thread_id, ..
            } => Some(*parent_thread_id),
            ThreadLineage::Root => None,
        }
    }

    pub fn controller_thread_id(&self) -> Option<ThreadId> {
        self.controller_thread_id
    }

    pub fn related_thread_ids(&self) -> Vec<ThreadId> {
        let mut related_thread_ids = Vec::new();
        if let ThreadInitiationSource::Thread { thread_id, .. } = &self.initiation {
            related_thread_ids.push(*thread_id);
        }
        if let ThreadLineage::Branch {
            parent_thread_id, ..
        } = &self.lineage
        {
            related_thread_ids.push(*parent_thread_id);
        }
        if let Some(attribution) = &self.spawn_attribution {
            related_thread_ids.push(attribution.source_thread_id);
        }
        if let Some(controller_thread_id) = self.controller_thread_id {
            related_thread_ids.push(controller_thread_id);
        }
        related_thread_ids
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadContext {
    pub coordinates: ThreadCoordinates,
    pub parent_thread_id: Option<ThreadId>,
    #[serde(default)]
    pub topology: ThreadTopology,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl ThreadContext {
    pub fn root(coordinates: ThreadCoordinates) -> Self {
        Self::with_topology(coordinates, ThreadTopology::root())
    }

    pub fn with_topology(coordinates: ThreadCoordinates, topology: ThreadTopology) -> Self {
        Self::with_topology_and_metadata(coordinates, topology, std::collections::BTreeMap::new())
    }

    pub fn with_topology_and_metadata(
        coordinates: ThreadCoordinates,
        topology: ThreadTopology,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> Self {
        Self {
            parent_thread_id: topology.compatibility_parent_thread_id(),
            coordinates,
            topology,
            metadata,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TurnBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_rounds: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_text_bytes: Option<usize>,
}

impl TurnBudget {
    pub fn is_empty(&self) -> bool {
        self.max_tool_rounds.is_none()
            && self.max_output_tokens.is_none()
            && self.max_context_text_bytes.is_none()
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ThreadLifecycleStatus {
    Starting,
    Idle,
    Running,
    Cancelling,
    Stopped,
    Failed,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ThreadSignalKind {
    InterruptCancel,
    Shutdown,
    UserQueue,
    UserSteer,
    UserInterrupt,
    CheckpointRequested,
    CheckpointCreated,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadSignal {
    pub id: ThreadSignalId,
    pub coordinates: ThreadCoordinates,
    pub kind: ThreadSignalKind,
    pub metadata: std::collections::BTreeMap<String, String>,
    pub created_at_ms: u64,
}

impl ThreadSignal {
    pub fn new(coordinates: ThreadCoordinates, kind: ThreadSignalKind) -> Self {
        Self {
            id: ThreadSignalId::new(),
            coordinates,
            kind,
            metadata: std::collections::BTreeMap::new(),
            created_at_ms: unix_timestamp_ms(),
        }
    }

    pub fn user_queue(coordinates: &ThreadCoordinates, turn_id: impl Into<String>) -> Self {
        let mut signal = Self::new(coordinates.clone(), ThreadSignalKind::UserQueue);
        signal
            .metadata
            .insert("turn_id".to_string(), turn_id.into());
        signal
    }

    pub fn user_steer(coordinates: &ThreadCoordinates, turn_id: impl Into<String>) -> Self {
        let mut signal = Self::new(coordinates.clone(), ThreadSignalKind::UserSteer);
        signal
            .metadata
            .insert("turn_id".to_string(), turn_id.into());
        signal
    }

    pub fn user_interrupt(coordinates: &ThreadCoordinates, turn_id: impl Into<String>) -> Self {
        let mut signal = Self::new(coordinates.clone(), ThreadSignalKind::UserInterrupt);
        signal
            .metadata
            .insert("turn_id".to_string(), turn_id.into());
        signal
    }

    pub fn user_submit(
        coordinates: &ThreadCoordinates,
        turn_id: impl Into<String>,
        mode: TurnSubmissionMode,
    ) -> Self {
        match mode {
            TurnSubmissionMode::Queue => Self::user_queue(coordinates, turn_id),
            TurnSubmissionMode::Steer => Self::user_steer(coordinates, turn_id),
            TurnSubmissionMode::Interrupt => Self::user_interrupt(coordinates, turn_id),
        }
    }

    pub fn interrupt_cancel(coordinates: &ThreadCoordinates, reason: impl Into<String>) -> Self {
        let mut signal = Self::new(coordinates.clone(), ThreadSignalKind::InterruptCancel);
        signal.metadata.insert("reason".to_string(), reason.into());
        signal
    }

    pub fn shutdown(coordinates: &ThreadCoordinates) -> Self {
        Self::new(coordinates.clone(), ThreadSignalKind::Shutdown)
    }

    pub fn failed(coordinates: &ThreadCoordinates, message: impl Into<String>) -> Self {
        let mut signal = Self::new(coordinates.clone(), ThreadSignalKind::Failed);
        signal
            .metadata
            .insert("message".to_string(), message.into());
        signal
    }

    pub fn with_metadata(mut self, metadata: std::collections::BTreeMap<String, String>) -> Self {
        self.metadata.extend(metadata);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadLifecycleRecord {
    pub coordinates: ThreadCoordinates,
    pub parent_thread_id: Option<ThreadId>,
    #[serde(default)]
    pub topology: ThreadTopology,
    pub status: ThreadLifecycleStatus,
    pub latest_signal_id: Option<ThreadSignalId>,
    pub latest_checkpoint_id: Option<ThreadCheckpointId>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl ThreadLifecycleRecord {
    pub fn new(
        context: &ThreadContext,
        status: ThreadLifecycleStatus,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> Self {
        let now = unix_timestamp_ms();
        Self {
            coordinates: context.coordinates.clone(),
            parent_thread_id: context.parent_thread_id,
            topology: context.topology.clone(),
            status,
            latest_signal_id: None,
            latest_checkpoint_id: None,
            created_at_ms: now,
            updated_at_ms: now,
            metadata,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTerminalState {
    Completed,
    Cancelled,
    Stopped,
    Failed,
    TimedOut,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RuntimeModelRequestMode {
    Complete,
    Stream,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RuntimeModelRequestPurpose {
    Turn,
    Compaction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeModelRequestErrorClass {
    Cancelled,
    Fatal,
    Retryable,
    RateLimited,
    UnsupportedCapability,
    StreamAssembly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePermissionDecision {
    Allow,
    Deny,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeApprovalDecision {
    Approved,
    Denied,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeToolLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadInteractionKind {
    PromptSubmitted,
    PromptReceived,
    ResultAttached,
    ControlRequested,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TurnSubmissionMode {
    #[default]
    Queue,
    Steer,
    Interrupt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadStatus {
    Starting,
    Idle,
    Running,
    Cancelling,
    Stopped,
    Failed,
}

impl From<ThreadStatus> for ThreadLifecycleStatus {
    fn from(status: ThreadStatus) -> Self {
        match status {
            ThreadStatus::Starting => Self::Starting,
            ThreadStatus::Idle => Self::Idle,
            ThreadStatus::Running => Self::Running,
            ThreadStatus::Cancelling => Self::Cancelling,
            ThreadStatus::Stopped => Self::Stopped,
            ThreadStatus::Failed => Self::Failed,
        }
    }
}

fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {

    #[test]
    fn topology_serializes_existing_snake_case_shape() {
        let source_thread_id =
            crate::ThreadId::parse_str("018f9fe0-35a7-7a80-8f65-12e7e0b20b52").unwrap();
        let topology = crate::ThreadTopology::spawned_from(source_thread_id);

        let json = serde_json::to_value(topology).unwrap();

        assert_eq!(json["initiation"]["type"], "thread");
        assert_eq!(
            json["initiation"]["thread_id"],
            "018f9fe0-35a7-7a80-8f65-12e7e0b20b52"
        );
        assert_eq!(json["lineage"]["type"], "root");
        assert_eq!(
            json["spawn_attribution"]["source_thread_id"],
            "018f9fe0-35a7-7a80-8f65-12e7e0b20b52"
        );
        assert_eq!(
            json["controller_thread_id"],
            "018f9fe0-35a7-7a80-8f65-12e7e0b20b52"
        );
    }

    /// These strings are persisted in the metadata store and on the wire, so a
    /// variant rename must not silently change them.
    #[test]
    fn enum_strings_match_persisted_values() {
        assert_eq!(crate::ThreadLifecycleStatus::Starting.as_ref(), "starting");
        assert_eq!(crate::ThreadLifecycleStatus::Idle.as_ref(), "idle");
        assert_eq!(crate::ThreadLifecycleStatus::Running.as_ref(), "running");
        assert_eq!(
            crate::ThreadLifecycleStatus::Cancelling.as_ref(),
            "cancelling"
        );
        assert_eq!(crate::ThreadLifecycleStatus::Stopped.as_ref(), "stopped");
        assert_eq!(crate::ThreadLifecycleStatus::Failed.as_ref(), "failed");

        assert_eq!(
            crate::ThreadSignalKind::InterruptCancel.as_ref(),
            "interrupt_cancel"
        );
        assert_eq!(crate::ThreadSignalKind::Shutdown.as_ref(), "shutdown");
        assert_eq!(crate::ThreadSignalKind::UserQueue.as_ref(), "user_queue");
        assert_eq!(crate::ThreadSignalKind::UserSteer.as_ref(), "user_steer");
        assert_eq!(
            crate::ThreadSignalKind::UserInterrupt.as_ref(),
            "user_interrupt"
        );
        assert_eq!(
            crate::ThreadSignalKind::CheckpointRequested.as_ref(),
            "checkpoint_requested"
        );
        assert_eq!(
            crate::ThreadSignalKind::CheckpointCreated.as_ref(),
            "checkpoint_created"
        );
        assert_eq!(crate::ThreadSignalKind::Failed.as_ref(), "failed");

        assert_eq!(
            crate::RuntimeModelRequestMode::Complete.as_ref(),
            "complete"
        );
        assert_eq!(crate::RuntimeModelRequestMode::Stream.as_ref(), "stream");

        assert_eq!(crate::RuntimeModelRequestPurpose::Turn.as_ref(), "turn");
        assert_eq!(
            crate::RuntimeModelRequestPurpose::Compaction.as_ref(),
            "compaction"
        );

        assert_eq!(crate::TurnSubmissionMode::Queue.as_ref(), "queue");
        assert_eq!(crate::TurnSubmissionMode::Steer.as_ref(), "steer");
        assert_eq!(crate::TurnSubmissionMode::Interrupt.as_ref(), "interrupt");
    }

    #[test]
    fn status_conversion_stays_stable() {
        assert_eq!(
            crate::ThreadLifecycleStatus::from(crate::ThreadStatus::Cancelling),
            crate::ThreadLifecycleStatus::Cancelling
        );
    }
}
