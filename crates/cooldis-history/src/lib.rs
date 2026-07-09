use async_trait::async_trait;
use cooldis_runtime_contracts::{
    JsonSchemaValidationError, SchemaRegistry, ThreadCheckpointId, ThreadCoordinates, ThreadId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

pub type HistoryResult<T> = Result<T, HistoryError>;

pub const STREAM_RECORD_SCHEMA_V1: &str = "cooldis.stream.record/1";
pub const STREAM_CURSOR_SCHEMA_V1: &str = "cooldis.stream.cursor/1";
pub const STREAM_BACKEND_CAPABILITIES_SCHEMA_V1: &str = "cooldis.stream.backend_capabilities/1";
pub const STREAM_APPEND_ACK_SCHEMA_V1: &str = "cooldis.stream.append_ack/1";
pub const STREAM_ROUTING_DECISION_SCHEMA_V1: &str = "cooldis.stream.routing_decision/1";
pub const CONTEXT_READ_PLAN_SCHEMA_V1: &str = "cooldis.context.read_plan/1";
pub const DEBUG_THREAD_EXPORT_SCHEMA_V1: &str = "cooldis.debug.thread_export/1";
pub const EVENT_KIND_SCHEMA_VERSION: &str = "cooldis.events/0.2";

pub const COMPACTION_SUMMARY_PREFIX: &str = "Compacted conversation summary:";

pub fn render_compaction_summary(summary: &str) -> String {
    let summary = summary.trim();
    if summary.is_empty() {
        format!("{COMPACTION_SUMMARY_PREFIX}\n(no summary available)")
    } else if summary.starts_with(COMPACTION_SUMMARY_PREFIX) {
        summary.to_string()
    } else {
        format!("{COMPACTION_SUMMARY_PREFIX}\n{summary}")
    }
}

pub fn compaction_summary_message(summary: &str) -> CanonicalMessage {
    CanonicalMessage::user_text(render_compaction_summary(summary))
}

#[derive(Debug, Error)]
pub enum HistoryError {
    #[error("session entry not found: {0}")]
    EntryNotFound(SessionEntryId),
    #[error(
        "session entry {entry_id} belongs to thread {actual_thread_id}, not {requested_thread_id}"
    )]
    EntryThreadMismatch {
        entry_id: SessionEntryId,
        requested_thread_id: ThreadId,
        actual_thread_id: ThreadId,
    },
    #[error("thread history belongs to {actual:?}, not {requested:?}")]
    ThreadScopeMismatch {
        requested: Box<ThreadCoordinates>,
        actual: Box<ThreadCoordinates>,
    },
    #[error(
        "thread base for child {child_thread_id} would create a cycle through {ancestor_thread_id}"
    )]
    ThreadBaseCycle {
        child_thread_id: ThreadId,
        ancestor_thread_id: ThreadId,
    },
    #[error("history storage failed: {0}")]
    Storage(String),
    #[error("history codec failed: {0}")]
    Codec(String),
    /// A discharged event reached an append path without provenance.
    #[error("discharged event {0} has no provenance")]
    DischargedWithoutProvenance(EventRecordId),
    #[error("stream cursor targets {cursor_stream_id}, not requested stream {requested_stream_id}")]
    StreamCursorStreamMismatch {
        cursor_stream_id: EventStreamId,
        requested_stream_id: EventStreamId,
    },
    #[error(
        "stream cursor for {stream_id} at sequence {sequence} expected event {expected_event_id}, found sequence {actual_sequence:?} event {actual_event_id:?}"
    )]
    StreamCursorMismatch {
        stream_id: EventStreamId,
        sequence: i64,
        expected_event_id: EventRecordId,
        actual_sequence: Option<i64>,
        actual_event_id: Option<EventRecordId>,
    },
    #[error(
        "fenced append to {stream_id} expected next sequence {expected_next_sequence}, stream is at {actual_next_sequence}"
    )]
    AppendFenceConflict {
        stream_id: EventStreamId,
        expected_next_sequence: i64,
        actual_next_sequence: i64,
    },
    #[error("this event store does not support fenced appends")]
    FencedAppendUnsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SessionEntryId(Uuid);

impl SessionEntryId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for SessionEntryId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct EventStreamId(String);

impl EventStreamId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn for_thread(coordinates: &ThreadCoordinates) -> Self {
        Self(format!("thread:{}", coordinates.thread_id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EventStreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct EventSequence(i64);

impl EventSequence {
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct EventRecordId(Uuid);

impl EventRecordId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for EventRecordId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for EventRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Frozen event-kind vocabulary, version `cooldis.events/0.2`.
///
/// Laws:
/// - The vocabulary is append-only: kinds may be added in later versions,
///   but an existing kind's string and semantics are frozen forever.
/// - Parsing is fail-closed: an unknown kind string is an error, never a
///   passthrough. There is no `Other` variant by design.
/// - Event kinds are the trigger addressing scheme for every future
///   propagator and controller; renaming one would make receipts lie.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum EventKind {
    /// A session entry was appended to the thread stream.
    SessionEntryAppended,
    /// Context assembly completed; the payload is the assembly receipt.
    ContextCompileCompleted,
    /// A context summarizer discharged a summary checkpoint event. The compacted
    /// text lives in the payload; provenance records the summarizer boundary.
    ContextSummaryCompleted,
    /// A context controller selected a named read plan for future assembly.
    ContextReadPlanSet,
    /// A manifest compiled to a resolved plan; the payload is the compile
    /// receipt. Emitted by the WS-A compile/bind layer.
    ManifestCompileCompleted,
    /// An agent ref resolved through aliases to a manifest hash and bound;
    /// the payload is the bind receipt. Emitted at publish and at run time.
    ManifestBindCompleted,
    /// A tool universe's contracts were witnessed (at bind or on demand);
    /// the payload is `ToolUniverseDiscoveryReceipt` — server ref, discovery
    /// hash, and per-tool schema hashes. Witnessed origin: the contracts
    /// arrived from outside the system.
    ToolUniverseDiscoveryCompleted,
    /// One `tool.call` against a live universe completed; the payload is
    /// `ToolUniverseCallReceipt` — server ref, tool name, the schema hash
    /// the arguments were validated against, and the output hash.
    ToolUniverseCallCompleted,
    /// A model/tool surface requested one tool call.
    ToolCallRequested,
    /// A controller suspended a tool call pending later control input.
    ToolCallSuspended,
    /// A controller decided how a pending tool call should proceed.
    ToolCallDecision,
    /// A tool executor observed a completed tool invocation.
    ToolCallCompleted,
    /// A turn submission entered the thread.
    TurnSubmitted,
    /// A turn is waiting on a durable control fact.
    TurnWaiting,
    /// A previously waiting turn resumed.
    TurnResumed,
    /// A turn reached quiescence.
    TurnCompleted,
    /// A controller requested external approval.
    ApprovalRequested,
    /// An approved external surface witnessed an approval decision.
    ApprovalResolved,
    /// An external grantor started a standing activation mandate.
    MandateStarted,
    /// An external grantor revoked a standing activation mandate.
    MandateRevoked,
    /// A controller requested another turn.
    TurnContinueRequested,
    /// The scheduler accepted a continuation request.
    TurnContinuationAccepted,
    /// The scheduler rejected a continuation request.
    TurnContinuationRejected,
    /// A loop completed successfully.
    LoopCompleted,
    /// A loop stopped because it is blocked.
    LoopBlocked,
    /// A loop stopped because its budget is exhausted.
    LoopBudgetExhausted,
    /// A loop stopped because continuation was denied.
    LoopDenied,
    /// A coupling activation completed and emitted its run receipt.
    CouplingRunCompleted,
    /// A coupling activation failed and emitted its run receipt.
    CouplingRunFailed,
    /// A placement controller selected where execution should run.
    PlacementDecision,
    /// A coupling proposed spawning supervised child work. A durable
    /// projector consumes the request, performs the spawn through the
    /// thread/turn kernel package, and the kernel witnesses `thread.spawned`
    /// — the same requested/projector grammar as IO egress.
    ThreadSpawnRequested,
    /// A parent thread spawned a child thread with the recorded manifest,
    /// policy, grants, and input digest.
    ThreadSpawned,
    /// A spawned child thread reached a terminal state and joined back to its
    /// parent lineage.
    ThreadJoined,
    /// A policy identity became active. The binding is valid until the next
    /// `policy.bound` with the same `policy_id`.
    PolicyBound,
    /// A thread petitioned for additional grants. Resolution is recorded with
    /// the existing approval event pair.
    GrantPetitioned,
    /// A standing mandate produced a clock occurrence.
    TimerFired,
    /// An external IO route received an ingress envelope.
    IoIngressReceived,
    /// A tool path requested an IO egress action for later projection.
    IoEgressRequested,
    /// An IO egress attempt was delivered to the external route.
    IoEgressDelivered,
    /// An IO egress attempt failed and may have been dead-lettered.
    IoEgressFailed,
    /// An admission policy chose how to handle one or more ingress events.
    AdmissionDecided,
}

impl EventKind {
    pub const fn all() -> &'static [EventKind] {
        &[
            Self::SessionEntryAppended,
            Self::ContextCompileCompleted,
            Self::ContextSummaryCompleted,
            Self::ContextReadPlanSet,
            Self::ManifestCompileCompleted,
            Self::ManifestBindCompleted,
            Self::ToolUniverseDiscoveryCompleted,
            Self::ToolUniverseCallCompleted,
            Self::ToolCallRequested,
            Self::ToolCallSuspended,
            Self::ToolCallDecision,
            Self::ToolCallCompleted,
            Self::TurnSubmitted,
            Self::TurnWaiting,
            Self::TurnResumed,
            Self::TurnCompleted,
            Self::ApprovalRequested,
            Self::ApprovalResolved,
            Self::MandateStarted,
            Self::MandateRevoked,
            Self::TurnContinueRequested,
            Self::TurnContinuationAccepted,
            Self::TurnContinuationRejected,
            Self::LoopCompleted,
            Self::LoopBlocked,
            Self::LoopBudgetExhausted,
            Self::LoopDenied,
            Self::CouplingRunCompleted,
            Self::CouplingRunFailed,
            Self::PlacementDecision,
            Self::ThreadSpawnRequested,
            Self::ThreadSpawned,
            Self::ThreadJoined,
            Self::PolicyBound,
            Self::GrantPetitioned,
            Self::TimerFired,
            Self::IoIngressReceived,
            Self::IoEgressRequested,
            Self::IoEgressDelivered,
            Self::IoEgressFailed,
            Self::AdmissionDecided,
        ]
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::SessionEntryAppended => "session.entry.appended",
            Self::ContextCompileCompleted => "context.compile.completed",
            Self::ContextSummaryCompleted => "context.summary.completed",
            Self::ContextReadPlanSet => "context.read_plan.set",
            Self::ManifestCompileCompleted => "manifest.compile.completed",
            Self::ManifestBindCompleted => "manifest.bind.completed",
            Self::ToolUniverseDiscoveryCompleted => "tool.universe.discovery.completed",
            Self::ToolUniverseCallCompleted => "tool.universe.call.completed",
            Self::ToolCallRequested => "tool.call.requested",
            Self::ToolCallSuspended => "tool.call.suspended",
            Self::ToolCallDecision => "tool.call.decision",
            Self::ToolCallCompleted => "tool.call.completed",
            Self::TurnSubmitted => "turn.submitted",
            Self::TurnWaiting => "turn.waiting",
            Self::TurnResumed => "turn.resumed",
            Self::TurnCompleted => "turn.completed",
            Self::ApprovalRequested => "approval.requested",
            Self::ApprovalResolved => "approval.resolved",
            Self::MandateStarted => "mandate.started",
            Self::MandateRevoked => "mandate.revoked",
            Self::TurnContinueRequested => "turn.continue.requested",
            Self::TurnContinuationAccepted => "turn.continuation.accepted",
            Self::TurnContinuationRejected => "turn.continuation.rejected",
            Self::LoopCompleted => "loop.completed",
            Self::LoopBlocked => "loop.blocked",
            Self::LoopBudgetExhausted => "loop.budget_exhausted",
            Self::LoopDenied => "loop.denied",
            Self::CouplingRunCompleted => "coupling.run.completed",
            Self::CouplingRunFailed => "coupling.run.failed",
            Self::PlacementDecision => "placement.decision",
            Self::ThreadSpawnRequested => "thread.spawn.requested",
            Self::ThreadSpawned => "thread.spawned",
            Self::ThreadJoined => "thread.joined",
            Self::PolicyBound => "policy.bound",
            Self::GrantPetitioned => "grant.petitioned",
            Self::TimerFired => "timer.fired",
            Self::IoIngressReceived => "io.ingress.received",
            Self::IoEgressRequested => "io.egress.requested",
            Self::IoEgressDelivered => "io.egress.delivered",
            Self::IoEgressFailed => "io.egress.failed",
            Self::AdmissionDecided => "admission.decided",
        }
    }

    pub fn payload_schema_id(self) -> &'static str {
        match self {
            Self::SessionEntryAppended => "cooldis.event.session.entry.appended/1",
            Self::ContextCompileCompleted => "cooldis.event.context.compile.completed/1",
            Self::ContextSummaryCompleted => "cooldis.event.context.summary.completed/1",
            Self::ContextReadPlanSet => "cooldis.event.context.read_plan.set/1",
            Self::ManifestCompileCompleted => "cooldis.event.manifest.compile.completed/1",
            Self::ManifestBindCompleted => "cooldis.event.manifest.bind.completed/1",
            Self::ToolUniverseDiscoveryCompleted => {
                "cooldis.event.tool.universe.discovery.completed/1"
            }
            Self::ToolUniverseCallCompleted => "cooldis.event.tool.universe.call.completed/1",
            Self::ToolCallRequested => "cooldis.event.tool.call.requested/1",
            Self::ToolCallSuspended => "cooldis.event.tool.call.suspended/1",
            Self::ToolCallDecision => "cooldis.event.tool.call.decision/1",
            Self::ToolCallCompleted => "cooldis.event.tool.call.completed/1",
            Self::TurnSubmitted => "cooldis.event.turn.submitted/1",
            Self::TurnWaiting => "cooldis.event.turn.waiting/1",
            Self::TurnResumed => "cooldis.event.turn.resumed/1",
            Self::TurnCompleted => "cooldis.event.turn.completed/1",
            Self::ApprovalRequested => "cooldis.event.approval.requested/1",
            Self::ApprovalResolved => "cooldis.event.approval.resolved/1",
            Self::MandateStarted => "cooldis.event.mandate.started/1",
            Self::MandateRevoked => "cooldis.event.mandate.revoked/1",
            Self::TurnContinueRequested => "cooldis.event.turn.continue.requested/1",
            Self::TurnContinuationAccepted => "cooldis.event.turn.continuation.accepted/1",
            Self::TurnContinuationRejected => "cooldis.event.turn.continuation.rejected/1",
            Self::LoopCompleted => "cooldis.event.loop.completed/1",
            Self::LoopBlocked => "cooldis.event.loop.blocked/1",
            Self::LoopBudgetExhausted => "cooldis.event.loop.budget_exhausted/1",
            Self::LoopDenied => "cooldis.event.loop.denied/1",
            Self::CouplingRunCompleted => "cooldis.event.coupling.run.completed/1",
            Self::CouplingRunFailed => "cooldis.event.coupling.run.failed/1",
            Self::PlacementDecision => "cooldis.event.placement.decision/1",
            Self::ThreadSpawnRequested => "cooldis.event.thread.spawn.requested/1",
            Self::ThreadSpawned => "cooldis.event.thread.spawned/1",
            Self::ThreadJoined => "cooldis.event.thread.joined/1",
            Self::PolicyBound => "cooldis.event.policy.bound/1",
            Self::GrantPetitioned => "cooldis.event.grant.petitioned/1",
            Self::TimerFired => "cooldis.event.timer.fired/1",
            Self::IoIngressReceived => "cooldis.event.io.ingress.received/1",
            Self::IoEgressRequested => "cooldis.event.io.egress.requested/1",
            Self::IoEgressDelivered => "cooldis.event.io.egress.delivered/1",
            Self::IoEgressFailed => "cooldis.event.io.egress.failed/1",
            Self::AdmissionDecided => "cooldis.event.admission.decided/1",
        }
    }
}

impl std::str::FromStr for EventKind {
    type Err = HistoryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "session.entry.appended" => Ok(Self::SessionEntryAppended),
            "context.compile.completed" => Ok(Self::ContextCompileCompleted),
            "context.summary.completed" => Ok(Self::ContextSummaryCompleted),
            "context.read_plan.set" => Ok(Self::ContextReadPlanSet),
            "manifest.compile.completed" => Ok(Self::ManifestCompileCompleted),
            "manifest.bind.completed" => Ok(Self::ManifestBindCompleted),
            "tool.universe.discovery.completed" => Ok(Self::ToolUniverseDiscoveryCompleted),
            "tool.universe.call.completed" => Ok(Self::ToolUniverseCallCompleted),
            "tool.call.requested" => Ok(Self::ToolCallRequested),
            "tool.call.suspended" => Ok(Self::ToolCallSuspended),
            "tool.call.decision" => Ok(Self::ToolCallDecision),
            "tool.call.completed" => Ok(Self::ToolCallCompleted),
            "turn.submitted" => Ok(Self::TurnSubmitted),
            "turn.waiting" => Ok(Self::TurnWaiting),
            "turn.resumed" => Ok(Self::TurnResumed),
            "turn.completed" => Ok(Self::TurnCompleted),
            "approval.requested" => Ok(Self::ApprovalRequested),
            "approval.resolved" => Ok(Self::ApprovalResolved),
            "mandate.started" => Ok(Self::MandateStarted),
            "mandate.revoked" => Ok(Self::MandateRevoked),
            "turn.continue.requested" => Ok(Self::TurnContinueRequested),
            "turn.continuation.accepted" => Ok(Self::TurnContinuationAccepted),
            "turn.continuation.rejected" => Ok(Self::TurnContinuationRejected),
            "loop.completed" => Ok(Self::LoopCompleted),
            "loop.blocked" => Ok(Self::LoopBlocked),
            "loop.budget_exhausted" => Ok(Self::LoopBudgetExhausted),
            "loop.denied" => Ok(Self::LoopDenied),
            "coupling.run.completed" => Ok(Self::CouplingRunCompleted),
            "coupling.run.failed" => Ok(Self::CouplingRunFailed),
            "placement.decision" => Ok(Self::PlacementDecision),
            "thread.spawn.requested" => Ok(Self::ThreadSpawnRequested),
            "thread.spawned" => Ok(Self::ThreadSpawned),
            "thread.joined" => Ok(Self::ThreadJoined),
            "policy.bound" => Ok(Self::PolicyBound),
            "grant.petitioned" => Ok(Self::GrantPetitioned),
            "timer.fired" => Ok(Self::TimerFired),
            "io.ingress.received" => Ok(Self::IoIngressReceived),
            "io.egress.requested" => Ok(Self::IoEgressRequested),
            "io.egress.delivered" => Ok(Self::IoEgressDelivered),
            "io.egress.failed" => Ok(Self::IoEgressFailed),
            "admission.decided" => Ok(Self::AdmissionDecided),
            other => Err(HistoryError::Codec(format!("unknown event kind: {other}"))),
        }
    }
}

impl TryFrom<String> for EventKind {
    type Error = HistoryError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<EventKind> for String {
    fn from(kind: EventKind) -> Self {
        kind.as_str().to_string()
    }
}

impl std::fmt::Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Terminal child-thread states recorded by `thread.joined`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadTerminalState {
    Completed,
    Failed,
    Cancelled,
    BudgetExhausted,
}

/// Policy identities bound into the event stream. `Other` is a policy-kind
/// extension point inside the payload, not a catch-all event kind.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyKind {
    AdmissionRoute,
    CouplingSet,
    Orchestrator,
    Other(String),
}

/// Admission decisions a route policy can choose for an ingress batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionDecision {
    Queue,
    Steer,
    Interrupt,
    Fork,
    Observe,
    Reject,
    Coalesce,
}

/// Payload of `thread.spawn.requested`: a coupling's proposal to spawn
/// supervised child work. The projector that consumes it performs the spawn
/// under the parent's `threads.spawn` grant and `allow_child_agents` policy —
/// the coupling route grants no authority of its own.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadSpawnRequestedPayload {
    pub parent_thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_turn_id: Option<String>,
    /// Registry ref of the agent manifest the child runs under.
    pub child_agent_ref: String,
    /// The child's first turn input.
    pub initial_submission: String,
    /// Joins this request to the resulting `thread.spawned` and to the
    /// supervisor's completion fold.
    pub correlation_id: String,
    /// When true the supervisor also discharges `turn.waiting` for the
    /// parent, which resumes on the child-completion fold.
    #[serde(default)]
    pub block_parent: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadSpawnedPayload {
    pub parent_thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_turn_id: Option<String>,
    pub child_thread_id: ThreadId,
    pub child_manifest_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_policy_hash: Option<String>,
    /// Serialized grant set as recorded at spawn.
    pub granted: Vec<String>,
    pub inputs_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<ThreadSpawnedForkPayload>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadSpawnedForkPayload {
    pub mode: String,
    #[serde(rename = "sourceCut")]
    pub source_cut: ThreadSpawnedForkSourceCutPayload,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSpawnedForkSourceCutPayload {
    pub thread_id: ThreadId,
    pub checkpoint_id: ThreadCheckpointId,
    pub leaf_entry_id: Option<SessionEntryId>,
    pub stream_id: EventStreamId,
    pub stream_to_sequence: Option<EventSequence>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadJoinedPayload {
    pub child_thread_id: ThreadId,
    pub spawned_event_id: EventRecordId,
    pub terminal_state: ThreadTerminalState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_digest: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyBoundPayload {
    pub policy_kind: PolicyKind,
    pub policy_id: String,
    pub content_hash: String,
    /// "Valid until next policy.bound of same policy_id" semantics.
    pub valid_from_note: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrantPetitionedPayload {
    pub thread_id: ThreadId,
    pub requested: Vec<String>,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_event_ids: Option<Vec<EventRecordId>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimerFiredPayload {
    pub mandate_event_id: EventRecordId,
    pub scheduled_for: String,
    pub occurrence_index: u64,
    pub catch_up: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IoIngressReceivedPayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_conversation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_actor_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_message_id: Option<String>,
    pub envelope_digest: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IoEgressRequestedPayload {
    pub egress_kind: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_target: Option<Value>,
    pub requested_by_tool_call_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_event_id: Option<EventRecordId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IoEgressDeliveredPayload {
    pub route_id: String,
    pub egress_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_message_id: Option<String>,
    pub attempts: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IoEgressFailedPayload {
    pub route_id: String,
    pub egress_kind: String,
    pub attempts: u32,
    pub error_class: String,
    pub dead_lettered: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdmissionDecidedPayload {
    pub route_id: String,
    pub policy_hash: String,
    pub decision: AdmissionDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admissible: Option<Vec<AdmissionDecision>>,
    pub source_ingress_event_ids: Vec<EventRecordId>,
}

/// Where an event came from, relative to the system boundary.
///
/// Laws:
/// - `Witnessed`: the event records something that arrived from outside the
///   system (for example a user-authored session entry). It has no upstream
///   events inside the system.
/// - `Discharged`: the event was produced by a coupling (propagator,
///   projection, or controller). A discharged event MUST carry non-empty
///   provenance; appending one without provenance is an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventOrigin {
    Witnessed,
    Discharged,
}

impl EventOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Witnessed => "witnessed",
            Self::Discharged => "discharged",
        }
    }
}

/// Provenance for a discharged event: which coupling produced it, from what
/// upstream records. Empty provenance is only legal on witnessed events.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventProvenance {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_streams: Vec<EventStreamId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_event_ids: Vec<EventRecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_range: Option<ObservationSourceRange>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_ranges: Vec<ObservationSourceRange>,
    /// Identity of the coupling that discharged this event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discharged_by: Option<String>,
    /// Function (and version) the coupling ran to produce the discharge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    /// Hash of the configuration that parameterized the discharge function.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_hash: Option<String>,
}

impl EventProvenance {
    pub fn is_empty(&self) -> bool {
        self.source_streams.is_empty()
            && self.source_event_ids.is_empty()
            && self.source_range.is_none()
            && self.source_ranges.is_empty()
            && self.discharged_by.is_none()
            && self.function.is_none()
            && self.config_hash.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewEventRecord {
    pub id: EventRecordId,
    pub coordinates: ThreadCoordinates,
    pub created_at_ms: i64,
    pub kind: EventKind,
    pub origin: EventOrigin,
    #[serde(default)]
    pub provenance: EventProvenance,
    pub payload: Value,
}

impl NewEventRecord {
    /// A witnessed event: arrived from outside the system, no provenance.
    pub fn witnessed(coordinates: ThreadCoordinates, kind: EventKind, payload: Value) -> Self {
        Self {
            id: EventRecordId::new(),
            coordinates,
            created_at_ms: now_ms(),
            kind,
            origin: EventOrigin::Witnessed,
            provenance: EventProvenance::default(),
            payload,
        }
    }

    /// A discharged event: produced by a coupling. Provenance is required;
    /// the append path rejects discharged events with empty provenance.
    pub fn discharged(
        coordinates: ThreadCoordinates,
        kind: EventKind,
        payload: Value,
        provenance: EventProvenance,
    ) -> Self {
        Self {
            id: EventRecordId::new(),
            coordinates,
            created_at_ms: now_ms(),
            kind,
            origin: EventOrigin::Discharged,
            provenance,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: EventRecordId,
    pub stream_id: EventStreamId,
    pub sequence: EventSequence,
    pub coordinates: ThreadCoordinates,
    pub created_at_ms: i64,
    pub kind: EventKind,
    pub origin: EventOrigin,
    #[serde(default)]
    pub provenance: EventProvenance,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StreamRecordEnvelopeV1 {
    pub schema: String,
    pub event_id: EventRecordId,
    pub stream_id: EventStreamId,
    pub sequence: EventSequence,
    pub coordinates: ThreadCoordinates,
    pub created_at_ms: i64,
    pub kind: String,
    pub origin: EventOrigin,
    pub payload_schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_context: Option<Value>,
    #[serde(default)]
    pub provenance: EventProvenance,
    pub payload: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamAckClass {
    LocalCommitted,
    QueryProjected,
    StreamCommitted,
    BroadcastVisible,
    Archived,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamBackendKindV1 {
    Sqlite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamStorageScopeV1 {
    LocalEmbedded,
    RemoteDurable,
    Hybrid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamBackendCapabilitiesV1 {
    pub schema: String,
    pub backend_kind: StreamBackendKindV1,
    pub storage_scope: StreamStorageScopeV1,
    pub ack_classes: Vec<StreamAckClass>,
    pub supports_atomic_batch_append: bool,
    pub supports_verified_cursor_replay: bool,
    pub supports_query_projection: bool,
    pub supports_expected_tail: bool,
    pub supports_fencing_tokens: bool,
    pub supports_live_follow: bool,
    pub supports_broadcast: bool,
    pub supports_cold_archive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
}

impl StreamBackendCapabilitiesV1 {
    pub fn sqlite_local(local_path: impl Into<String>) -> Self {
        Self {
            schema: STREAM_BACKEND_CAPABILITIES_SCHEMA_V1.to_string(),
            backend_kind: StreamBackendKindV1::Sqlite,
            storage_scope: StreamStorageScopeV1::LocalEmbedded,
            ack_classes: vec![
                StreamAckClass::LocalCommitted,
                StreamAckClass::QueryProjected,
            ],
            supports_atomic_batch_append: true,
            supports_verified_cursor_replay: true,
            supports_query_projection: true,
            supports_expected_tail: false,
            supports_fencing_tokens: false,
            supports_live_follow: false,
            supports_broadcast: false,
            supports_cold_archive: false,
            local_path: Some(local_path.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamAppendAckV1 {
    pub schema: String,
    pub stream_id: EventStreamId,
    pub start_sequence: EventSequence,
    pub end_sequence: EventSequence,
    pub tail_sequence: EventSequence,
    pub tail_event_id: EventRecordId,
    pub acks: Vec<StreamAckClass>,
}

impl StreamAppendAckV1 {
    pub fn from_appended(
        stream_id: EventStreamId,
        appended: &[EventRecord],
        acks: Vec<StreamAckClass>,
    ) -> HistoryResult<Self> {
        let Some(first) = appended.first() else {
            return Err(HistoryError::Codec(
                "append ack requires at least one event".to_string(),
            ));
        };
        let Some(last) = appended.last() else {
            return Err(HistoryError::Codec(
                "append ack requires at least one event".to_string(),
            ));
        };
        if acks.is_empty() {
            return Err(HistoryError::Codec(
                "append ack requires at least one ack class".to_string(),
            ));
        }
        for (index, event) in appended.iter().enumerate() {
            if event.stream_id != stream_id {
                return Err(HistoryError::Codec(format!(
                    "append ack event {} belongs to stream {}, not {}",
                    event.id, event.stream_id, stream_id
                )));
            }
            let expected = first.sequence.get() + index as i64;
            if event.sequence.get() != expected {
                return Err(HistoryError::Codec(format!(
                    "append ack events must be contiguous: expected sequence {expected}, got {}",
                    event.sequence.get()
                )));
            }
        }
        Ok(Self {
            schema: STREAM_APPEND_ACK_SCHEMA_V1.to_string(),
            stream_id,
            start_sequence: first.sequence,
            end_sequence: last.sequence,
            tail_sequence: last.sequence,
            tail_event_id: last.id,
            acks,
        })
    }

    pub fn sqlite_local(stream_id: EventStreamId, appended: &[EventRecord]) -> HistoryResult<Self> {
        Self::from_appended(
            stream_id,
            appended,
            vec![
                StreamAckClass::LocalCommitted,
                StreamAckClass::QueryProjected,
            ],
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamRouteProfile {
    AuthorityStore,
    ExportBundle,
    ModelTrace,
    RuntimeTrace,
    BrowserSafeProjection,
    AnalyticsAggregate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamRoutingKeysV1 {
    pub schema: String,
    pub stream_id: EventStreamId,
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub thread_id: ThreadId,
    pub kind: String,
    pub origin: EventOrigin,
    pub payload_schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discharged_by: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamRoutingDecisionV1 {
    pub schema: String,
    pub event_id: EventRecordId,
    pub stream_id: EventStreamId,
    pub sequence: EventSequence,
    pub kind: String,
    pub routes: Vec<StreamRouteProfile>,
    pub keys: StreamRoutingKeysV1,
}

impl StreamRecordEnvelopeV1 {
    pub fn route_decision_v1(&self) -> StreamRoutingDecisionV1 {
        let kind = self.kind.parse::<EventKind>().ok();
        let mut routes = vec![
            StreamRouteProfile::AuthorityStore,
            StreamRouteProfile::ExportBundle,
        ];
        if kind.is_some_and(event_kind_routes_to_model_trace) {
            routes.push(StreamRouteProfile::ModelTrace);
        }
        if kind.is_some_and(event_kind_routes_to_runtime_trace) {
            routes.push(StreamRouteProfile::RuntimeTrace);
        }
        if stream_id_routes_to_browser_projection(&self.stream_id) {
            routes.push(StreamRouteProfile::BrowserSafeProjection);
        }
        if kind.is_some_and(event_kind_routes_to_analytics) {
            routes.push(StreamRouteProfile::AnalyticsAggregate);
        }

        StreamRoutingDecisionV1 {
            schema: STREAM_ROUTING_DECISION_SCHEMA_V1.to_string(),
            event_id: self.event_id,
            stream_id: self.stream_id.clone(),
            sequence: self.sequence,
            kind: self.kind.clone(),
            routes,
            keys: StreamRoutingKeysV1 {
                schema: self.schema.clone(),
                stream_id: self.stream_id.clone(),
                tenant_id: self.coordinates.tenant_id.clone(),
                user_id: self.coordinates.user_id.clone(),
                session_id: self.coordinates.session_id.clone(),
                thread_id: self.coordinates.thread_id,
                kind: self.kind.clone(),
                origin: self.origin,
                payload_schema: self.payload_schema.clone(),
                trace_id: trace_id_from_context(self.trace_context.as_ref()),
                discharged_by: self.provenance.discharged_by.clone(),
            },
        }
    }
}

impl EventRecord {
    pub fn from_new(
        stream_id: EventStreamId,
        sequence: EventSequence,
        record: NewEventRecord,
    ) -> Self {
        Self {
            id: record.id,
            stream_id,
            sequence,
            coordinates: record.coordinates,
            created_at_ms: record.created_at_ms,
            kind: record.kind,
            origin: record.origin,
            provenance: record.provenance,
            payload: record.payload,
        }
    }

    pub fn to_stream_record_v1(&self) -> StreamRecordEnvelopeV1 {
        StreamRecordEnvelopeV1 {
            schema: STREAM_RECORD_SCHEMA_V1.to_string(),
            event_id: self.id,
            stream_id: self.stream_id.clone(),
            sequence: self.sequence,
            coordinates: self.coordinates.clone(),
            created_at_ms: self.created_at_ms,
            kind: self.kind.as_str().to_string(),
            origin: self.origin,
            payload_schema: self.kind.payload_schema_id().to_string(),
            trace_context: None,
            provenance: self.provenance.clone(),
            payload: self.payload.clone(),
        }
    }

    pub fn cursor_v1(&self) -> StreamCursorV1 {
        StreamCursorV1::from_event(self)
    }

    pub fn route_decision_v1(&self) -> StreamRoutingDecisionV1 {
        self.to_stream_record_v1().route_decision_v1()
    }

    pub fn validate_stream_record_v1(&self) -> HistoryResult<()> {
        let envelope = serde_json::to_value(self.to_stream_record_v1())
            .map_err(|err| HistoryError::Codec(format!("encode stream record envelope: {err}")))?;
        let registry =
            stream_schema_registry_v1().map_err(|err| HistoryError::Codec(err.to_string()))?;
        registry
            .validate(STREAM_RECORD_SCHEMA_V1, &envelope)
            .map_err(|err| HistoryError::Codec(err.to_string()))?;
        if self.kind == EventKind::IoEgressRequested {
            registry
                .validate(self.kind.payload_schema_id(), &self.payload)
                .map_err(|err| HistoryError::Codec(err.to_string()))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamCursorV1 {
    pub schema: String,
    pub stream_id: EventStreamId,
    pub sequence: EventSequence,
    pub event_id: EventRecordId,
}

impl StreamCursorV1 {
    pub fn new(stream_id: EventStreamId, sequence: EventSequence, event_id: EventRecordId) -> Self {
        Self {
            schema: STREAM_CURSOR_SCHEMA_V1.to_string(),
            stream_id,
            sequence,
            event_id,
        }
    }

    pub fn from_event(event: &EventRecord) -> Self {
        Self::new(event.stream_id.clone(), event.sequence, event.id)
    }

    pub fn validate_stream_cursor_v1(&self) -> HistoryResult<()> {
        if self.schema != STREAM_CURSOR_SCHEMA_V1 {
            return Err(HistoryError::Codec(format!(
                "unsupported stream cursor schema {:?}",
                self.schema
            )));
        }
        if self.sequence.get() < 1 {
            return Err(HistoryError::Codec(format!(
                "stream cursor sequence must be positive, got {}",
                self.sequence.get()
            )));
        }
        let cursor = serde_json::to_value(self)
            .map_err(|err| HistoryError::Codec(format!("encode stream cursor envelope: {err}")))?;
        stream_schema_registry_v1()
            .map_err(|err| HistoryError::Codec(err.to_string()))?
            .validate(STREAM_CURSOR_SCHEMA_V1, &cursor)
            .map_err(|err| HistoryError::Codec(err.to_string()))
    }
}

pub fn stream_schema_registry_v1() -> Result<SchemaRegistry, JsonSchemaValidationError> {
    let mut registry = SchemaRegistry::new();
    registry.register(STREAM_RECORD_SCHEMA_V1, stream_record_schema_v1())?;
    registry.register(STREAM_CURSOR_SCHEMA_V1, stream_cursor_schema_v1())?;
    registry.register(
        STREAM_BACKEND_CAPABILITIES_SCHEMA_V1,
        stream_backend_capabilities_schema_v1(),
    )?;
    registry.register(STREAM_APPEND_ACK_SCHEMA_V1, stream_append_ack_schema_v1())?;
    registry.register(
        STREAM_ROUTING_DECISION_SCHEMA_V1,
        stream_routing_decision_schema_v1(),
    )?;
    registry.register(CONTEXT_READ_PLAN_SCHEMA_V1, context_read_plan_schema_v1())?;
    registry.register(
        DEBUG_THREAD_EXPORT_SCHEMA_V1,
        debug_thread_export_schema_v1(),
    )?;
    registry.register(
        EventKind::ContextCompileCompleted.payload_schema_id(),
        context_compile_completed_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::ContextSummaryCompleted.payload_schema_id(),
        context_summary_completed_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::ContextReadPlanSet.payload_schema_id(),
        context_read_plan_set_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::ThreadSpawnRequested.payload_schema_id(),
        thread_spawn_requested_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::ThreadSpawned.payload_schema_id(),
        thread_spawned_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::ThreadJoined.payload_schema_id(),
        thread_joined_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::PolicyBound.payload_schema_id(),
        policy_bound_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::GrantPetitioned.payload_schema_id(),
        grant_petitioned_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::TimerFired.payload_schema_id(),
        timer_fired_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::IoIngressReceived.payload_schema_id(),
        io_ingress_received_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::IoEgressRequested.payload_schema_id(),
        io_egress_requested_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::IoEgressDelivered.payload_schema_id(),
        io_egress_delivered_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::IoEgressFailed.payload_schema_id(),
        io_egress_failed_payload_schema_v1(),
    )?;
    registry.register(
        EventKind::AdmissionDecided.payload_schema_id(),
        admission_decided_payload_schema_v1(),
    )?;
    Ok(registry)
}

fn event_kind_routes_to_model_trace(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::ContextCompileCompleted
            | EventKind::ToolUniverseDiscoveryCompleted
            | EventKind::ToolUniverseCallCompleted
            | EventKind::ToolCallRequested
            | EventKind::ToolCallSuspended
            | EventKind::ToolCallDecision
            | EventKind::ToolCallCompleted
            | EventKind::TurnSubmitted
            | EventKind::TurnCompleted
    )
}

fn event_kind_routes_to_runtime_trace(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::ContextSummaryCompleted
            | EventKind::ContextReadPlanSet
            | EventKind::ManifestCompileCompleted
            | EventKind::ManifestBindCompleted
            | EventKind::TurnWaiting
            | EventKind::TurnResumed
            | EventKind::ApprovalRequested
            | EventKind::ApprovalResolved
            | EventKind::MandateStarted
            | EventKind::MandateRevoked
            | EventKind::TurnContinueRequested
            | EventKind::TurnContinuationAccepted
            | EventKind::TurnContinuationRejected
            | EventKind::LoopCompleted
            | EventKind::LoopBlocked
            | EventKind::LoopBudgetExhausted
            | EventKind::LoopDenied
            | EventKind::CouplingRunCompleted
            | EventKind::CouplingRunFailed
            | EventKind::PlacementDecision
            | EventKind::ThreadSpawned
            | EventKind::ThreadJoined
            | EventKind::PolicyBound
            | EventKind::GrantPetitioned
            | EventKind::TimerFired
            | EventKind::IoIngressReceived
            | EventKind::IoEgressRequested
            | EventKind::IoEgressDelivered
            | EventKind::IoEgressFailed
            | EventKind::AdmissionDecided
    )
}

fn event_kind_routes_to_analytics(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::ToolUniverseCallCompleted
            | EventKind::ToolCallCompleted
            | EventKind::TurnCompleted
            | EventKind::LoopCompleted
            | EventKind::LoopBlocked
            | EventKind::LoopBudgetExhausted
            | EventKind::LoopDenied
            | EventKind::CouplingRunCompleted
            | EventKind::CouplingRunFailed
    )
}

fn stream_id_routes_to_browser_projection(stream_id: &EventStreamId) -> bool {
    stream_id.as_str().starts_with("thread:")
        || stream_id.as_str().starts_with("control:")
        || stream_id.as_str().starts_with("derived:")
}

fn trace_id_from_context(trace_context: Option<&Value>) -> Option<String> {
    trace_context?
        .get("trace_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

pub fn validate_context_payload_schema_v1(
    kind: EventKind,
    payload: &Value,
) -> Result<(), JsonSchemaValidationError> {
    stream_schema_registry_v1()?.validate(kind.payload_schema_id(), payload)
}

fn stream_record_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "schema",
            "event_id",
            "stream_id",
            "sequence",
            "coordinates",
            "created_at_ms",
            "kind",
            "origin",
            "payload_schema",
            "provenance",
            "payload"
        ],
        "additionalProperties": true,
        "properties": {
            "schema": {"enum": [STREAM_RECORD_SCHEMA_V1]},
            "event_id": {"type": "string"},
            "stream_id": {"type": "string"},
            "sequence": {"type": "integer"},
            "coordinates": {
                "type": "object",
                "required": ["tenant_id", "user_id", "session_id", "thread_id"],
                "additionalProperties": false,
                "properties": {
                    "tenant_id": {"type": "string"},
                    "user_id": {"type": "string"},
                    "session_id": {"type": "string"},
                    "thread_id": {"type": "string"}
                }
            },
            "created_at_ms": {"type": "integer"},
            "kind": {"type": "string"},
            "origin": {"enum": ["witnessed", "discharged"]},
            "payload_schema": {"type": "string"},
            "trace_context": {
                "type": "object",
                "additionalProperties": true
            },
            "provenance": {
                "type": "object",
                "additionalProperties": true
            },
            "payload": {
                "type": "object",
                "additionalProperties": true
            }
        }
    })
}

fn stream_cursor_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["schema", "stream_id", "sequence", "event_id"],
        "additionalProperties": false,
        "properties": {
            "schema": {"enum": [STREAM_CURSOR_SCHEMA_V1]},
            "stream_id": {"type": "string"},
            "sequence": {"type": "integer"},
            "event_id": {"type": "string"}
        }
    })
}

fn stream_backend_capabilities_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "schema",
            "backend_kind",
            "storage_scope",
            "ack_classes",
            "supports_atomic_batch_append",
            "supports_verified_cursor_replay",
            "supports_query_projection",
            "supports_expected_tail",
            "supports_fencing_tokens",
            "supports_live_follow",
            "supports_broadcast",
            "supports_cold_archive"
        ],
        "additionalProperties": false,
        "properties": {
            "schema": {"enum": [STREAM_BACKEND_CAPABILITIES_SCHEMA_V1]},
            "backend_kind": {
                "enum": ["sqlite"]
            },
            "storage_scope": {
                "enum": ["local_embedded", "remote_durable", "hybrid"]
            },
            "ack_classes": stream_ack_classes_schema_v1(),
            "supports_atomic_batch_append": {"type": "boolean"},
            "supports_verified_cursor_replay": {"type": "boolean"},
            "supports_query_projection": {"type": "boolean"},
            "supports_expected_tail": {"type": "boolean"},
            "supports_fencing_tokens": {"type": "boolean"},
            "supports_live_follow": {"type": "boolean"},
            "supports_broadcast": {"type": "boolean"},
            "supports_cold_archive": {"type": "boolean"},
            "local_path": {"type": "string"}
        }
    })
}

fn stream_append_ack_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "schema",
            "stream_id",
            "start_sequence",
            "end_sequence",
            "tail_sequence",
            "tail_event_id",
            "acks"
        ],
        "additionalProperties": false,
        "properties": {
            "schema": {"enum": [STREAM_APPEND_ACK_SCHEMA_V1]},
            "stream_id": {"type": "string"},
            "start_sequence": {"type": "integer"},
            "end_sequence": {"type": "integer"},
            "tail_sequence": {"type": "integer"},
            "tail_event_id": {"type": "string"},
            "acks": stream_ack_classes_schema_v1()
        }
    })
}

fn stream_routing_decision_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["schema", "event_id", "stream_id", "sequence", "kind", "routes", "keys"],
        "additionalProperties": false,
        "properties": {
            "schema": {"enum": [STREAM_ROUTING_DECISION_SCHEMA_V1]},
            "event_id": {"type": "string"},
            "stream_id": {"type": "string"},
            "sequence": {"type": "integer"},
            "kind": {"type": "string"},
            "routes": {
                "type": "array",
                "items": {
                    "enum": [
                        "authority_store",
                        "export_bundle",
                        "model_trace",
                        "runtime_trace",
                        "browser_safe_projection",
                        "analytics_aggregate"
                    ]
                }
            },
            "keys": {
                "type": "object",
                "required": [
                    "schema",
                    "stream_id",
                    "tenant_id",
                    "user_id",
                    "session_id",
                    "thread_id",
                    "kind",
                    "origin",
                    "payload_schema"
                ],
                "additionalProperties": false,
                "properties": {
                    "schema": {"type": "string"},
                    "stream_id": {"type": "string"},
                    "tenant_id": {"type": "string"},
                    "user_id": {"type": "string"},
                    "session_id": {"type": "string"},
                    "thread_id": {"type": "string"},
                    "kind": {"type": "string"},
                    "origin": {"enum": ["witnessed", "discharged"]},
                    "payload_schema": {"type": "string"},
                    "trace_id": {"type": "string"},
                    "discharged_by": {"type": "string"}
                }
            }
        }
    })
}

fn stream_ack_classes_schema_v1() -> Value {
    serde_json::json!({
        "type": "array",
        "items": {
            "enum": [
                "local_committed",
                "query_projected",
                "stream_committed",
                "broadcast_visible",
                "archived"
            ]
        }
    })
}

fn context_read_plan_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["schema", "name", "source_stream", "frontier", "entries"],
        "additionalProperties": false,
        "properties": {
            "schema": {"enum": [CONTEXT_READ_PLAN_SCHEMA_V1]},
            "name": {"type": "string"},
            "source_stream": {"type": "string"},
            "frontier": {"enum": ["compile_frontier"]},
            "entries": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["kind"],
                    "additionalProperties": false,
                    "properties": {
                        "kind": {"enum": ["raw_range", "event_ref", "drop_range"]},
                        "stream_id": {"type": "string"},
                        "event_id": {"type": "string"},
                        "event_role": {"type": "string"},
                        "reason": {"type": "string"},
                        "range": {"type": "object", "additionalProperties": true},
                        "covers": {"type": "object", "additionalProperties": true}
                    }
                }
            }
        }
    })
}

fn context_compile_completed_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["schema", "read_plan"],
        "additionalProperties": true,
        "properties": {
            "schema": {"enum": [EventKind::ContextCompileCompleted.payload_schema_id()]},
            "strategy": {"type": "string"},
            "output_hash": {"type": "string"},
            "read_plan": context_read_plan_schema_v1()
        }
    })
}

fn context_summary_completed_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["schema", "role", "text", "covered_ranges", "content"],
        "additionalProperties": true,
        "properties": {
            "schema": {"enum": [EventKind::ContextSummaryCompleted.payload_schema_id()]},
            "role": {"enum": ["summary_checkpoint"]},
            "text": {"type": "string"},
            "covered_ranges": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["stream_id", "from_sequence", "to_sequence"],
                    "additionalProperties": false,
                    "properties": {
                        "stream_id": {"type": "string"},
                        "from_sequence": {"type": "integer"},
                        "to_sequence": {"type": "integer"}
                    }
                }
            },
            "content": {
                "type": "object",
                "required": ["sha256"],
                "additionalProperties": false,
                "properties": {
                    "sha256": {"type": "string"}
                }
            }
        }
    })
}

fn context_read_plan_set_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["schema", "scope", "name", "read_plan"],
        "additionalProperties": true,
        "properties": {
            "schema": {"enum": [EventKind::ContextReadPlanSet.payload_schema_id()]},
            "scope": {"enum": ["thread"]},
            "name": {"type": "string"},
            "pipeline_id": {"type": "string"},
            "source_id": {"type": "string"},
            "summary_event_id": {"type": "string"},
            "read_plan": context_read_plan_schema_v1()
        }
    })
}

fn thread_spawned_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "parent_thread_id",
            "child_thread_id",
            "child_manifest_hash",
            "granted",
            "inputs_hash"
        ],
        "additionalProperties": true,
        "properties": {
            "parent_thread_id": {"type": "string"},
            "parent_turn_id": {"type": "string"},
            "child_thread_id": {"type": "string"},
            "child_manifest_hash": {"type": "string"},
            "child_policy_hash": {"type": "string"},
            "granted": grant_set_schema_v1(),
            "inputs_hash": {"type": "string"},
            "fork": {
                "type": "object",
                "required": ["mode", "sourceCut"],
                "additionalProperties": true,
                "properties": {
                    "mode": {"type": "string"},
                    "sourceCut": {
                        "type": "object",
                        "required": [
                            "threadId",
                            "checkpointId",
                            "leafEntryId",
                            "streamId",
                            "streamToSequence"
                        ],
                        "additionalProperties": true,
                        "properties": {
                            "threadId": {"type": "string"},
                            "checkpointId": {"type": "string"},
                            "leafEntryId": {"type": ["string", "null"]},
                            "streamId": {"type": "string"},
                            "streamToSequence": {"type": ["integer", "null"]}
                        }
                    }
                }
            }
        }
    })
}

fn thread_spawn_requested_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "parent_thread_id",
            "child_agent_ref",
            "initial_submission",
            "correlation_id"
        ],
        "additionalProperties": true,
        "properties": {
            "parent_thread_id": {"type": "string"},
            "parent_turn_id": {"type": "string"},
            "child_agent_ref": {"type": "string"},
            "initial_submission": {"type": "string"},
            "correlation_id": {"type": "string"},
            "block_parent": {"type": "boolean"}
        }
    })
}

fn thread_joined_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["child_thread_id", "spawned_event_id", "terminal_state"],
        "additionalProperties": true,
        "properties": {
            "child_thread_id": {"type": "string"},
            "spawned_event_id": {"type": "string"},
            "terminal_state": {
                "enum": ["completed", "failed", "cancelled", "budget_exhausted"]
            },
            "result_digest": {"type": "string"}
        }
    })
}

fn policy_bound_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["policy_kind", "policy_id", "content_hash", "valid_from_note"],
        "additionalProperties": true,
        "properties": {
            "policy_kind": {
                "type": ["string", "object"],
                "additionalProperties": true
            },
            "policy_id": {"type": "string"},
            "content_hash": {"type": "string"},
            "valid_from_note": {"type": "string"}
        }
    })
}

fn grant_petitioned_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["thread_id", "requested", "reason"],
        "additionalProperties": true,
        "properties": {
            "thread_id": {"type": "string"},
            "requested": grant_set_schema_v1(),
            "reason": {"type": "string"},
            "evidence_event_ids": event_id_array_schema_v1()
        }
    })
}

fn timer_fired_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["mandate_event_id", "scheduled_for", "occurrence_index", "catch_up"],
        "additionalProperties": true,
        "properties": {
            "mandate_event_id": {"type": "string"},
            "scheduled_for": {"type": "string"},
            "occurrence_index": {"type": "integer"},
            "catch_up": {"type": "boolean"}
        }
    })
}

fn io_ingress_received_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["envelope_digest"],
        "additionalProperties": true,
        "properties": {
            "route_id": {"type": "string"},
            "dedupe_key": {"type": "string"},
            "external_conversation_id": {"type": "string"},
            "external_actor_id": {"type": "string"},
            "external_message_id": {"type": "string"},
            "envelope_digest": {"type": "string"}
        }
    })
}

fn io_egress_requested_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["egress_kind", "requested_by_tool_call_id"],
        "additionalProperties": true,
        "properties": {
            "egress_kind": {
                "type": "object",
                "additionalProperties": true
            },
            "resolved_target": {
                "type": "object",
                "additionalProperties": true
            },
            "requested_by_tool_call_id": {"type": "string"},
            "quote": {"type": "string"},
            "match_event_id": {"type": "string"}
        }
    })
}

fn io_egress_delivered_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["route_id", "egress_kind", "attempts"],
        "additionalProperties": true,
        "properties": {
            "route_id": {"type": "string"},
            "egress_kind": {"type": "string"},
            "external_message_id": {"type": "string"},
            "attempts": {"type": "integer"}
        }
    })
}

fn io_egress_failed_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["route_id", "egress_kind", "attempts", "error_class", "dead_lettered"],
        "additionalProperties": true,
        "properties": {
            "route_id": {"type": "string"},
            "egress_kind": {"type": "string"},
            "attempts": {"type": "integer"},
            "error_class": {"type": "string"},
            "dead_lettered": {"type": "boolean"}
        }
    })
}

fn admission_decided_payload_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "route_id",
            "policy_hash",
            "decision",
            "source_ingress_event_ids"
        ],
        "additionalProperties": true,
        "properties": {
            "route_id": {"type": "string"},
            "policy_hash": {"type": "string"},
            "decision": admission_decision_schema_v1(),
            "admissible": {
                "type": "array",
                "items": admission_decision_schema_v1()
            },
            "source_ingress_event_ids": event_id_array_schema_v1()
        }
    })
}

fn grant_set_schema_v1() -> Value {
    serde_json::json!({
        "type": "array",
        "items": {"type": "string"}
    })
}

fn event_id_array_schema_v1() -> Value {
    serde_json::json!({
        "type": "array",
        "items": {"type": "string"}
    })
}

fn admission_decision_schema_v1() -> Value {
    serde_json::json!({
        "enum": ["queue", "steer", "interrupt", "fork", "observe", "reject", "coalesce"]
    })
}

fn debug_thread_export_schema_v1() -> Value {
    let ack_classes = debug_export_ack_classes_schema_v1();
    let backend = debug_export_backend_schema_v1();
    let stream_record = debug_export_stream_record_schema_v1();
    let receipt = debug_export_receipt_schema_v1();
    let range = debug_export_range_schema_v1();
    serde_json::json!({
        "type": "object",
        "required": [
            "schema",
            "threadId",
            "generatedAtMs",
            "backend",
            "ackClasses",
            "redaction",
            "thread",
            "streams",
            "receipts"
        ],
        "additionalProperties": false,
        "properties": {
            "schema": {"enum": [DEBUG_THREAD_EXPORT_SCHEMA_V1]},
            "threadId": {"type": "string"},
            "generatedAtMs": {"type": "integer"},
            "backend": backend,
            "ackClasses": ack_classes,
            "thread": {
                "type": ["object", "null"],
                "additionalProperties": true
            },
            "redaction": {
                "type": "object",
                "required": ["enabled", "mode", "replacement", "redactedKeys"],
                "additionalProperties": false,
                "properties": {
                    "enabled": {"type": "boolean"},
                    "mode": {"enum": ["secret-shaped-json-keys", "none"]},
                    "replacement": {"type": "string"},
                    "redactedKeys": {"type": "array", "items": {"type": "string"}}
                }
            },
            "streams": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "selector",
                        "streamId",
                        "backend",
                        "ackClasses",
                        "range",
                        "data",
                        "eventCount",
                        "truncated",
                        "cursor",
                        "streamCursor"
                    ],
                    "additionalProperties": false,
                    "properties": {
                        "selector": {"type": "string"},
                        "streamId": {"type": "string"},
                        "backend": debug_export_backend_schema_v1(),
                        "ackClasses": debug_export_ack_classes_schema_v1(),
                        "range": range,
                        "data": {
                            "type": "array",
                            "items": stream_record
                        },
                        "eventCount": {"type": "integer"},
                        "truncated": {"type": "boolean"},
                        "cursor": {"type": ["string", "null"]},
                        "streamCursor": nullable_stream_cursor_schema_v1()
                    }
                }
            },
            "receipts": {
                "type": "array",
                "items": receipt
            }
        }
    })
}

fn debug_export_range_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "fromSequence",
            "fromCursor",
            "lastExportedSequence",
            "lastExportedStreamCursor",
            "toCursor",
            "tailSequence",
            "tailStreamCursor",
            "tailCursor"
        ],
        "additionalProperties": false,
        "properties": {
            "fromSequence": {"type": "integer"},
            "fromCursor": {"type": "string"},
            "lastExportedSequence": {"type": ["integer", "null"]},
            "lastExportedStreamCursor": nullable_stream_cursor_schema_v1(),
            "toCursor": {"type": ["string", "null"]},
            "tailSequence": {"type": ["integer", "null"]},
            "tailStreamCursor": nullable_stream_cursor_schema_v1(),
            "tailCursor": {"type": "string"}
        }
    })
}

fn nullable_stream_cursor_schema_v1() -> Value {
    let mut schema = stream_cursor_schema_v1();
    schema
        .as_object_mut()
        .unwrap()
        .insert("type".to_string(), serde_json::json!(["object", "null"]));
    schema
}

fn debug_export_stream_record_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "schema",
            "event_id",
            "stream_id",
            "sequence",
            "kind",
            "origin",
            "payload_schema",
            "payload",
            "eventId",
            "atMs"
        ],
        "additionalProperties": true,
        "properties": {
            "schema": {"enum": [STREAM_RECORD_SCHEMA_V1]},
            "event_id": {"type": "string"},
            "stream_id": {"type": "string"},
            "sequence": {"type": "integer"},
            "kind": {"type": "string"},
            "origin": {"enum": ["witnessed", "discharged"]},
            "payload_schema": {"type": "string"},
            "payload": {"type": "object", "additionalProperties": true},
            "eventId": {"type": "string"},
            "atMs": {"type": "integer"}
        }
    })
}

fn debug_export_receipt_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "eventId",
            "streamId",
            "sequence",
            "kind",
            "origin",
            "payloadSchema",
            "createdAtMs"
        ],
        "additionalProperties": false,
        "properties": {
            "eventId": {"type": "string"},
            "streamId": {"type": "string"},
            "sequence": {"type": "integer"},
            "kind": {"type": "string"},
            "origin": {"enum": ["discharged"]},
            "payloadSchema": {"type": "string"},
            "createdAtMs": {"type": "integer"}
        }
    })
}

fn debug_export_backend_schema_v1() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["kind", "sessionStorePath"],
        "additionalProperties": false,
        "properties": {
            "kind": {"enum": ["sqlite"]},
            "sessionStorePath": {"type": "string"},
            "ackClasses": debug_export_ack_classes_schema_v1()
        }
    })
}

fn debug_export_ack_classes_schema_v1() -> Value {
    serde_json::json!({
        "type": "array",
        "items": {"enum": ["local_committed", "query_projected"]}
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ObservationId(Uuid);

impl ObservationId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for ObservationId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ObservationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservationSourceRange {
    pub stream_id: EventStreamId,
    pub from_sequence: EventSequence,
    pub to_sequence: EventSequence,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservationProvenance {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_streams: Vec<EventStreamId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_event_ids: Vec<EventRecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_range: Option<ObservationSourceRange>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_ranges: Vec<ObservationSourceRange>,
    pub derivation_strategy: String,
    pub derivation_version: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewObservationRecord {
    pub id: ObservationId,
    pub kind: String,
    pub scope: ThreadCoordinates,
    pub payload: Value,
    pub created_at_ms: i64,
    pub provenance: ObservationProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<ObservationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

impl NewObservationRecord {
    pub fn new(kind: impl Into<String>, scope: ThreadCoordinates, payload: Value) -> Self {
        Self {
            id: ObservationId::new(),
            kind: kind.into(),
            scope,
            payload,
            created_at_ms: now_ms(),
            provenance: ObservationProvenance::default(),
            supersedes: None,
            confidence: None,
        }
    }

    pub fn with_provenance(mut self, provenance: ObservationProvenance) -> Self {
        self.provenance = provenance;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObservationRecord {
    pub id: ObservationId,
    pub kind: String,
    pub scope: ThreadCoordinates,
    pub payload: Value,
    pub created_at_ms: i64,
    pub provenance: ObservationProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<ObservationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

impl From<NewObservationRecord> for ObservationRecord {
    fn from(record: NewObservationRecord) -> Self {
        Self {
            id: record.id,
            kind: record.kind,
            scope: record.scope,
            payload: record.payload,
            created_at_ms: record.created_at_ms,
            provenance: record.provenance,
            supersedes: record.supersedes,
            confidence: record.confidence,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderApi {
    OpenAIResponses,
    OpenAIChatCompletions,
    AnthropicMessages,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingProvider {
    Anthropic,
    OpenAIResponses,
    OpenAICompatible,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ThinkingMetadata {
    None,
    Anthropic {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    AnthropicRedacted {
        data: String,
    },
    OpenAIResponses {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_index: Option<usize>,
        summary_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encrypted_content: Option<String>,
    },
    Opaque {
        provider: String,
        value: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CacheControl {
    Ephemeral {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl: Option<CacheTtl>,
    },
}

impl CacheControl {
    pub fn ephemeral() -> Self {
        Self::Ephemeral { ttl: None }
    }

    pub fn ephemeral_1h() -> Self {
        Self::Ephemeral {
            ttl: Some(CacheTtl::OneHour),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheTtl {
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanonicalContent {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
    },
    Image {
        data: String,
        mime_type: String,
    },
    Thinking {
        text: String,
        provider: ThinkingProvider,
        metadata: ThinkingMetadata,
    },
    ToolCall {
        id: String,
        name: String,
        #[serde(default)]
        arguments: Value,
    },
}

impl CanonicalContent {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text {
            text: text.into(),
            cache_control: None,
        }
    }

    pub fn cached_text(text: impl Into<String>, cache_control: CacheControl) -> Self {
        Self::Text {
            text: text.into(),
            cache_control: Some(cache_control),
        }
    }

    pub fn tool_call(id: impl Into<String>, name: impl Into<String>, arguments: Value) -> Self {
        Self::ToolCall {
            id: id.into(),
            name: name.into(),
            arguments,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalStopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
    PauseTurn,
    Cancelled,
    Error,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum CanonicalMessage {
    User {
        content: Vec<CanonicalContent>,
        timestamp_ms: i64,
    },
    Assistant {
        content: Vec<CanonicalContent>,
        api: ProviderApi,
        provider: String,
        model: String,
        usage: CanonicalUsage,
        stop_reason: CanonicalStopReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error_message: Option<String>,
        timestamp_ms: i64,
    },
    ToolResult {
        tool_call_id: String,
        tool_name: String,
        content: Vec<CanonicalContent>,
        is_error: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_control: Option<CacheControl>,
        timestamp_ms: i64,
    },
}

impl CanonicalMessage {
    pub fn user_text(text: impl Into<String>) -> Self {
        Self::User {
            content: vec![CanonicalContent::text(text)],
            timestamp_ms: now_ms(),
        }
    }

    pub fn assistant(
        provider: impl Into<String>,
        api: ProviderApi,
        model: impl Into<String>,
        content: Vec<CanonicalContent>,
        stop_reason: CanonicalStopReason,
    ) -> Self {
        Self::assistant_with_usage(
            provider,
            api,
            model,
            content,
            CanonicalUsage::default(),
            stop_reason,
        )
    }

    pub fn assistant_with_usage(
        provider: impl Into<String>,
        api: ProviderApi,
        model: impl Into<String>,
        content: Vec<CanonicalContent>,
        usage: CanonicalUsage,
        stop_reason: CanonicalStopReason,
    ) -> Self {
        Self::Assistant {
            content,
            api,
            provider: provider.into(),
            model: model.into(),
            usage,
            stop_reason,
            error_message: None,
            timestamp_ms: now_ms(),
        }
    }

    pub fn tool_result(
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
        content: impl Into<String>,
        is_error: bool,
    ) -> Self {
        Self::ToolResult {
            tool_call_id: tool_call_id.into(),
            tool_name: tool_name.into(),
            content: vec![CanonicalContent::text(content)],
            is_error,
            cache_control: None,
            timestamp_ms: now_ms(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEntryKind {
    Message {
        message: CanonicalMessage,
    },
    ModelChange {
        provider: String,
        api: ProviderApi,
        model: String,
    },
    Compaction {
        summary: String,
    },
    BranchSummary {
        summary: String,
    },
    Runtime {
        kind: String,
        payload: Value,
    },
    CustomContextMessage {
        message: CanonicalMessage,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEntry {
    pub entry_id: SessionEntryId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_entry_id: Option<SessionEntryId>,
    pub coordinates: ThreadCoordinates,
    pub created_at_ms: i64,
    pub kind: SessionEntryKind,
}

impl SessionEntry {
    pub fn new(
        coordinates: ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> Self {
        Self {
            entry_id: SessionEntryId::new(),
            parent_entry_id,
            coordinates,
            created_at_ms: now_ms(),
            kind,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionContext {
    pub entries: Vec<SessionEntry>,
    pub messages: Vec<CanonicalMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_cuts: Vec<SessionContextSourceCut>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionContextSourceCut {
    pub coordinates: ThreadCoordinates,
    pub stream_id: EventStreamId,
    #[serde(default)]
    pub inherited: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entry_ids: Vec<SessionEntryId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadForkReason {
    ManifestUpdate,
    ToolAdded,
    ModelChanged,
    Manual,
}

impl Default for ThreadForkReason {
    fn default() -> Self {
        Self::Manual
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ThreadBaseRef {
    pub child_thread_id: ThreadId,
    pub parent_thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_checkpoint_id: Option<ThreadCheckpointId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_leaf_entry_id: Option<SessionEntryId>,
    pub parent_stream_id: EventStreamId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_stream_to_sequence: Option<EventSequence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_binding_snapshot_id: Option<String>,
    #[serde(default)]
    pub reason: ThreadForkReason,
    pub created_at_ms: i64,
}

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn append(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry>;

    async fn append_with_provenance(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry>;

    async fn active_leaf(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>>;

    async fn select_branch(
        &self,
        coordinates: &ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()>;

    async fn build_context(&self, coordinates: &ThreadCoordinates)
    -> HistoryResult<SessionContext>;

    async fn clone_branch(
        &self,
        source_coordinates: &ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>>;

    async fn fork_by_reference(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> HistoryResult<()>;
}

#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>>;

    /// Append `records` only if the stream's next sequence equals
    /// `expected_next_sequence` (sequences are 1-based; an empty stream
    /// expects 1).
    ///
    /// On mismatch this returns [`HistoryError::AppendFenceConflict`] and
    /// appends nothing — never a partial batch. The check and the append are
    /// atomic with respect to every other append on the same store, so a
    /// caller that read the stream, decided, and appends through this fence
    /// cannot race a concurrent writer past its decision (ADR 0001 append
    /// fencing). Use plain [`EventStore::append_events`] where last-writer
    /// semantics are correct.
    ///
    /// Stores that cannot honor the atomicity contract keep this default
    /// body, which fails closed with
    /// [`HistoryError::FencedAppendUnsupported`].
    async fn append_events_fenced(
        &self,
        stream_id: &EventStreamId,
        expected_next_sequence: EventSequence,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let _ = (stream_id, expected_next_sequence, records);
        Err(HistoryError::FencedAppendUnsupported)
    }

    async fn read_events(
        &self,
        stream_id: &EventStreamId,
        from_sequence: Option<EventSequence>,
    ) -> HistoryResult<Vec<EventRecord>>;

    async fn read_events_after_cursor(
        &self,
        stream_id: &EventStreamId,
        cursor: &StreamCursorV1,
    ) -> HistoryResult<Vec<EventRecord>> {
        cursor.validate_stream_cursor_v1()?;
        if &cursor.stream_id != stream_id {
            return Err(HistoryError::StreamCursorStreamMismatch {
                cursor_stream_id: cursor.stream_id.clone(),
                requested_stream_id: stream_id.clone(),
            });
        }

        let mut events = self.read_events(stream_id, Some(cursor.sequence)).await?;
        let current = events.first();
        let matches_cursor = current
            .is_some_and(|event| event.sequence == cursor.sequence && event.id == cursor.event_id);
        if !matches_cursor {
            return Err(HistoryError::StreamCursorMismatch {
                stream_id: stream_id.clone(),
                sequence: cursor.sequence.get(),
                expected_event_id: cursor.event_id,
                actual_sequence: current.map(|event| event.sequence.get()),
                actual_event_id: current.map(|event| event.id),
            });
        }
        events.remove(0);
        Ok(events)
    }
}

#[async_trait]
pub trait ObservationStore: Send + Sync {
    async fn append_observation(
        &self,
        record: NewObservationRecord,
    ) -> HistoryResult<ObservationRecord>;

    async fn list_observations(
        &self,
        scope: &ThreadCoordinates,
        kind: Option<&str>,
    ) -> HistoryResult<Vec<ObservationRecord>>;
}

pub trait RuntimeStore: SessionStore + EventStore + ObservationStore {}

impl<T> RuntimeStore for T where T: SessionStore + EventStore + ObservationStore + Send + Sync {}

#[derive(Clone, Default)]
pub struct InMemorySessionStore {
    inner: Arc<RwLock<InMemorySessionStoreInner>>,
}

#[derive(Default)]
struct InMemorySessionStoreInner {
    entries: HashMap<ThreadId, HashMap<SessionEntryId, SessionEntry>>,
    active_leaf: HashMap<ThreadId, SessionEntryId>,
    bases: HashMap<ThreadId, ThreadBaseRef>,
    events: HashMap<EventStreamId, Vec<EventRecord>>,
    observations: HashMap<ThreadId, Vec<ObservationRecord>>,
}

impl InMemorySessionStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SessionStore for InMemorySessionStore {
    async fn append(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.append_inner(coordinates, parent_entry_id, kind, None)
            .await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry> {
        self.append_inner(coordinates, parent_entry_id, kind, Some(provenance))
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        let inner = self.inner.read().await;
        let Some(active_leaf) = inner.active_leaf.get(&coordinates.thread_id).copied() else {
            return Ok(None);
        };
        let entry = inner
            .entries
            .get(&coordinates.thread_id)
            .and_then(|entries| entries.get(&active_leaf))
            .ok_or(HistoryError::EntryNotFound(active_leaf))?;
        validate_entry_coordinates(coordinates, entry)?;
        Ok(Some(active_leaf))
    }

    async fn select_branch(
        &self,
        coordinates: &ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()> {
        let mut inner = self.inner.write().await;
        let Some(leaf_entry_id) = leaf_entry_id else {
            inner.active_leaf.remove(&coordinates.thread_id);
            return Ok(());
        };
        let entries_by_id = inner
            .entries
            .get(&coordinates.thread_id)
            .ok_or(HistoryError::EntryNotFound(leaf_entry_id))?;
        branch_path(entries_by_id, leaf_entry_id, coordinates)?;
        inner
            .active_leaf
            .insert(coordinates.thread_id, leaf_entry_id);
        Ok(())
    }

    async fn build_context(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<SessionContext> {
        let inner = self.inner.read().await;
        build_in_memory_context(&inner, coordinates, None, false, &mut HashSet::new())
    }

    async fn clone_branch(
        &self,
        source_coordinates: &ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        let mut inner = self.inner.write().await;
        inner.bases.remove(&target_coordinates.thread_id);
        let Some(source_leaf) = source_leaf else {
            inner.active_leaf.remove(&target_coordinates.thread_id);
            return Ok(None);
        };
        let entries_by_id = inner
            .entries
            .get(&source_coordinates.thread_id)
            .ok_or(HistoryError::EntryNotFound(source_leaf))?;
        let path = branch_path(entries_by_id, source_leaf, source_coordinates)?;

        let mut parent_entry_id = None;
        let mut latest_entry_id = None;
        for source_entry in path {
            let entry = SessionEntry::new(
                target_coordinates.clone(),
                parent_entry_id,
                source_entry.kind.clone(),
            );
            parent_entry_id = Some(entry.entry_id);
            latest_entry_id = Some(entry.entry_id);
            inner
                .entries
                .entry(target_coordinates.thread_id)
                .or_default()
                .insert(entry.entry_id, entry.clone());
            append_in_memory_event(
                &mut inner,
                &EventStreamId::for_thread(target_coordinates),
                session_entry_event(&entry),
            )?;
        }
        if let Some(entry_id) = latest_entry_id {
            inner
                .active_leaf
                .insert(target_coordinates.thread_id, entry_id);
        }
        Ok(latest_entry_id)
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> HistoryResult<()> {
        validate_thread_base_ref(source_coordinates, target_coordinates, &base)?;
        let mut inner = self.inner.write().await;
        validate_in_memory_base_cycle(
            &inner,
            target_coordinates.thread_id,
            source_coordinates.thread_id,
        )?;
        if let Some(parent_leaf) = base.parent_leaf_entry_id {
            build_in_memory_context(
                &inner,
                source_coordinates,
                Some(parent_leaf),
                false,
                &mut HashSet::new(),
            )?;
        }
        inner.active_leaf.remove(&target_coordinates.thread_id);
        inner.bases.insert(target_coordinates.thread_id, base);
        Ok(())
    }
}

impl InMemorySessionStore {
    async fn append_inner(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: Option<EventProvenance>,
    ) -> HistoryResult<SessionEntry> {
        let mut inner = self.inner.write().await;
        let thread_id = coordinates.thread_id;
        let parent_entry_id = match parent_entry_id {
            Some(parent) => {
                let entries = inner.entries.entry(thread_id).or_default();
                let parent_entry = entries
                    .get(&parent)
                    .ok_or(HistoryError::EntryNotFound(parent))?;
                validate_entry_coordinates(coordinates, parent_entry)?;
                Some(parent)
            }
            None => inner.active_leaf.get(&thread_id).copied(),
        };

        let entry = SessionEntry::new(coordinates.clone(), parent_entry_id, kind);
        inner
            .entries
            .entry(thread_id)
            .or_default()
            .insert(entry.entry_id, entry.clone());
        inner.active_leaf.insert(thread_id, entry.entry_id);
        append_in_memory_event(
            &mut inner,
            &EventStreamId::for_thread(coordinates),
            session_entry_event_with_optional_provenance(&entry, provenance),
        )?;
        Ok(entry)
    }
}

#[async_trait]
impl EventStore for InMemorySessionStore {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let mut inner = self.inner.write().await;
        let current_len = inner
            .events
            .get(stream_id)
            .map(|events| events.len() as i64)
            .unwrap_or_default();
        let mut appended = Vec::with_capacity(records.len());
        for (index, record) in records.into_iter().enumerate() {
            validate_new_event(&record)?;
            let event = EventRecord::from_new(
                stream_id.clone(),
                EventSequence::new(current_len + index as i64 + 1),
                record,
            );
            event.validate_stream_record_v1()?;
            appended.push(event);
        }
        inner
            .events
            .entry(stream_id.clone())
            .or_default()
            .extend(appended.clone());
        Ok(appended)
    }

    async fn read_events(
        &self,
        stream_id: &EventStreamId,
        from_sequence: Option<EventSequence>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let inner = self.inner.read().await;
        let events = inner
            .events
            .get(stream_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|event| {
                from_sequence
                    .map(|sequence| event.sequence.get() >= sequence.get())
                    .unwrap_or(true)
            })
            .collect();
        Ok(events)
    }
}

#[async_trait]
impl ObservationStore for InMemorySessionStore {
    async fn append_observation(
        &self,
        record: NewObservationRecord,
    ) -> HistoryResult<ObservationRecord> {
        let mut inner = self.inner.write().await;
        let record = ObservationRecord::from(record);
        inner
            .observations
            .entry(record.scope.thread_id)
            .or_default()
            .push(record.clone());
        Ok(record)
    }

    async fn list_observations(
        &self,
        scope: &ThreadCoordinates,
        kind: Option<&str>,
    ) -> HistoryResult<Vec<ObservationRecord>> {
        let inner = self.inner.read().await;
        let observations = inner
            .observations
            .get(&scope.thread_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|observation| observation.scope == *scope)
            .filter(|observation| kind.map(|kind| observation.kind == kind).unwrap_or(true))
            .collect();
        Ok(observations)
    }
}

fn branch_path(
    entries_by_id: &HashMap<SessionEntryId, SessionEntry>,
    leaf_entry_id: SessionEntryId,
    coordinates: &ThreadCoordinates,
) -> HistoryResult<Vec<SessionEntry>> {
    let mut path = Vec::new();
    let mut cursor = Some(leaf_entry_id);
    while let Some(entry_id) = cursor {
        let entry = entries_by_id
            .get(&entry_id)
            .ok_or(HistoryError::EntryNotFound(entry_id))?;
        validate_entry_coordinates(coordinates, entry)?;
        cursor = entry.parent_entry_id;
        path.push(entry.clone());
    }
    path.reverse();
    Ok(path)
}

fn build_in_memory_context(
    inner: &InMemorySessionStoreInner,
    coordinates: &ThreadCoordinates,
    local_leaf_override: Option<SessionEntryId>,
    inherited: bool,
    visiting: &mut HashSet<ThreadId>,
) -> HistoryResult<SessionContext> {
    if !visiting.insert(coordinates.thread_id) {
        return Err(HistoryError::ThreadBaseCycle {
            child_thread_id: coordinates.thread_id,
            ancestor_thread_id: coordinates.thread_id,
        });
    }

    let mut entries = Vec::new();
    let mut source_cuts = Vec::new();
    if let Some(base) = inner.bases.get(&coordinates.thread_id) {
        if base.child_thread_id != coordinates.thread_id {
            return Err(HistoryError::Storage(format!(
                "thread base child id {} does not match requested thread {}",
                base.child_thread_id, coordinates.thread_id
            )));
        }
        let parent_coordinates = coordinates_with_thread_id(coordinates, base.parent_thread_id);
        let parent_context = build_in_memory_context(
            inner,
            &parent_coordinates,
            base.parent_leaf_entry_id,
            true,
            visiting,
        )?;
        entries.extend(parent_context.entries);
        source_cuts.extend(parent_context.source_cuts);
    }

    let local_leaf =
        local_leaf_override.or_else(|| inner.active_leaf.get(&coordinates.thread_id).copied());
    if let Some(local_leaf) = local_leaf {
        let entries_by_id = inner
            .entries
            .get(&coordinates.thread_id)
            .ok_or(HistoryError::EntryNotFound(local_leaf))?;
        let local_path = branch_path(entries_by_id, local_leaf, coordinates)?;
        if !local_path.is_empty() {
            source_cuts.push(SessionContextSourceCut {
                coordinates: coordinates.clone(),
                stream_id: EventStreamId::for_thread(coordinates),
                inherited,
                entry_ids: local_path.iter().map(|entry| entry.entry_id).collect(),
            });
        }
        entries.extend(local_path);
    }

    visiting.remove(&coordinates.thread_id);
    let mut messages = Vec::new();
    append_model_visible_messages(&entries, &mut messages);
    Ok(SessionContext {
        entries,
        messages,
        source_cuts,
    })
}

fn validate_in_memory_base_cycle(
    inner: &InMemorySessionStoreInner,
    child_thread_id: ThreadId,
    parent_thread_id: ThreadId,
) -> HistoryResult<()> {
    let mut cursor = Some(parent_thread_id);
    let mut visited = HashSet::new();
    while let Some(thread_id) = cursor {
        if thread_id == child_thread_id || !visited.insert(thread_id) {
            return Err(HistoryError::ThreadBaseCycle {
                child_thread_id,
                ancestor_thread_id: thread_id,
            });
        }
        cursor = inner
            .bases
            .get(&thread_id)
            .map(|base| base.parent_thread_id);
    }
    Ok(())
}

pub fn validate_thread_base_ref(
    source_coordinates: &ThreadCoordinates,
    target_coordinates: &ThreadCoordinates,
    base: &ThreadBaseRef,
) -> HistoryResult<()> {
    if source_coordinates.scope() != target_coordinates.scope() {
        return Err(HistoryError::ThreadScopeMismatch {
            requested: Box::new(target_coordinates.clone()),
            actual: Box::new(source_coordinates.clone()),
        });
    }
    if base.parent_thread_id != source_coordinates.thread_id {
        return Err(HistoryError::Storage(format!(
            "thread base parent id {} does not match source thread {}",
            base.parent_thread_id, source_coordinates.thread_id
        )));
    }
    if base.child_thread_id != target_coordinates.thread_id {
        return Err(HistoryError::Storage(format!(
            "thread base child id {} does not match target thread {}",
            base.child_thread_id, target_coordinates.thread_id
        )));
    }
    let expected_parent_stream = EventStreamId::for_thread(source_coordinates);
    if base.parent_stream_id != expected_parent_stream {
        return Err(HistoryError::Storage(format!(
            "thread base parent stream {} does not match source stream {}",
            base.parent_stream_id, expected_parent_stream
        )));
    }
    Ok(())
}

pub fn coordinates_with_thread_id(
    coordinates: &ThreadCoordinates,
    thread_id: ThreadId,
) -> ThreadCoordinates {
    ThreadCoordinates {
        tenant_id: coordinates.tenant_id.clone(),
        user_id: coordinates.user_id.clone(),
        session_id: coordinates.session_id.clone(),
        thread_id,
    }
}

pub fn decode_entry(json: &str) -> HistoryResult<SessionEntry> {
    serde_json::from_str(json).map_err(codec_error)
}

pub fn append_model_visible_messages(
    entries: &[SessionEntry],
    messages: &mut Vec<CanonicalMessage>,
) {
    for entry in entries {
        match &entry.kind {
            SessionEntryKind::Message { message }
            | SessionEntryKind::CustomContextMessage { message } => {
                messages.push(message.clone());
            }
            SessionEntryKind::Compaction { summary } => {
                messages.clear();
                messages.push(compaction_summary_message(summary));
            }
            SessionEntryKind::ModelChange { .. }
            | SessionEntryKind::BranchSummary { .. }
            | SessionEntryKind::Runtime { .. } => {}
        }
    }
}

fn append_in_memory_event(
    inner: &mut InMemorySessionStoreInner,
    stream_id: &EventStreamId,
    record: NewEventRecord,
) -> HistoryResult<EventRecord> {
    validate_new_event(&record)?;
    let sequence = EventSequence::new(
        inner
            .events
            .get(stream_id)
            .map(|events| events.len() as i64)
            .unwrap_or_default()
            + 1,
    );
    let event = EventRecord::from_new(stream_id.clone(), sequence, record);
    event.validate_stream_record_v1()?;
    inner
        .events
        .entry(stream_id.clone())
        .or_default()
        .push(event.clone());
    Ok(event)
}

pub fn session_entry_event(entry: &SessionEntry) -> NewEventRecord {
    session_entry_event_with_optional_provenance(entry, None)
}

pub fn session_entry_event_with_provenance(
    entry: &SessionEntry,
    provenance: EventProvenance,
) -> NewEventRecord {
    session_entry_event_with_optional_provenance(entry, Some(provenance))
}

fn session_entry_event_with_optional_provenance(
    entry: &SessionEntry,
    provenance: Option<EventProvenance>,
) -> NewEventRecord {
    let mut payload = serde_json::json!({
        "entry_id": entry.entry_id.to_string(),
        "parent_entry_id": entry.parent_entry_id.map(|id| id.to_string()),
        "entry_kind": session_entry_kind_name(&entry.kind),
    });
    if let SessionEntryKind::Message {
        message: CanonicalMessage::Assistant { usage, .. },
    }
    | SessionEntryKind::CustomContextMessage {
        message: CanonicalMessage::Assistant { usage, .. },
    } = &entry.kind
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("usage".to_string(), serde_json::to_value(usage).unwrap());
    }
    if session_entry_is_user_authored(&entry.kind) {
        return NewEventRecord::witnessed(
            entry.coordinates.clone(),
            EventKind::SessionEntryAppended,
            payload,
        );
    }
    let provenance = provenance.unwrap_or_else(|| EventProvenance {
        discharged_by: Some("session-store:append".to_string()),
        ..EventProvenance::default()
    });
    NewEventRecord::discharged(
        entry.coordinates.clone(),
        EventKind::SessionEntryAppended,
        payload,
        provenance,
    )
}

pub fn session_entry_is_user_authored(kind: &SessionEntryKind) -> bool {
    matches!(
        kind,
        SessionEntryKind::Message {
            message: CanonicalMessage::User { .. },
        } | SessionEntryKind::CustomContextMessage {
            message: CanonicalMessage::User { .. },
        }
    )
}

fn session_entry_kind_name(kind: &SessionEntryKind) -> &'static str {
    match kind {
        SessionEntryKind::Message { .. } => "message",
        SessionEntryKind::ModelChange { .. } => "model_change",
        SessionEntryKind::Compaction { .. } => "compaction",
        SessionEntryKind::BranchSummary { .. } => "branch_summary",
        SessionEntryKind::Runtime { .. } => "runtime",
        SessionEntryKind::CustomContextMessage { .. } => "custom_context_message",
    }
}

pub fn parse_uuid(value: &str) -> HistoryResult<Uuid> {
    Uuid::parse_str(value).map_err(codec_error)
}

pub fn parse_thread_id(value: &str) -> HistoryResult<ThreadId> {
    ThreadId::parse_str(value).map_err(codec_error)
}

pub fn parse_event_origin(value: &str) -> HistoryResult<EventOrigin> {
    match value {
        "witnessed" => Ok(EventOrigin::Witnessed),
        "discharged" => Ok(EventOrigin::Discharged),
        other => Err(HistoryError::Codec(format!(
            "unknown event origin: {other}"
        ))),
    }
}

pub fn validate_new_event(record: &NewEventRecord) -> HistoryResult<()> {
    if record.origin == EventOrigin::Discharged && record.provenance.is_empty() {
        return Err(HistoryError::DischargedWithoutProvenance(record.id));
    }
    Ok(())
}

pub fn validate_entry_coordinates(
    requested: &ThreadCoordinates,
    entry: &SessionEntry,
) -> HistoryResult<()> {
    if entry.coordinates == *requested {
        return Ok(());
    }
    if entry.coordinates.thread_id != requested.thread_id {
        return Err(HistoryError::EntryThreadMismatch {
            entry_id: entry.entry_id,
            requested_thread_id: requested.thread_id,
            actual_thread_id: entry.coordinates.thread_id,
        });
    }
    Err(HistoryError::ThreadScopeMismatch {
        requested: Box::new(requested.clone()),
        actual: Box::new(entry.coordinates.clone()),
    })
}

pub fn storage_error(err: impl std::fmt::Display) -> HistoryError {
    HistoryError::Storage(err.to_string())
}

pub fn codec_error(err: impl std::fmt::Display) -> HistoryError {
    HistoryError::Codec(err.to_string())
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests;
