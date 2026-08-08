//! Canonical history stores treat the journal as authority and mutable tables
//! such as SQLite `active_leaves` as derived read models. On open, branch
//! selection events rebuild that cache with the last selection per thread
//! winning.
//!
//! Legacy databases are the migration exception. A thread with no
//! `thread.branch.selected` event retains its pre-event `active_leaves` row so
//! an upgrade does not erase the only surviving branch choice. One-time schema
//! migrations may also update legacy event rows to add frozen schema identity
//! and honest migration provenance. These retrofits are migration work only;
//! runtime branch selection must append witnessed journal authority.

pub type HistoryResult<T> = Result<T, HistoryError>;

pub const STREAM_RECORD_SCHEMA_V1: &str = "cooldis.stream.record/1";
pub const STREAM_CURSOR_SCHEMA_V1: &str = "cooldis.stream.cursor/1";
pub const STREAM_BACKEND_CAPABILITIES_SCHEMA_V1: &str = "cooldis.stream.backend_capabilities/1";
pub const STREAM_APPEND_ACK_SCHEMA_V1: &str = "cooldis.stream.append_ack/1";
pub const STREAM_ROUTING_DECISION_SCHEMA_V1: &str = "cooldis.stream.routing_decision/1";
pub const CONTEXT_READ_PLAN_SCHEMA_V1: &str = "cooldis.context.read_plan/1";
pub const DEBUG_THREAD_EXPORT_SCHEMA_V1: &str = "cooldis.debug.thread_export/1";
pub const EVENT_KIND_SCHEMA_VERSION: &str = "cooldis.events/0.3";

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

pub fn compaction_summary_message(summary: &str, timestamp_ms: i64) -> CanonicalMessage {
    CanonicalMessage::user_text_at(render_compaction_summary(summary), timestamp_ms)
}

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("session entry not found: {0}")]
    EntryNotFound(SessionEntryId),
    #[error(
        "session entry {entry_id} belongs to thread {actual_thread_id}, not {requested_thread_id}"
    )]
    EntryThreadMismatch {
        entry_id: SessionEntryId,
        requested_thread_id: verlet_runtime_contracts::ThreadId,
        actual_thread_id: verlet_runtime_contracts::ThreadId,
    },
    #[error("thread history belongs to {actual:?}, not {requested:?}")]
    ThreadScopeMismatch {
        requested: Box<verlet_runtime_contracts::ThreadCoordinates>,
        actual: Box<verlet_runtime_contracts::ThreadCoordinates>,
    },
    #[error(
        "thread base for child {child_thread_id} would create a cycle through {ancestor_thread_id}"
    )]
    ThreadBaseCycle {
        child_thread_id: verlet_runtime_contracts::ThreadId,
        ancestor_thread_id: verlet_runtime_contracts::ThreadId,
    },
    #[error("history storage failed: {0}")]
    Storage(String),
    #[error("history codec failed: {0}")]
    Codec(String),
    /// A discharged event reached an append path without provenance.
    #[error("discharged event {0} has no provenance")]
    DischargedWithoutProvenance(EventRecordId),
    #[error("event id already exists: {0}")]
    DuplicateEventId(EventRecordId),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SessionEntryId(uuid::Uuid);

impl SessionEntryId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
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

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct EventStreamId(String);

impl EventStreamId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn for_thread(coordinates: &verlet_runtime_contracts::ThreadCoordinates) -> Self {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct EventSequence(i64);

impl EventSequence {
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct EventRecordId(uuid::Uuid);

impl EventRecordId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
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

/// Frozen event-kind vocabulary, version `cooldis.events/0.3`.
///
/// Laws:
/// - The vocabulary is append-only: kinds may be added in later versions,
///   but an existing kind's string and semantics are frozen forever.
/// - Parsing is fail-closed: an unknown kind string is an error, never a
///   passthrough. There is no `Other` variant by design.
/// - Event kinds are the trigger addressing scheme for every future
///   propagator and controller; renaming one would make receipts lie.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(try_from = "String", into = "String")]
pub enum EventKind {
    /// A session entry was appended to the thread stream.
    #[strum(serialize = "session.entry.appended")]
    SessionEntryAppended,
    /// Context assembly completed; the payload is the assembly receipt.
    #[strum(serialize = "context.compile.completed")]
    ContextCompileCompleted,
    /// A context summarizer discharged a summary checkpoint event. The compacted
    /// text lives in the payload; provenance records the summarizer boundary.
    #[strum(serialize = "context.summary.completed")]
    ContextSummaryCompleted,
    /// A context controller selected a named read plan for future assembly.
    #[strum(serialize = "context.read_plan.set")]
    ContextReadPlanSet,
    /// A manifest compiled to a resolved plan; the payload is the compile
    /// receipt. Emitted by the WS-A compile/bind layer.
    #[strum(serialize = "manifest.compile.completed")]
    ManifestCompileCompleted,
    /// An agent ref resolved through aliases to a manifest hash and bound;
    /// the payload is the bind receipt. Emitted at publish and at run time.
    #[strum(serialize = "manifest.bind.completed")]
    ManifestBindCompleted,
    /// A tool universe's contracts were witnessed (at bind or on demand);
    /// the payload is `ToolUniverseDiscoveryReceipt` — server ref, discovery
    /// hash, and per-tool schema hashes. Witnessed origin: the contracts
    /// arrived from outside the system.
    #[strum(serialize = "tool.universe.discovery.completed")]
    ToolUniverseDiscoveryCompleted,
    /// One `tool.call` against a live universe completed; the payload is
    /// `ToolUniverseCallReceipt` — server ref, tool name, the schema hash
    /// the arguments were validated against, and the output hash.
    #[strum(serialize = "tool.universe.call.completed")]
    ToolUniverseCallCompleted,
    /// A model/tool surface requested one tool call.
    #[strum(serialize = "tool.call.requested")]
    ToolCallRequested,
    /// A controller suspended a tool call pending later control input.
    #[strum(serialize = "tool.call.suspended")]
    ToolCallSuspended,
    /// A controller decided how a pending tool call should proceed.
    #[strum(serialize = "tool.call.decision")]
    ToolCallDecision,
    /// A tool executor observed a completed tool invocation.
    #[strum(serialize = "tool.call.completed")]
    ToolCallCompleted,
    /// A turn submission entered the thread.
    #[strum(serialize = "turn.submitted")]
    TurnSubmitted,
    /// A turn is waiting on a durable control fact.
    #[strum(serialize = "turn.waiting")]
    TurnWaiting,
    /// A previously waiting turn resumed.
    #[strum(serialize = "turn.resumed")]
    TurnResumed,
    /// A turn reached quiescence.
    #[strum(serialize = "turn.completed")]
    TurnCompleted,
    /// A controller requested external approval.
    #[strum(serialize = "approval.requested")]
    ApprovalRequested,
    /// An approved external surface witnessed an approval decision.
    #[strum(serialize = "approval.resolved")]
    ApprovalResolved,
    /// An external grantor started a standing activation mandate.
    #[strum(serialize = "mandate.started")]
    MandateStarted,
    /// An external grantor revoked a standing activation mandate.
    #[strum(serialize = "mandate.revoked")]
    MandateRevoked,
    /// A controller requested another turn.
    #[strum(serialize = "turn.continue.requested")]
    TurnContinueRequested,
    /// The scheduler accepted a continuation request.
    #[strum(serialize = "turn.continuation.accepted")]
    TurnContinuationAccepted,
    /// The scheduler rejected a continuation request.
    #[strum(serialize = "turn.continuation.rejected")]
    TurnContinuationRejected,
    /// A loop completed successfully.
    #[strum(serialize = "loop.completed")]
    LoopCompleted,
    /// A loop stopped because it is blocked.
    #[strum(serialize = "loop.blocked")]
    LoopBlocked,
    /// A loop stopped because its budget is exhausted.
    #[strum(serialize = "loop.budget_exhausted")]
    LoopBudgetExhausted,
    /// A loop stopped because continuation was denied.
    #[strum(serialize = "loop.denied")]
    LoopDenied,
    /// A coupling activation completed and emitted its run receipt.
    #[strum(serialize = "coupling.run.completed")]
    CouplingRunCompleted,
    /// A coupling activation failed and emitted its run receipt.
    #[strum(serialize = "coupling.run.failed")]
    CouplingRunFailed,
    /// A placement controller selected where execution should run.
    #[strum(serialize = "placement.decision")]
    PlacementDecision,
    /// A coupling proposed spawning supervised child work. A durable
    /// projector consumes the request, performs the spawn through the
    /// thread/turn kernel package, and the kernel witnesses `thread.spawned`
    /// — the same requested/projector grammar as IO egress.
    #[strum(serialize = "thread.spawn.requested")]
    ThreadSpawnRequested,
    /// A parent thread spawned a child thread with the recorded manifest,
    /// policy, grants, and input digest.
    #[strum(serialize = "thread.spawned")]
    ThreadSpawned,
    /// A spawned child thread reached a terminal state and joined back to its
    /// parent lineage.
    #[strum(serialize = "thread.joined")]
    ThreadJoined,
    /// A thread's live branch selection changed; appended in the same
    /// transaction as the `active_leaves` cache update. Selecting no branch
    /// (clearing) is itself a witnessed selection. Added in
    /// `cooldis.events/0.3`.
    #[strum(serialize = "thread.branch.selected")]
    ThreadBranchSelected,
    /// A lazily reloaded thread's journal could not reconstruct full
    /// lifecycle identity and the loader fell back to a fabricated root
    /// record. Added in `cooldis.events/0.3`.
    #[strum(serialize = "thread.reload.degraded")]
    ThreadReloadDegraded,
    /// A policy identity became active. The binding is valid until the next
    /// `policy.bound` with the same `policy_id`.
    #[strum(serialize = "policy.bound")]
    PolicyBound,
    /// A thread petitioned for additional grants. Resolution is recorded with
    /// the existing approval event pair.
    #[strum(serialize = "grant.petitioned")]
    GrantPetitioned,
    /// A standing mandate produced a clock occurrence.
    #[strum(serialize = "timer.fired")]
    TimerFired,
    /// A boundary client appended one opaque, schema-declared record to a
    /// client-owned stream. The declared kind and schema live in the payload.
    #[strum(serialize = "client.record.appended")]
    ClientRecordAppended,
    /// An external IO route received an ingress envelope.
    #[strum(serialize = "io.ingress.received")]
    IoIngressReceived,
    /// The runtime accepted sole responsibility for an admitted ingress
    /// envelope's outcome, fenced onto the resolved thread's control stream
    /// before any non-idempotent effect (ADR 0003). `io.ingress` names the
    /// ingress-envelope outcome lifecycle, not the producing component or
    /// stream. Added in `cooldis.events/0.3`.
    #[strum(serialize = "io.ingress.claimed")]
    IoIngressClaimed,
    /// A claimed ingress outcome reached its terminal state, with provenance
    /// to the claim and its execution evidence. A settled claim is terminal:
    /// redelivery dedupes against it. Added in `cooldis.events/0.3`.
    #[strum(serialize = "io.ingress.settled")]
    IoIngressSettled,
    /// A tool path requested an IO egress action for later projection.
    #[strum(serialize = "io.egress.requested")]
    IoEgressRequested,
    /// An IO egress attempt was delivered to the external route.
    #[strum(serialize = "io.egress.delivered")]
    IoEgressDelivered,
    /// An IO egress attempt failed and may have been dead-lettered.
    #[strum(serialize = "io.egress.failed")]
    IoEgressFailed,
    /// An admission policy chose how to handle one or more ingress events.
    #[strum(serialize = "admission.decided")]
    AdmissionDecided,
}

impl EventKind {
    pub fn payload_schema_id(self) -> String {
        format!("cooldis.event.{self}/1")
    }
}

impl TryFrom<String> for EventKind {
    type Error = HistoryError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parsed: Result<Self, strum::ParseError> = value.parse();
        // `strum::ParseError` carries no detail beyond "no such variant"; the
        // offending string is the only useful context, so it is what we keep.
        parsed.map_err(|_| HistoryError::Codec(format!("unknown event kind: {value}")))
    }
}

impl From<EventKind> for String {
    fn from(kind: EventKind) -> Self {
        kind.to_string()
    }
}

/// Terminal child-thread states recorded by `thread.joined`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadTerminalState {
    Completed,
    Failed,
    Cancelled,
    BudgetExhausted,
}

/// Policy identities bound into the event stream. `Other` is a policy-kind
/// extension point inside the payload, not a catch-all event kind.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyKind {
    AdmissionRoute,
    CouplingSet,
    Orchestrator,
    Other(String),
}

/// Admission decisions a route policy can choose for an ingress batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadSpawnRequestedPayload {
    pub parent_thread_id: verlet_runtime_contracts::ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_turn_id: Option<String>,
    /// Caller-facing alias for the child handle. Absent on pre-handle-lane
    /// records and supervisor requests that do not declare an alias.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_name: Option<String>,
    /// Deterministic first-turn identity for a dispatched child. Absent on
    /// legacy supervisor requests, whose projector derives it from the
    /// request event id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_turn_id: Option<String>,
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadSpawnedPayload {
    pub parent_thread_id: verlet_runtime_contracts::ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_turn_id: Option<String>,
    pub child_thread_id: verlet_runtime_contracts::ThreadId,
    pub child_manifest_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_policy_hash: Option<String>,
    /// Serialized grant set as recorded at spawn.
    pub granted: Vec<String>,
    pub inputs_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<ThreadSpawnedForkPayload>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadSpawnedForkPayload {
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_event_id: Option<EventRecordId>,
    #[serde(rename = "sourceCut")]
    pub source_cut: ThreadSpawnedForkSourceCutPayload,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSpawnedForkSourceCutPayload {
    pub thread_id: verlet_runtime_contracts::ThreadId,
    pub checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
    pub leaf_entry_id: Option<SessionEntryId>,
    pub stream_id: EventStreamId,
    pub stream_to_sequence: Option<EventSequence>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadJoinedPayload {
    pub child_thread_id: verlet_runtime_contracts::ThreadId,
    pub spawned_event_id: EventRecordId,
    pub terminal_state: ThreadTerminalState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_digest: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PolicyBoundPayload {
    pub policy_kind: PolicyKind,
    pub policy_id: String,
    pub content_hash: String,
    /// "Valid until next policy.bound of same policy_id" semantics.
    pub valid_from_note: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GrantPetitionedPayload {
    pub thread_id: verlet_runtime_contracts::ThreadId,
    pub requested: Vec<String>,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_event_ids: Option<Vec<EventRecordId>>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TimerFiredPayload {
    pub mandate_event_id: EventRecordId,
    pub scheduled_for: String,
    pub occurrence_index: u64,
    pub catch_up: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClientRecordAppendedPayload {
    pub client_kind: String,
    pub client_schema: String,
    pub principal_id: String,
    pub body: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
    /// Selected ingress content whose payload is itself a durable fold
    /// source. Absent on legacy and ordinary ingress witnesses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<serde_json::Value>,
    pub envelope_digest: String,
}

/// Intended outcome carried by an `io.ingress.claimed` event. Exactly one
/// intent exists per claim; the variants mirror the admission outcomes
/// (ADR 0003).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum IngressOutcomeIntent {
    Turn {
        turn_id: String,
        submission_mode: String,
        input_digest: String,
    },
    Fork {
        child_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        child_thread_id: Option<verlet_runtime_contracts::ThreadId>,
        input_digest: String,
    },
    Interrupt {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replacement_turn_id: Option<String>,
        cancel_reason: String,
        input_digest: String,
    },
    Observe {
        reason: String,
    },
    Reject {
        reason: String,
    },
}

/// Payload for `io.ingress.claimed`. Laws (ADR 0003): at most one claim
/// exists per ingress envelope id; the claim precedes every non-idempotent
/// effect; every claim settles exactly once.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoIngressClaimedPayload {
    /// Envelope set the claim covers (multiple when coalesced).
    pub ingress_envelope_ids: Vec<String>,
    /// Control-stream `io.ingress.received` witness event ids for those
    /// envelopes.
    pub ingress_witness_event_ids: Vec<EventRecordId>,
    /// The `admission.decided` event this claim executes.
    pub admission_event_id: EventRecordId,
    pub intent: IngressOutcomeIntent,
}

/// How a claim reached its settle (ADR 0003 recovery law).
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressSettledBy {
    Execution,
    Recovery,
}

/// Payload for `io.ingress.settled`. A settled claim is terminal:
/// redelivery dedupes against it and repeats no control effects.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoIngressSettledPayload {
    /// The claim this settle terminates.
    pub claim_event_id: EventRecordId,
    pub ingress_envelope_ids: Vec<String>,
    /// Earliest executing-side evidence for the claimed outcome; absent for
    /// effect-free outcomes (observe, reject). `turn.submitted` is NOT
    /// evidence: it is the submitting side's apply-time record (EMO-364).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_event_id: Option<EventRecordId>,
    pub settled_by: IngressSettledBy,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoEgressRequestedPayload {
    pub egress_kind: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_target: Option<serde_json::Value>,
    pub requested_by_tool_call_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_event_id: Option<EventRecordId>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoEgressDeliveredPayload {
    pub route_id: String,
    pub egress_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_message_id: Option<String>,
    pub attempts: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IoEgressFailedPayload {
    pub route_id: String,
    pub egress_kind: String,
    pub attempts: u32,
    pub error_class: String,
    pub dead_lettered: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AdmissionDecidedPayload {
    pub route_id: String,
    pub policy_hash: String,
    pub decision: AdmissionDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admissible: Option<Vec<AdmissionDecision>>,
    pub source_ingress_event_ids: Vec<EventRecordId>,
}

/// Payload for `thread.branch.selected`. Appended in the same transaction
/// as the `active_leaves` cache update; the cache is thereby a derived read
/// model, rebuildable by folding these events (last selection per thread
/// wins).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadBranchSelectedPayload {
    pub thread_id: verlet_runtime_contracts::ThreadId,
    /// The selected leaf entry. `None` clears the selection and is itself a
    /// witnessed selection of "no branch".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_entry_id: Option<SessionEntryId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_entry_id: Option<SessionEntryId>,
}

/// Payload for `thread.reload.degraded`. The witnessed fallback fact when a
/// pre-payload thread's journal cannot reconstruct full lifecycle identity
/// on lazy reload and the loader applies a fabricated root record
/// (EMO-370). Degradation is never silent: this event accompanies every
/// fallback.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadReloadDegradedPayload {
    pub thread_id: verlet_runtime_contracts::ThreadId,
    /// Identity fields the journal could not supply (for example
    /// "topology", "parent_thread_id", "metadata").
    pub missing: Vec<String>,
    /// The fallback identity applied; today always "fabricated_root".
    pub fallback: String,
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
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EventOrigin {
    Witnessed,
    Discharged,
}

/// Provenance for a discharged event: which coupling produced it, from what
/// upstream records. Empty provenance is only legal on witnessed events.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NewEventRecord {
    pub id: EventRecordId,
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub created_at_ms: i64,
    pub kind: EventKind,
    pub origin: EventOrigin,
    #[serde(default)]
    pub provenance: EventProvenance,
    pub payload: serde_json::Value,
}

impl NewEventRecord {
    /// A witnessed event: arrived from outside the system, no provenance.
    pub fn witnessed(
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Self {
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
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        kind: EventKind,
        payload: serde_json::Value,
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EventRecord {
    pub id: EventRecordId,
    pub stream_id: EventStreamId,
    pub sequence: EventSequence,
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub created_at_ms: i64,
    pub kind: EventKind,
    pub origin: EventOrigin,
    #[serde(default)]
    pub provenance: EventProvenance,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamRecordEnvelopeV1 {
    pub schema: String,
    pub event_id: EventRecordId,
    pub stream_id: EventStreamId,
    pub sequence: EventSequence,
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub created_at_ms: i64,
    pub kind: String,
    pub origin: EventOrigin,
    pub payload_schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_context: Option<serde_json::Value>,
    #[serde(default)]
    pub provenance: EventProvenance,
    pub payload: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamAckClass {
    LocalCommitted,
    QueryProjected,
    StreamCommitted,
    BroadcastVisible,
    Archived,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamBackendKindV1 {
    Sqlite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamStorageScopeV1 {
    LocalEmbedded,
    RemoteDurable,
    Hybrid,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
            supports_expected_tail: true,
            supports_fencing_tokens: false,
            supports_live_follow: false,
            supports_broadcast: false,
            supports_cold_archive: false,
            local_path: Some(local_path.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamRouteProfile {
    AuthorityStore,
    ExportBundle,
    ModelTrace,
    RuntimeTrace,
    BrowserSafeProjection,
    AnalyticsAggregate,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamRoutingKeysV1 {
    pub schema: String,
    pub stream_id: EventStreamId,
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub thread_id: verlet_runtime_contracts::ThreadId,
    pub kind: String,
    pub origin: EventOrigin,
    pub payload_schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discharged_by: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
            kind: self.kind.to_string(),
            origin: self.origin,
            payload_schema: self.kind.payload_schema_id(),
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
        let registry = stream_schema_registry_v1();
        registry
            .validate(STREAM_RECORD_SCHEMA_V1, &envelope)
            .map_err(|err| HistoryError::Codec(err.to_string()))?;
        if self.kind == EventKind::IoEgressRequested {
            registry
                .validate(&self.kind.payload_schema_id(), &self.payload)
                .map_err(|err| HistoryError::Codec(err.to_string()))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
            .validate(STREAM_CURSOR_SCHEMA_V1, &cursor)
            .map_err(|err| HistoryError::Codec(err.to_string()))
    }
}

/// Returns the frozen V1 stream schema registry, cached once per process.
///
/// # Panics
///
/// Panics on first use if any frozen schema is malformed.
pub fn stream_schema_registry_v1() -> &'static verlet_runtime_contracts::SchemaRegistry {
    static REGISTRY: std::sync::LazyLock<verlet_runtime_contracts::SchemaRegistry> =
        std::sync::LazyLock::new(|| {
            (|| -> Result<verlet_runtime_contracts::SchemaRegistry, verlet_runtime_contracts::JsonSchemaValidationError> {
            let mut registry = verlet_runtime_contracts::SchemaRegistry::new();
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
                EventKind::ThreadBranchSelected.payload_schema_id(),
                thread_branch_selected_payload_schema_v1(),
            )?;
            registry.register(
                EventKind::ThreadReloadDegraded.payload_schema_id(),
                thread_reload_degraded_payload_schema_v1(),
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
                EventKind::ClientRecordAppended.payload_schema_id(),
                client_record_appended_payload_schema_v1(),
            )?;
            registry.register(
                EventKind::IoIngressReceived.payload_schema_id(),
                io_ingress_received_payload_schema_v1(),
            )?;
            registry.register(
                EventKind::IoIngressClaimed.payload_schema_id(),
                io_ingress_claimed_payload_schema_v1(),
            )?;
            registry.register(
                EventKind::IoIngressSettled.payload_schema_id(),
                io_ingress_settled_payload_schema_v1(),
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
        })()
        .expect("frozen V1 stream schema registry must contain only valid schemas")
        });

    &REGISTRY
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
            | EventKind::ThreadBranchSelected
            | EventKind::ThreadReloadDegraded
            | EventKind::PolicyBound
            | EventKind::GrantPetitioned
            | EventKind::TimerFired
            | EventKind::IoIngressReceived
            | EventKind::IoIngressClaimed
            | EventKind::IoIngressSettled
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

fn trace_id_from_context(trace_context: Option<&serde_json::Value>) -> Option<String> {
    trace_context?
        .get("trace_id")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

pub fn validate_context_payload_schema_v1(
    kind: EventKind,
    payload: &serde_json::Value,
) -> Result<(), verlet_runtime_contracts::JsonSchemaValidationError> {
    stream_schema_registry_v1().validate(&kind.payload_schema_id(), payload)
}

fn stream_record_schema_v1() -> serde_json::Value {
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

fn stream_cursor_schema_v1() -> serde_json::Value {
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

fn stream_backend_capabilities_schema_v1() -> serde_json::Value {
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

fn stream_append_ack_schema_v1() -> serde_json::Value {
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

fn stream_routing_decision_schema_v1() -> serde_json::Value {
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

fn stream_ack_classes_schema_v1() -> serde_json::Value {
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

fn context_read_plan_schema_v1() -> serde_json::Value {
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

fn context_compile_completed_payload_schema_v1() -> serde_json::Value {
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

fn context_summary_completed_payload_schema_v1() -> serde_json::Value {
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

fn context_read_plan_set_payload_schema_v1() -> serde_json::Value {
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

fn thread_spawned_payload_schema_v1() -> serde_json::Value {
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
                    "claim_event_id": {"type": "string"},
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

fn thread_spawn_requested_payload_schema_v1() -> serde_json::Value {
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
            "task_name": {"type": "string"},
            "submitted_turn_id": {"type": "string"},
            "child_agent_ref": {"type": "string"},
            "initial_submission": {"type": "string"},
            "correlation_id": {"type": "string"},
            "block_parent": {"type": "boolean"}
        }
    })
}

fn thread_joined_payload_schema_v1() -> serde_json::Value {
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

fn thread_branch_selected_payload_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["thread_id"],
        "additionalProperties": true,
        "properties": {
            "thread_id": {"type": "string"},
            "selected_entry_id": {"type": ["string", "null"]},
            "prior_entry_id": {"type": ["string", "null"]}
        }
    })
}

fn policy_bound_payload_schema_v1() -> serde_json::Value {
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

fn grant_petitioned_payload_schema_v1() -> serde_json::Value {
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

fn timer_fired_payload_schema_v1() -> serde_json::Value {
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

fn client_record_appended_payload_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["client_kind", "client_schema", "principal_id", "body"],
        "additionalProperties": false,
        "properties": {
            "client_kind": {"type": "string"},
            "client_schema": {"type": "string"},
            "principal_id": {"type": "string"},
            "body": {
                "type": ["object", "array", "string", "number", "boolean", "null"],
                "additionalProperties": true,
                "items": true
            }
        }
    })
}

fn io_ingress_received_payload_schema_v1() -> serde_json::Value {
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

fn thread_reload_degraded_payload_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["thread_id", "missing", "fallback"],
        "additionalProperties": false,
        "properties": {
            "thread_id": {"type": "string"},
            "missing": {
                "type": "array",
                "items": {"type": "string"}
            },
            "fallback": {"enum": ["fabricated_root"]}
        }
    })
}

fn io_ingress_claimed_payload_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": [
            "ingress_envelope_ids",
            "ingress_witness_event_ids",
            "admission_event_id",
            "intent"
        ],
        "additionalProperties": false,
        "properties": {
            "ingress_envelope_ids": string_array_schema_v1(),
            "ingress_witness_event_ids": string_array_schema_v1(),
            "admission_event_id": {"type": "string"},
            "intent": {
                "type": "object",
                "required": ["outcome"],
                "additionalProperties": true,
                "properties": {
                    "outcome": {"enum": ["turn", "fork", "interrupt", "observe", "reject"]}
                }
            }
        }
    })
}

fn io_ingress_settled_payload_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["claim_event_id", "ingress_envelope_ids", "settled_by"],
        "additionalProperties": false,
        "properties": {
            "claim_event_id": {"type": "string"},
            "ingress_envelope_ids": string_array_schema_v1(),
            "evidence_event_id": {"type": "string"},
            "settled_by": {"enum": ["execution", "recovery"]}
        }
    })
}

fn string_array_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "items": {"type": "string"}
    })
}

fn io_egress_requested_payload_schema_v1() -> serde_json::Value {
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

fn io_egress_delivered_payload_schema_v1() -> serde_json::Value {
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

fn io_egress_failed_payload_schema_v1() -> serde_json::Value {
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

fn admission_decided_payload_schema_v1() -> serde_json::Value {
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

fn grant_set_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "items": {"type": "string"}
    })
}

fn event_id_array_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "items": {"type": "string"}
    })
}

fn admission_decision_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "enum": ["queue", "steer", "interrupt", "fork", "observe", "reject", "coalesce"]
    })
}

fn debug_thread_export_schema_v1() -> serde_json::Value {
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

fn debug_export_range_schema_v1() -> serde_json::Value {
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

fn nullable_stream_cursor_schema_v1() -> serde_json::Value {
    let mut schema = stream_cursor_schema_v1();
    schema
        .as_object_mut()
        .unwrap()
        .insert("type".to_string(), serde_json::json!(["object", "null"]));
    schema
}

fn debug_export_stream_record_schema_v1() -> serde_json::Value {
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

fn debug_export_receipt_schema_v1() -> serde_json::Value {
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

fn debug_export_backend_schema_v1() -> serde_json::Value {
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

fn debug_export_ack_classes_schema_v1() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "items": {"enum": ["local_committed", "query_projected"]}
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ObservationId(uuid::Uuid);

impl ObservationId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ObservationSourceRange {
    pub stream_id: EventStreamId,
    pub from_sequence: EventSequence,
    pub to_sequence: EventSequence,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NewObservationRecord {
    pub id: ObservationId,
    pub kind: String,
    pub scope: verlet_runtime_contracts::ThreadCoordinates,
    pub payload: serde_json::Value,
    pub created_at_ms: i64,
    pub provenance: ObservationProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<ObservationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

impl NewObservationRecord {
    pub fn new(
        kind: impl Into<String>,
        scope: verlet_runtime_contracts::ThreadCoordinates,
        payload: serde_json::Value,
    ) -> Self {
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ObservationRecord {
    pub id: ObservationId,
    pub kind: String,
    pub scope: verlet_runtime_contracts::ThreadCoordinates,
    pub payload: serde_json::Value,
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderApi {
    OpenAIResponses,
    OpenAIChatCompletions,
    AnthropicMessages,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingProvider {
    Anthropic,
    OpenAIResponses,
    OpenAICompatible,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
        value: serde_json::Value,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheTtl {
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
        arguments: serde_json::Value,
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

    pub fn tool_call(
        id: impl Into<String>,
        name: impl Into<String>,
        arguments: serde_json::Value,
    ) -> Self {
        Self::ToolCall {
            id: id.into(),
            name: name.into(),
            arguments,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
        Self::user_text_at(text, now_ms())
    }

    /// Builds user text at a persisted source time so "assembly is deterministic"
    /// and synthetic context never depends on the assembly-time clock.
    pub fn user_text_at(text: impl Into<String>, timestamp_ms: i64) -> Self {
        Self::User {
            content: vec![CanonicalContent::text(text)],
            timestamp_ms,
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, strum::AsRefStr)]
#[serde(tag = "type", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
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
        payload: serde_json::Value,
    },
    CustomContextMessage {
        message: CanonicalMessage,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionEntry {
    pub entry_id: SessionEntryId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_entry_id: Option<SessionEntryId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub created_at_ms: i64,
    pub kind: SessionEntryKind,
}

impl SessionEntry {
    pub fn new(
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> Self {
        Self {
            entry_id: SessionEntryId::new(),
            parent_entry_id,
            turn_id: None,
            coordinates,
            created_at_ms: now_ms(),
            kind,
        }
    }

    pub fn for_turn(
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        turn_id: impl Into<String>,
        kind: SessionEntryKind,
    ) -> Self {
        let mut entry = Self::new(coordinates, parent_entry_id, kind);
        entry.turn_id = Some(turn_id.into());
        entry
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionContext {
    pub entries: Vec<SessionEntry>,
    pub messages: Vec<CanonicalMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_cuts: Vec<SessionContextSourceCut>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionContextSourceCut {
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub stream_id: EventStreamId,
    #[serde(default)]
    pub inherited: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entry_ids: Vec<SessionEntryId>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ThreadBaseRef {
    pub child_thread_id: verlet_runtime_contracts::ThreadId,
    pub parent_thread_id: verlet_runtime_contracts::ThreadId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_checkpoint_id: Option<verlet_runtime_contracts::ThreadCheckpointId>,
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

#[async_trait::async_trait]
pub trait SessionStore: Send + Sync {
    async fn append(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry>;

    async fn append_with_provenance(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry>;

    /// Persist a turn's input exactly once. Replaying the same `turn_id`
    /// adopts the original entry; a different payload for that id fails
    /// closed.
    async fn append_turn_input(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: &str,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry>;

    async fn active_leaf(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>>;

    async fn select_branch(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()>;

    async fn build_context(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> HistoryResult<SessionContext>;

    async fn clone_branch(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>>;

    async fn fork_by_reference(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> HistoryResult<()>;
}

#[async_trait::async_trait]
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

#[async_trait::async_trait]
pub trait ObservationStore: Send + Sync {
    async fn append_observation(
        &self,
        record: NewObservationRecord,
    ) -> HistoryResult<ObservationRecord>;

    async fn list_observations(
        &self,
        scope: &verlet_runtime_contracts::ThreadCoordinates,
        kind: Option<&str>,
    ) -> HistoryResult<Vec<ObservationRecord>>;
}

pub trait RuntimeStore: SessionStore + EventStore + ObservationStore {}

impl<T> RuntimeStore for T where T: SessionStore + EventStore + ObservationStore + Send + Sync {}

#[derive(Clone, Default)]
pub struct InMemorySessionStore {
    inner: std::sync::Arc<tokio::sync::RwLock<InMemorySessionStoreInner>>,
}

#[derive(Default)]
struct InMemorySessionStoreInner {
    entries: std::collections::HashMap<
        verlet_runtime_contracts::ThreadId,
        std::collections::HashMap<SessionEntryId, SessionEntry>,
    >,
    active_leaf: std::collections::HashMap<verlet_runtime_contracts::ThreadId, SessionEntryId>,
    bases: std::collections::HashMap<verlet_runtime_contracts::ThreadId, ThreadBaseRef>,
    events: std::collections::HashMap<EventStreamId, Vec<EventRecord>>,
    event_ids: std::collections::HashSet<EventRecordId>,
    observations:
        std::collections::HashMap<verlet_runtime_contracts::ThreadId, Vec<ObservationRecord>>,
}

impl InMemorySessionStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl SessionStore for InMemorySessionStore {
    async fn append(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.append_inner(coordinates, parent_entry_id, kind, None)
            .await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry> {
        self.append_inner(coordinates, parent_entry_id, kind, Some(provenance))
            .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: &str,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        let mut inner = self.inner.write().await;
        let thread_id = coordinates.thread_id;
        if let Some(existing) = inner.entries.get(&thread_id).and_then(|entries| {
            entries
                .values()
                .find(|entry| entry.turn_id.as_deref() == Some(turn_id))
        }) {
            validate_entry_coordinates(coordinates, existing)?;
            if !turn_input_kinds_match(&existing.kind, &kind) {
                return Err(HistoryError::Storage(format!(
                    "turn {turn_id} input does not match its persisted session entry"
                )));
            }
            return Ok(existing.clone());
        }
        let parent_entry_id = inner.active_leaf.get(&thread_id).copied();
        let entry = SessionEntry::for_turn(coordinates.clone(), parent_entry_id, turn_id, kind);
        inner
            .entries
            .entry(thread_id)
            .or_default()
            .insert(entry.entry_id, entry.clone());
        inner.active_leaf.insert(thread_id, entry.entry_id);
        append_in_memory_event(
            &mut inner,
            &EventStreamId::for_thread(coordinates),
            session_entry_event(&entry),
        )?;
        Ok(entry)
    }

    async fn active_leaf(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
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
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()> {
        let mut inner = self.inner.write().await;
        let prior_entry_id = inner.active_leaf.get(&coordinates.thread_id).copied();
        if let Some(leaf_entry_id) = leaf_entry_id {
            let entries_by_id = inner
                .entries
                .get(&coordinates.thread_id)
                .ok_or(HistoryError::EntryNotFound(leaf_entry_id))?;
            branch_path(entries_by_id, leaf_entry_id, coordinates)?;
        }
        let payload = serde_json::to_value(ThreadBranchSelectedPayload {
            thread_id: coordinates.thread_id,
            selected_entry_id: leaf_entry_id,
            prior_entry_id,
        })
        .map_err(codec_error)?;
        append_in_memory_event(
            &mut inner,
            &EventStreamId::for_thread(coordinates),
            NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::ThreadBranchSelected,
                payload,
            ),
        )?;
        match leaf_entry_id {
            Some(leaf_entry_id) => {
                inner
                    .active_leaf
                    .insert(coordinates.thread_id, leaf_entry_id);
            }
            None => {
                inner.active_leaf.remove(&coordinates.thread_id);
            }
        }
        Ok(())
    }

    async fn build_context(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> HistoryResult<SessionContext> {
        let inner = self.inner.read().await;
        build_in_memory_context(
            &inner,
            coordinates,
            None,
            false,
            &mut std::collections::HashSet::new(),
        )
    }

    async fn clone_branch(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
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
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
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
                &mut std::collections::HashSet::new(),
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
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
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

#[async_trait::async_trait]
impl EventStore for InMemorySessionStore {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let mut inner = self.inner.write().await;
        append_in_memory_events(&mut inner, stream_id, records)
    }

    async fn append_events_fenced(
        &self,
        stream_id: &EventStreamId,
        expected_next_sequence: EventSequence,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let mut inner = self.inner.write().await;
        let actual_next_sequence = inner
            .events
            .get(stream_id)
            .map(|events| events.len() as i64)
            .unwrap_or_default()
            + 1;
        if actual_next_sequence != expected_next_sequence.get() {
            return Err(HistoryError::AppendFenceConflict {
                stream_id: stream_id.clone(),
                expected_next_sequence: expected_next_sequence.get(),
                actual_next_sequence,
            });
        }
        append_in_memory_events(&mut inner, stream_id, records)
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

fn append_in_memory_events(
    inner: &mut InMemorySessionStoreInner,
    stream_id: &EventStreamId,
    records: Vec<NewEventRecord>,
) -> HistoryResult<Vec<EventRecord>> {
    let mut batch_ids = std::collections::HashSet::with_capacity(records.len());
    for record in &records {
        validate_new_event(record)?;
        if inner.event_ids.contains(&record.id) || !batch_ids.insert(record.id) {
            return Err(HistoryError::DuplicateEventId(record.id));
        }
    }
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
    inner.event_ids.extend(batch_ids);
    Ok(appended)
}

#[async_trait::async_trait]
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
        scope: &verlet_runtime_contracts::ThreadCoordinates,
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
    entries_by_id: &std::collections::HashMap<SessionEntryId, SessionEntry>,
    leaf_entry_id: SessionEntryId,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
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
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    local_leaf_override: Option<SessionEntryId>,
    inherited: bool,
    visiting: &mut std::collections::HashSet<verlet_runtime_contracts::ThreadId>,
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
    strip_thread_start_identity_entries(&mut entries, &mut source_cuts);
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
    child_thread_id: verlet_runtime_contracts::ThreadId,
    parent_thread_id: verlet_runtime_contracts::ThreadId,
) -> HistoryResult<()> {
    let mut cursor = Some(parent_thread_id);
    let mut visited = std::collections::HashSet::new();
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
    source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
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
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    thread_id: verlet_runtime_contracts::ThreadId,
) -> verlet_runtime_contracts::ThreadCoordinates {
    verlet_runtime_contracts::ThreadCoordinates {
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
                messages.push(compaction_summary_message(summary, entry.created_at_ms));
            }
            SessionEntryKind::ModelChange { .. }
            | SessionEntryKind::BranchSummary { .. }
            | SessionEntryKind::Runtime { .. } => {}
        }
    }
}

pub fn session_entry_is_thread_start_identity(entry: &SessionEntry) -> bool {
    matches!(
        &entry.kind,
        SessionEntryKind::Runtime { kind, .. } if kind == "thread_started"
    )
}

pub fn strip_thread_start_identity_entries(
    entries: &mut Vec<SessionEntry>,
    source_cuts: &mut Vec<SessionContextSourceCut>,
) {
    let identity_entry_ids = entries
        .iter()
        .filter(|entry| session_entry_is_thread_start_identity(entry))
        .map(|entry| entry.entry_id)
        .collect::<std::collections::HashSet<_>>();
    if identity_entry_ids.is_empty() {
        return;
    }
    entries.retain(|entry| !identity_entry_ids.contains(&entry.entry_id));
    for cut in source_cuts.iter_mut() {
        cut.entry_ids
            .retain(|entry_id| !identity_entry_ids.contains(entry_id));
    }
    source_cuts.retain(|cut| !cut.entry_ids.is_empty());
}

fn append_in_memory_event(
    inner: &mut InMemorySessionStoreInner,
    stream_id: &EventStreamId,
    record: NewEventRecord,
) -> HistoryResult<EventRecord> {
    validate_new_event(&record)?;
    if inner.event_ids.contains(&record.id) {
        return Err(HistoryError::DuplicateEventId(record.id));
    }
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
    inner.event_ids.insert(event.id);
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
        "entry_kind": entry.kind.as_ref(),
    });
    if let Some(turn_id) = &entry.turn_id
        && let Some(object) = payload.as_object_mut()
    {
        object.insert(
            "turn_id".to_string(),
            serde_json::Value::String(turn_id.clone()),
        );
    }
    if let SessionEntryKind::Runtime {
        kind,
        payload: runtime_payload,
    } = &entry.kind
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("runtime_kind".to_string(), serde_json::json!(kind));
        object.insert("runtime_payload".to_string(), runtime_payload.clone());
    }
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

pub fn turn_input_kinds_match(left: &SessionEntryKind, right: &SessionEntryKind) -> bool {
    match (left, right) {
        (
            SessionEntryKind::Message {
                message: CanonicalMessage::User { content: left, .. },
            },
            SessionEntryKind::Message {
                message: CanonicalMessage::User { content: right, .. },
            },
        ) => left == right,
        _ => left == right,
    }
}

pub fn parse_uuid(value: &str) -> HistoryResult<uuid::Uuid> {
    uuid::Uuid::parse_str(value).map_err(codec_error)
}

pub fn parse_thread_id(value: &str) -> HistoryResult<verlet_runtime_contracts::ThreadId> {
    verlet_runtime_contracts::ThreadId::parse_str(value).map_err(codec_error)
}

pub fn validate_new_event(record: &NewEventRecord) -> HistoryResult<()> {
    if record.origin == EventOrigin::Discharged && record.provenance.is_empty() {
        return Err(HistoryError::DischargedWithoutProvenance(record.id));
    }
    Ok(())
}

pub fn validate_entry_coordinates(
    requested: &verlet_runtime_contracts::ThreadCoordinates,
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests;
