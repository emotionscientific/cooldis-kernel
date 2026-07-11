use super::ThreadEvent;
use super::runtime_utils::unix_timestamp_ms;
use crate::CompactionTrigger;
use crate::agent::hooks::{HookEventName, HookRunStatus};
use crate::kernel::context_compiler::AgentContextCompilationDiagnostics;
use crate::kernel::history::CanonicalStopReason;
use cooldis_runtime_contracts::{
    RuntimeApprovalDecision, RuntimeEventId, RuntimeModelRequestErrorClass,
    RuntimeModelRequestMode, RuntimeModelRequestPurpose, RuntimePermissionDecision,
    RuntimeTerminalState, RuntimeToolLogLevel, RuntimeUsage, ThreadCheckpointId, ThreadCoordinates,
    ThreadId, ThreadInteractionKind, ThreadLifecycleStatus, ThreadTopology,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::broadcast;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeEvent {
    pub id: RuntimeEventId,
    pub coordinates: ThreadCoordinates,
    pub created_at_ms: u64,
    pub kind: RuntimeEventKind,
}

impl RuntimeEvent {
    pub fn new(coordinates: ThreadCoordinates, kind: RuntimeEventKind) -> Self {
        Self {
            id: RuntimeEventId::new(),
            coordinates,
            created_at_ms: unix_timestamp_ms(),
            kind,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEventKind {
    ThreadStarted {
        parent_thread_id: Option<ThreadId>,
        #[serde(default)]
        topology: ThreadTopology,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, String>,
    },
    ThreadInteraction {
        interaction_id: RuntimeEventId,
        kind: ThreadInteractionKind,
        source_thread_id: ThreadId,
        target_thread_id: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result_preview: Option<String>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, String>,
    },
    TextDelta {
        text: String,
    },
    ThinkingDelta {
        text: String,
    },
    ToolCallStarted {
        call_id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolCallResult {
        call_id: String,
        output: String,
        success: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
    },
    ToolLog {
        call_id: String,
        tool_name: String,
        level: RuntimeToolLogLevel,
        message: String,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        metadata: BTreeMap<String, String>,
    },
    HookStarted {
        // lexicon-allow: hook - stable runtime event field for existing hook integration.
        hook_id: String,
        event_name: HookEventName,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        matcher: Option<String>,
    },
    HookCompleted {
        // lexicon-allow: hook - stable runtime event field for existing hook integration.
        hook_id: String,
        event_name: HookEventName,
        status: HookRunStatus,
        duration_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    ApprovalRequested {
        approval_id: String,
        action: String,
        metadata: BTreeMap<String, String>,
    },
    ApprovalResolved {
        approval_id: String,
        decision: RuntimeApprovalDecision,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    PermissionDecision {
        call_id: String,
        tool_name: String,
        decision: RuntimePermissionDecision,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    ContextCompiled {
        diagnostics: AgentContextCompilationDiagnostics,
        provider_dropped_messages: usize,
        provider_truncated_text_bytes: usize,
        provider_retained_text_bytes: usize,
    },
    ModelRequestStarted {
        request_id: String,
        turn_id: String,
        provider: String,
        api: String,
        model: String,
        mode: RuntimeModelRequestMode,
        purpose: RuntimeModelRequestPurpose,
        system_block_count: usize,
        message_count: usize,
        tool_count: usize,
        max_tokens: u32,
    },
    ModelRequestRetryScheduled {
        request_id: String,
        next_request_id: String,
        turn_id: String,
        provider: String,
        api: String,
        model: String,
        mode: RuntimeModelRequestMode,
        purpose: RuntimeModelRequestPurpose,
        attempt: u32,
        next_attempt: u32,
        delay_ms: u64,
        error_class: RuntimeModelRequestErrorClass,
        error: String,
    },
    ModelRequestFallbackSelected {
        request_id: String,
        turn_id: String,
        from_provider: String,
        from_api: String,
        from_model: String,
        to_provider: String,
        to_api: String,
        to_model: String,
        mode: RuntimeModelRequestMode,
        purpose: RuntimeModelRequestPurpose,
        error_class: RuntimeModelRequestErrorClass,
        error: String,
    },
    ModelRequestCompleted {
        request_id: String,
        turn_id: String,
        provider: String,
        api: String,
        model: String,
        mode: RuntimeModelRequestMode,
        purpose: RuntimeModelRequestPurpose,
        duration_ms: u64,
        usage: RuntimeUsage,
        stop_reason: CanonicalStopReason,
    },
    ModelRequestFailed {
        request_id: String,
        turn_id: String,
        provider: String,
        api: String,
        model: String,
        mode: RuntimeModelRequestMode,
        purpose: RuntimeModelRequestPurpose,
        duration_ms: u64,
        error_class: RuntimeModelRequestErrorClass,
        error: String,
    },
    Terminal {
        state: RuntimeTerminalState,
    },
    Timeout {
        operation: String,
        timeout_ms: u64,
    },
    PolicyRejected {
        code: String,
        message: String,
    },
    Recovery {
        action: String,
        reason: String,
    },
    Usage {
        usage: RuntimeUsage,
    },
    SubthreadStarted {
        child_thread_id: ThreadId,
    },
    SubthreadFinished {
        child_thread_id: ThreadId,
        status: ThreadLifecycleStatus,
    },
    Checkpoint {
        checkpoint_id: ThreadCheckpointId,
        label: Option<String>,
    },
    Compaction {
        trigger: CompactionTrigger,
        summary: String,
    },
    Cancelled {
        reason: String,
    },
    Failed {
        code: String,
        message: String,
    },
}

pub fn emit_runtime_event(
    events: &broadcast::Sender<ThreadEvent>,
    coordinates: &ThreadCoordinates,
    kind: RuntimeEventKind,
) {
    let event = RuntimeEvent::new(coordinates.clone(), kind);
    let _ = events.send(ThreadEvent::Runtime {
        thread_id: coordinates.thread_id,
        event,
    });
}
