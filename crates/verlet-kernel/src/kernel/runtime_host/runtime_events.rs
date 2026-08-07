#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeEvent {
    pub id: verlet_runtime_contracts::RuntimeEventId,
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub created_at_ms: u64,
    pub kind: RuntimeEventKind,
}

impl RuntimeEvent {
    pub fn new(
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        kind: RuntimeEventKind,
    ) -> Self {
        Self {
            id: verlet_runtime_contracts::RuntimeEventId::new(),
            coordinates,
            created_at_ms: crate::kernel::runtime_host::runtime_utils::unix_timestamp_ms(),
            kind,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEventKind {
    ThreadStarted {
        parent_thread_id: Option<verlet_runtime_contracts::ThreadId>,
        #[serde(default)]
        topology: verlet_runtime_contracts::ThreadTopology,
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        metadata: std::collections::BTreeMap<String, String>,
    },
    ThreadInteraction {
        interaction_id: verlet_runtime_contracts::RuntimeEventId,
        kind: verlet_runtime_contracts::ThreadInteractionKind,
        source_thread_id: verlet_runtime_contracts::ThreadId,
        target_thread_id: verlet_runtime_contracts::ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result_preview: Option<String>,
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        metadata: std::collections::BTreeMap<String, String>,
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
        level: verlet_runtime_contracts::RuntimeToolLogLevel,
        message: String,
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        metadata: std::collections::BTreeMap<String, String>,
    },
    HookStarted {
        // lexicon-allow: hook - stable runtime event field for existing hook integration.
        hook_id: String,
        event_name: crate::agent::hooks::HookEventName,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        matcher: Option<String>,
    },
    HookCompleted {
        // lexicon-allow: hook - stable runtime event field for existing hook integration.
        hook_id: String,
        event_name: crate::agent::hooks::HookEventName,
        status: crate::agent::hooks::HookRunStatus,
        duration_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    ApprovalRequested {
        approval_id: String,
        action: String,
        metadata: std::collections::BTreeMap<String, String>,
    },
    ApprovalResolved {
        approval_id: String,
        decision: verlet_runtime_contracts::RuntimeApprovalDecision,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    PermissionDecision {
        call_id: String,
        tool_name: String,
        decision: verlet_runtime_contracts::RuntimePermissionDecision,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    ContextCompiled {
        diagnostics: crate::kernel::context_compiler::AgentContextCompilationDiagnostics,
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
        mode: verlet_runtime_contracts::RuntimeModelRequestMode,
        purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose,
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
        mode: verlet_runtime_contracts::RuntimeModelRequestMode,
        purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose,
        attempt: u32,
        next_attempt: u32,
        delay_ms: u64,
        error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass,
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
        mode: verlet_runtime_contracts::RuntimeModelRequestMode,
        purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose,
        error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass,
        error: String,
    },
    ModelRequestCompleted {
        request_id: String,
        turn_id: String,
        provider: String,
        api: String,
        model: String,
        mode: verlet_runtime_contracts::RuntimeModelRequestMode,
        purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose,
        duration_ms: u64,
        usage: verlet_runtime_contracts::RuntimeUsage,
        stop_reason: verlet_history::CanonicalStopReason,
    },
    ModelRequestFailed {
        request_id: String,
        turn_id: String,
        provider: String,
        api: String,
        model: String,
        mode: verlet_runtime_contracts::RuntimeModelRequestMode,
        purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose,
        duration_ms: u64,
        error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass,
        error: String,
    },
    Terminal {
        state: verlet_runtime_contracts::RuntimeTerminalState,
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
        usage: verlet_runtime_contracts::RuntimeUsage,
    },
    SubthreadStarted {
        child_thread_id: verlet_runtime_contracts::ThreadId,
    },
    SubthreadFinished {
        child_thread_id: verlet_runtime_contracts::ThreadId,
        status: verlet_runtime_contracts::ThreadLifecycleStatus,
    },
    Checkpoint {
        checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
        label: Option<String>,
    },
    Compaction {
        trigger: crate::kernel::compaction::CompactionTrigger,
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
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    kind: RuntimeEventKind,
) {
    let event = RuntimeEvent::new(coordinates.clone(), kind);
    let _ = events.send(
        crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime {
            thread_id: coordinates.thread_id,
            event,
        },
    );
}
