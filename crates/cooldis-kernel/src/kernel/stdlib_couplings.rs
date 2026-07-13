use crate::{
    CONTEXT_READ_PLAN_SCHEMA_V1, CooldisError, CooldisResult, CouplingDischarge,
    CouplingExecutionResult, CouplingExecutor, CouplingInvocation, CouplingRunReceipt,
    CouplingRunStatus, EventKind, EventRecord, EventRecordId, EventSequence, MandateStartedPayload,
    MandateSubject, ObservationSourceRange, THREADS_SPAWN_CAPABILITY, ThreadSpawnRequestedPayload,
    TimerFiredPayload, ToolCallDecisionOutcomePayload, ToolCallDecisionPayload,
    ToolCallRequestedPayload, ToolCallSuspendedPayload, TurnContinuationSubject,
    TurnContinueRequestedPayload, agent::contracts::sha256_hex,
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

pub const STD_QUEUE_TASK_TEMPLATE_ID: &str = "std::queue.task";
pub const STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID: &str = "std::queue.completion_callback";
pub const STD_CONTEXT_SPILL_TEMPLATE_ID: &str = "std::context.spill";
pub const STD_CONTEXT_TRUNCATE_TEMPLATE_ID: &str = "std::context.truncate";
pub const STD_CONTEXT_SUMMARIZE_TEMPLATE_ID: &str = "std::context.summarize";
// lexicon-allow: memory - V1 stdlib product alias lowered to derived context events.
pub const STD_MEMORY_EXTRACT_TEMPLATE_ID: &str = "std::memory.extract";
// lexicon-allow: memory - V1 stdlib product alias lowered to derived context events.
pub const STD_MEMORY_RECALL_TEMPLATE_ID: &str = "std::memory.recall";
pub const STD_PROMPT_STEER_TEMPLATE_ID: &str = "std::prompt.steer";
pub const STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID: &str = "std::prompt.dynamic_instructions";
pub const STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID: &str = "std::permission.approval_gate";
pub const STD_PERMISSION_TOOL_GATE_TEMPLATE_ID: &str = "std::permission.tool_gate";
pub const STD_SCHEDULE_CRON_TEMPLATE_ID: &str = "std::schedule.cron";
pub const STD_SUPERVISOR_SPAWN_TEMPLATE_ID: &str = "std::supervisor.spawn";
pub const STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID: &str = "std::supervisor.child_completion";
pub const STD_RETRY_WITH_BUDGET_TEMPLATE_ID: &str = "std::retry.with_budget";
pub const STD_FAILURE_DEADLETTER_TEMPLATE_ID: &str = "std::failure.deadletter";

#[derive(Clone, Copy, Debug, Default)]
pub struct StdlibCouplingExecutor;

impl StdlibCouplingExecutor {
    pub fn supports_template(id: &str) -> bool {
        matches!(
            id,
            STD_QUEUE_TASK_TEMPLATE_ID
                | STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID
                | STD_CONTEXT_SPILL_TEMPLATE_ID
                | STD_CONTEXT_TRUNCATE_TEMPLATE_ID
                | STD_CONTEXT_SUMMARIZE_TEMPLATE_ID
                | STD_MEMORY_EXTRACT_TEMPLATE_ID
                | STD_MEMORY_RECALL_TEMPLATE_ID
                | STD_PROMPT_STEER_TEMPLATE_ID
                | STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID
                | STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID
                | STD_PERMISSION_TOOL_GATE_TEMPLATE_ID
                | STD_SCHEDULE_CRON_TEMPLATE_ID
                | STD_SUPERVISOR_SPAWN_TEMPLATE_ID
                | STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID
                | STD_RETRY_WITH_BUDGET_TEMPLATE_ID
                | STD_FAILURE_DEADLETTER_TEMPLATE_ID
        )
    }
}

#[async_trait]
impl CouplingExecutor for StdlibCouplingExecutor {
    async fn invoke(&self, request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
        match request.coupling.id.as_str() {
            STD_QUEUE_TASK_TEMPLATE_ID => invoke_queue_task(request),
            STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID => invoke_queue_completion_callback(request),
            STD_CONTEXT_SPILL_TEMPLATE_ID => invoke_context_spill(request),
            STD_CONTEXT_TRUNCATE_TEMPLATE_ID => invoke_context_truncate(request),
            STD_CONTEXT_SUMMARIZE_TEMPLATE_ID => invoke_context_summarize(request),
            STD_MEMORY_EXTRACT_TEMPLATE_ID => invoke_memory_extract(request),
            STD_MEMORY_RECALL_TEMPLATE_ID => invoke_memory_recall(request),
            STD_PROMPT_STEER_TEMPLATE_ID => invoke_prompt_steer(request),
            STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID => {
                invoke_prompt_dynamic_instructions(request)
            }
            STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID => invoke_permission_approval_gate(request),
            STD_PERMISSION_TOOL_GATE_TEMPLATE_ID => invoke_permission_tool_gate(request),
            STD_SCHEDULE_CRON_TEMPLATE_ID => invoke_schedule_cron(request),
            STD_SUPERVISOR_SPAWN_TEMPLATE_ID => invoke_supervisor_spawn(request),
            STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID => {
                invoke_supervisor_child_completion(request)
            }
            STD_RETRY_WITH_BUDGET_TEMPLATE_ID => invoke_retry_with_budget(request),
            STD_FAILURE_DEADLETTER_TEMPLATE_ID => invoke_failure_deadletter(request),
            id => Err(CooldisError::RuntimeFactory(format!(
                "stdlib coupling executor does not implement template {id:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct QueueTaskConfig {
    task_prefix: Option<String>,
    reason: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct ContextSpillConfig {
    summary_event_id: Option<EventRecordId>,
    summary_text: Option<String>,
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct ContextTruncateConfig {
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
    retain_tail_events: u32,
    reason: Option<String>,
}

impl Default for ContextTruncateConfig {
    fn default() -> Self {
        Self {
            read_plan_name: None,
            pipeline_id: None,
            retain_tail_events: 8,
            reason: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct ContextSummarizeConfig {
    summary_event_id: Option<EventRecordId>,
    summary_text: Option<String>,
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
    reason: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct MemoryExtractConfig {
    #[serde(alias = "memory_event_id")]
    checkpoint_event_id: Option<EventRecordId>,
    #[serde(alias = "memory_text")]
    text: Option<String>,
    #[serde(alias = "memory_kind")]
    observation_kind: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct MemoryRecallConfig {
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
    max_events: usize,
    #[serde(alias = "memory_kind")]
    observation_kind: Option<String>,
}

impl Default for MemoryRecallConfig {
    fn default() -> Self {
        Self {
            read_plan_name: None,
            pipeline_id: None,
            max_events: 8,
            observation_kind: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct PromptDynamicInstructionsConfig {
    instruction_event_id: Option<EventRecordId>,
    instruction_text: Option<String>,
    instruction_name: Option<String>,
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct PromptSteerConfig {
    action: PromptSteerAction,
    reason: Option<String>,
    loop_id: Option<String>,
    parent_turn_id: Option<String>,
    next_turn_input: Option<String>,
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
    checkpoint_event_id: Option<EventRecordId>,
    checkpoint_stream_id: Option<String>,
    event_role: Option<String>,
}

impl Default for PromptSteerConfig {
    fn default() -> Self {
        Self {
            action: PromptSteerAction::RequestContinuation,
            reason: None,
            loop_id: None,
            parent_turn_id: None,
            next_turn_input: None,
            read_plan_name: None,
            pipeline_id: None,
            checkpoint_event_id: None,
            checkpoint_stream_id: None,
            event_role: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PromptSteerAction {
    #[default]
    RequestContinuation,
    SetReadPlan,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct PermissionToolGateConfig {
    decision: PermissionToolGateDecision,
    reason: Option<String>,
    approval_id: Option<String>,
    arguments: Option<JsonValue>,
}

impl Default for PermissionToolGateConfig {
    fn default() -> Self {
        Self {
            decision: PermissionToolGateDecision::Allow,
            reason: None,
            approval_id: None,
            arguments: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct PermissionApprovalGateConfig {
    approval_id: Option<String>,
    reason: Option<String>,
    resume_token: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PermissionToolGateDecision {
    #[default]
    Allow,
    Rewrite,
    Deny,
    Wait,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct FailureDeadletterConfig {
    reason: Option<String>,
    queue: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct RetryWithBudgetConfig {
    max_attempts: u32,
    loop_id: Option<String>,
    parent_turn_id: Option<String>,
    next_turn_input: Option<String>,
    retryable_error_classes: Vec<String>,
    reason: Option<String>,
}

impl Default for RetryWithBudgetConfig {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            loop_id: None,
            parent_turn_id: None,
            next_turn_input: None,
            retryable_error_classes: Vec::new(),
            reason: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct ScheduleCronConfig {
    max_occurrences: u32,
    mandate_scope: ScheduleCronMandateScope,
    loop_id: Option<String>,
    parent_turn_id: Option<String>,
    next_turn_input: Option<String>,
    schedule_id: Option<String>,
    reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScheduleCronMandateScope {
    MatchAll,
    Subject(MandateSubject),
}

impl Default for ScheduleCronMandateScope {
    fn default() -> Self {
        Self::MatchAll
    }
}

impl ScheduleCronMandateScope {
    fn matches(&self, subject: &MandateSubject) -> bool {
        match self {
            Self::MatchAll => true,
            Self::Subject(expected) => expected == subject,
        }
    }
}

impl Default for ScheduleCronConfig {
    fn default() -> Self {
        Self {
            max_occurrences: 1,
            mandate_scope: ScheduleCronMandateScope::default(),
            loop_id: None,
            parent_turn_id: None,
            next_turn_input: None,
            schedule_id: None,
            reason: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct QueueCompletionCallbackConfig {
    watch_coupling_id: Option<String>,
    on_completed: QueueCompletionAction,
    reason: Option<String>,
    loop_id: Option<String>,
    parent_turn_id: Option<String>,
    next_turn_input: Option<String>,
}

impl Default for QueueCompletionCallbackConfig {
    fn default() -> Self {
        Self {
            watch_coupling_id: Some(STD_QUEUE_TASK_TEMPLATE_ID.to_string()),
            on_completed: QueueCompletionAction::CompleteLoop,
            reason: None,
            loop_id: None,
            parent_turn_id: None,
            next_turn_input: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct SupervisorSpawnConfig {
    #[serde(alias = "agent_ref")]
    child_agent_ref: Option<String>,
    #[serde(alias = "message")]
    initial_submission: Option<String>,
    parent_turn_id: Option<String>,
    correlation_id: Option<String>,
    block_parent: bool,
    reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct SupervisorChildCompletionConfig {
    watch_coupling_id: Option<String>,
    on_completed: SupervisorChildCompletionAction,
    reason: Option<String>,
    loop_id: Option<String>,
    parent_turn_id: Option<String>,
    next_turn_input: Option<String>,
    parent_thread_id: Option<String>,
    child_thread_id: Option<String>,
    child_turn_id: Option<String>,
}

impl Default for SupervisorChildCompletionConfig {
    fn default() -> Self {
        Self {
            watch_coupling_id: Some(STD_SUPERVISOR_SPAWN_TEMPLATE_ID.to_string()),
            on_completed: SupervisorChildCompletionAction::CompleteLoop,
            reason: None,
            loop_id: None,
            parent_turn_id: None,
            next_turn_input: None,
            parent_thread_id: None,
            child_thread_id: None,
            child_turn_id: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum SupervisorChildCompletionAction {
    #[default]
    CompleteLoop,
    RequestContinuation,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum QueueCompletionAction {
    #[default]
    CompleteLoop,
    RequestContinuation,
}

fn invoke_queue_task(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if request.trigger_event.kind != EventKind::TurnSubmitted {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_QUEUE_TASK_TEMPLATE_ID} expected turn.submitted trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = queue_task_config(&request.coupling.config)?;
    let turn_id = request
        .trigger_event
        .payload
        .get("turn_id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            CooldisError::RuntimeFactory(
                "std::queue.task trigger payload is missing turn_id".to_string(),
            )
        })?;
    let task_prefix = config.task_prefix.as_deref().unwrap_or("task");
    let mut payload = JsonMap::from_iter([
        (
            "schema".to_string(),
            json!(EventKind::TurnWaiting.payload_schema_id()),
        ),
        ("template_id".to_string(), json!(STD_QUEUE_TASK_TEMPLATE_ID)),
        (
            "snapshot_id".to_string(),
            json!(request.activation.snapshot_id),
        ),
        ("turn_id".to_string(), json!(turn_id)),
        (
            "task_id".to_string(),
            json!(format!("{task_prefix}:{}", request.trigger_event.id)),
        ),
        (
            "waiting_on_event_id".to_string(),
            json!(request.trigger_event.id.to_string()),
        ),
        ("status".to_string(), json!("queued")),
        (
            "reason".to_string(),
            json!(
                config
                    .reason
                    .unwrap_or_else(|| "queued by std::queue.task".to_string())
            ),
        ),
    ]);
    if let Some(entry_id) = request
        .trigger_event
        .payload
        .get("entry_id")
        .and_then(|value| value.as_str())
    {
        payload.insert("entry_id".to_string(), json!(entry_id));
    }

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::TurnWaiting,
            payload: JsonValue::Object(payload),
        }],
    })
}

fn invoke_queue_completion_callback(
    request: CouplingInvocation,
) -> CooldisResult<CouplingExecutionResult> {
    if request.trigger_event.kind != EventKind::CouplingRunCompleted {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID} expected coupling.run.completed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = queue_completion_config(&request.coupling.config)?;
    let completed = serde_json::from_value::<CouplingRunReceipt>(
        request.trigger_event.payload.clone(),
    )
    .map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::queue.completion_callback trigger payload is not a coupling run receipt: {err}"
        ))
    })?;
    if completed.status != CouplingRunStatus::Completed {
        return Ok(CouplingExecutionResult::default());
    }
    if let Some(watch_coupling_id) = &config.watch_coupling_id
        && completed.coupling_id != *watch_coupling_id
    {
        return Ok(CouplingExecutionResult::default());
    }

    let discharge = match config.on_completed {
        QueueCompletionAction::CompleteLoop => CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::LoopCompleted,
            payload: json!({
                "schema": EventKind::LoopCompleted.payload_schema_id(),
                "template_id": STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID,
                "snapshot_id": request.activation.snapshot_id,
                "completed_coupling_id": completed.coupling_id,
                "completed_trigger_event_id": completed.trigger_event_id.to_string(),
                "completed_discharged_event_ids": completed
                    .discharged_event_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                "reason": config
                    .reason
                    .unwrap_or_else(|| "queued work completed".to_string()),
            }),
        },
        QueueCompletionAction::RequestContinuation => {
            let next_turn_input = config.next_turn_input.ok_or_else(|| {
                CooldisError::RuntimeFactory(
                    "std::queue.completion_callback request_continuation requires next_turn_input"
                        .to_string(),
                )
            })?;
            let parent_turn_id = config.parent_turn_id.ok_or_else(|| {
                CooldisError::RuntimeFactory(
                    "std::queue.completion_callback request_continuation requires parent_turn_id"
                        .to_string(),
                )
            })?;
            let payload = TurnContinueRequestedPayload {
                subject: TurnContinuationSubject {
                    loop_id: config.loop_id.unwrap_or_else(|| "default".to_string()),
                    parent_turn_id,
                },
                snapshot_id: request.activation.snapshot_id,
                next_turn_input,
            };
            let mut payload = serde_json::to_value(payload).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "std::queue.completion_callback continuation payload codec failed: {err}"
                ))
            })?;
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "schema".to_string(),
                    json!(EventKind::TurnContinueRequested.payload_schema_id()),
                );
                object.insert(
                    "template_id".to_string(),
                    json!(STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID),
                );
                object.insert(
                    "completed_coupling_id".to_string(),
                    json!(completed.coupling_id),
                );
            }
            CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: EventKind::TurnContinueRequested,
                payload,
            }
        }
    };

    Ok(CouplingExecutionResult {
        discharges: vec![discharge],
    })
}

fn invoke_supervisor_spawn(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if !matches!(
        request.trigger_event.kind,
        EventKind::TurnSubmitted | EventKind::ToolCallRequested
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_SUPERVISOR_SPAWN_TEMPLATE_ID} expected turn.submitted or tool.call.requested trigger, got {}",
            request.trigger_event.kind
        )));
    }
    if !request
        .coupling
        .grants
        .iter()
        .any(|grant| grant == THREADS_SPAWN_CAPABILITY)
    {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_SUPERVISOR_SPAWN_TEMPLATE_ID} requires {THREADS_SPAWN_CAPABILITY} grant"
        )));
    }

    let config = supervisor_spawn_config(&request.coupling.config)?;
    let parent_turn_id = config
        .parent_turn_id
        .or_else(|| payload_string(&request.trigger_event.payload, &["turn_id"]));
    let child_agent_ref = config
        .child_agent_ref
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unbound".to_string());
    let initial_submission = config
        .initial_submission
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            CooldisError::RuntimeFactory(
                "std::supervisor.spawn requires initial_submission".to_string(),
            )
        })?;
    let correlation_id = config
        .correlation_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            format!(
                "{}:{}",
                STD_SUPERVISOR_SPAWN_TEMPLATE_ID, request.trigger_event.id
            )
        });

    let payload = ThreadSpawnRequestedPayload {
        parent_thread_id: request.trigger_event.coordinates.thread_id,
        parent_turn_id: parent_turn_id.clone(),
        task_name: None,
        submitted_turn_id: None,
        child_agent_ref,
        initial_submission,
        correlation_id: correlation_id.clone(),
        block_parent: config.block_parent,
    };
    let mut payload = serde_json::to_value(payload).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::supervisor.spawn request payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            json!(EventKind::ThreadSpawnRequested.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            json!(STD_SUPERVISOR_SPAWN_TEMPLATE_ID),
        );
        object.insert(
            "snapshot_id".to_string(),
            json!(request.activation.snapshot_id),
        );
        object.insert(
            "trigger_event_id".to_string(),
            json!(request.trigger_event.id.to_string()),
        );
        object.insert(
            "trigger_kind".to_string(),
            json!(request.trigger_event.kind.as_str()),
        );
        object.insert(
            "reason".to_string(),
            json!(
                config
                    .reason
                    .unwrap_or_else(|| "supervisor spawn requested".to_string())
            ),
        );
    }

    let request_event_id = EventRecordId::new();
    let mut discharges = vec![CouplingDischarge {
        event_id: Some(request_event_id),
        stream: "control".to_string(),
        kind: EventKind::ThreadSpawnRequested,
        payload,
    }];

    if config.block_parent {
        let parent_turn_id = parent_turn_id.ok_or_else(|| {
            CooldisError::RuntimeFactory(
                "std::supervisor.spawn block_parent requires parent_turn_id or trigger turn_id"
                    .to_string(),
            )
        })?;
        discharges.push(CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::TurnWaiting,
            payload: json!({
                "schema": EventKind::TurnWaiting.payload_schema_id(),
                "template_id": STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
                "snapshot_id": request.activation.snapshot_id,
                "turn_id": parent_turn_id,
                "waiting_on_event_id": request_event_id.to_string(),
                "correlation_id": correlation_id,
                "status": "waiting_on_child",
                "reason": "waiting on supervised child completion",
            }),
        });
    }

    Ok(CouplingExecutionResult { discharges })
}

fn invoke_context_spill(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if request.trigger_event.kind != EventKind::ContextCompileCompleted {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_CONTEXT_SPILL_TEMPLATE_ID} expected context.compile.completed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = context_spill_config(&request.coupling.config)?;
    let source_ranges = context_spill_source_ranges(&request);
    let summary_text = config
        .summary_text
        .unwrap_or_else(|| context_spill_summary_text(&request));
    let summary_event_id = config.summary_event_id.unwrap_or_else(EventRecordId::new);
    let read_plan_name = config
        .read_plan_name
        .unwrap_or_else(|| "history.default".to_string());
    let pipeline_id = config
        .pipeline_id
        .unwrap_or_else(|| "context.default".to_string());
    let summary_hash = sha256_hex(summary_text.as_bytes());
    let summary_payload = json!({
        "schema": EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": summary_text,
        "covered_ranges": source_ranges_json(&source_ranges),
        "content": {
            "sha256": summary_hash,
        },
        "template_id": STD_CONTEXT_SPILL_TEMPLATE_ID,
        "compile_event_id": request.trigger_event.id.to_string(),
    });
    let read_plan_payload = json!({
        "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": request.trigger_event.stream_id.as_str(),
        "summary_event_id": summary_event_id.to_string(),
        "template_id": STD_CONTEXT_SPILL_TEMPLATE_ID,
        "read_plan": {
            "schema": CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": request.trigger_event.stream_id.as_str(),
            "frontier": "compile_frontier",
            "entries": summary_checkpoint_entries(summary_event_id, &source_ranges),
        },
    });

    Ok(CouplingExecutionResult {
        discharges: vec![
            CouplingDischarge {
                event_id: Some(summary_event_id),
                stream: "derived:context".to_string(),
                kind: EventKind::ContextSummaryCompleted,
                payload: summary_payload,
            },
            CouplingDischarge {
                event_id: None,
                stream: "derived:context".to_string(),
                kind: EventKind::ContextReadPlanSet,
                payload: read_plan_payload,
            },
        ],
    })
}

fn invoke_context_truncate(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if request.trigger_event.kind != EventKind::ContextCompileCompleted {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_CONTEXT_TRUNCATE_TEMPLATE_ID} expected context.compile.completed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = context_truncate_config(&request.coupling.config)?;
    let source_ranges = context_spill_source_ranges(&request);
    let retain_tail_events = i64::from(config.retain_tail_events.max(1));
    let read_plan_name = config
        .read_plan_name
        .unwrap_or_else(|| "history.default".to_string());
    let pipeline_id = config
        .pipeline_id
        .unwrap_or_else(|| "context.truncate".to_string());
    let reason = config
        .reason
        .unwrap_or_else(|| "bounded context tail selected".to_string());
    let entries = truncate_read_plan_entries(&source_ranges, retain_tail_events, &reason);
    let payload = json!({
        "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": request.trigger_event.stream_id.as_str(),
        "template_id": STD_CONTEXT_TRUNCATE_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.as_str(),
        "retain_tail_events": retain_tail_events,
        "reason": reason,
        "read_plan": {
            "schema": CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": request.trigger_event.stream_id.as_str(),
            "frontier": "compile_frontier",
            "entries": entries,
        },
    });

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::ContextReadPlanSet,
            payload,
        }],
    })
}

fn invoke_context_summarize(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if !matches!(
        request.trigger_event.kind,
        EventKind::ContextCompileCompleted | EventKind::TurnCompleted
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_CONTEXT_SUMMARIZE_TEMPLATE_ID} expected context.compile.completed or turn.completed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = context_summarize_config(&request.coupling.config)?;
    let source_ranges = context_spill_source_ranges(&request);
    let summary_text = config
        .summary_text
        .or_else(|| {
            payload_string(
                &request.trigger_event.payload,
                &["summary_text", "summary", "output_text", "text"],
            )
        })
        .unwrap_or_else(|| {
            format!(
                "Summary checkpoint from {} {}.",
                request.trigger_event.kind, request.trigger_event.id
            )
        });
    let summary_event_id = config.summary_event_id.unwrap_or_else(EventRecordId::new);
    let read_plan_name = config
        .read_plan_name
        .unwrap_or_else(|| "history.default".to_string());
    let pipeline_id = config
        .pipeline_id
        .unwrap_or_else(|| "context.summarize".to_string());
    let reason = config
        .reason
        .unwrap_or_else(|| "summary checkpoint selected".to_string());
    let content_hash = sha256_hex(summary_text.as_bytes());
    let summary_payload = json!({
        "schema": EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": summary_text,
        "covered_ranges": source_ranges_json(&source_ranges),
        "content": {
            "sha256": content_hash,
        },
        "template_id": STD_CONTEXT_SUMMARIZE_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.as_str(),
        "snapshot_id": request.activation.snapshot_id,
        "reason": reason,
    });
    let read_plan_payload = json!({
        "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": request.trigger_event.stream_id.as_str(),
        "summary_event_id": summary_event_id.to_string(),
        "template_id": STD_CONTEXT_SUMMARIZE_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.as_str(),
        "read_plan": {
            "schema": CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": request.trigger_event.stream_id.as_str(),
            "frontier": "compile_frontier",
            "entries": summary_checkpoint_entries(summary_event_id, &source_ranges),
        },
    });

    Ok(CouplingExecutionResult {
        discharges: vec![
            CouplingDischarge {
                event_id: Some(summary_event_id),
                stream: "derived:context".to_string(),
                kind: EventKind::ContextSummaryCompleted,
                payload: summary_payload,
            },
            CouplingDischarge {
                event_id: None,
                stream: "derived:context".to_string(),
                kind: EventKind::ContextReadPlanSet,
                payload: read_plan_payload,
            },
        ],
    })
}

fn invoke_memory_extract(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if !matches!(
        request.trigger_event.kind,
        EventKind::TurnCompleted | EventKind::ToolCallCompleted
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_MEMORY_EXTRACT_TEMPLATE_ID} expected turn.completed or tool.call.completed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = memory_extract_config(&request.coupling.config)?;
    let memory_text = config
        .text
        .or_else(|| memory_text_from_payload(&request.trigger_event.payload))
        .unwrap_or_else(|| {
            format!(
                "Memory extracted from {} {}.",
                request.trigger_event.kind, request.trigger_event.id
            )
        });
    let memory_kind = config
        .observation_kind
        .unwrap_or_else(|| "observation".to_string());
    let memory_event_id = config.checkpoint_event_id;
    let source_ranges = context_spill_source_ranges(&request);
    let content_hash = sha256_hex(memory_text.as_bytes());
    let payload = json!({
        "schema": EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": memory_text,
        "covered_ranges": source_ranges_json(&source_ranges),
        "content": {
            "sha256": content_hash,
        },
        "template_id": STD_MEMORY_EXTRACT_TEMPLATE_ID,
        "memory_kind": memory_kind,
        "source_event_id": request.trigger_event.id.to_string(),
        "source_kind": request.trigger_event.kind.as_str(),
        "snapshot_id": request.activation.snapshot_id,
    });

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: memory_event_id,
            stream: "derived:memory".to_string(),
            kind: EventKind::ContextSummaryCompleted,
            payload,
        }],
    })
}

fn memory_text_from_payload(payload: &JsonValue) -> Option<String> {
    [
        "memory_text",
        "output_text",
        "summary",
        "result_preview",
        "text",
    ]
    .into_iter()
    .find_map(|key| {
        payload
            .get(key)
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
    })
}

fn invoke_memory_recall(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if !matches!(
        request.trigger_event.kind,
        EventKind::TurnSubmitted | EventKind::ContextCompileCompleted
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_MEMORY_RECALL_TEMPLATE_ID} expected turn.submitted or context.compile.completed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = memory_recall_config(&request.coupling.config)?;
    let read_plan_name = config
        .read_plan_name
        .unwrap_or_else(|| "memory.default".to_string());
    let pipeline_id = config
        .pipeline_id
        .unwrap_or_else(|| "context.memory".to_string());
    let max_events = config.max_events.max(1);
    let mut memories = request
        .source_events
        .iter()
        .filter(|event| event.kind == EventKind::ContextSummaryCompleted)
        .filter(|event| memory_kind_matches(&event.payload, config.observation_kind.as_deref()))
        .collect::<Vec<_>>();
    memories.sort_by(|left, right| {
        (
            left.stream_id.to_string(),
            left.sequence.get(),
            left.id.to_string(),
        )
            .cmp(&(
                right.stream_id.to_string(),
                right.sequence.get(),
                right.id.to_string(),
            ))
    });
    if memories.len() > max_events {
        memories = memories[memories.len() - max_events..].to_vec();
    }
    if memories.is_empty() {
        return Ok(CouplingExecutionResult::default());
    }
    let source_stream = memories[0].stream_id.as_str();
    let selected_event_ids = memories
        .iter()
        .map(|event| event.id.to_string())
        .collect::<Vec<_>>();
    let entries = memories
        .iter()
        .map(|event| {
            json!({
                "kind": "event_ref",
                "stream_id": event.stream_id.as_str(),
                "event_id": event.id.to_string(),
                "event_role": "memory_checkpoint",
            })
        })
        .collect::<Vec<_>>();
    let payload = json!({
        "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": source_stream,
        "template_id": STD_MEMORY_RECALL_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.as_str(),
        "snapshot_id": request.activation.snapshot_id,
        "selected_event_ids": selected_event_ids,
        "read_plan": {
            "schema": CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": source_stream,
            "frontier": "compile_frontier",
            "entries": entries,
        },
    });

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "derived:context".to_string(),
            kind: EventKind::ContextReadPlanSet,
            payload,
        }],
    })
}

fn memory_kind_matches(payload: &JsonValue, expected: Option<&str>) -> bool {
    expected.is_none_or(|expected| {
        payload.get("memory_kind").and_then(|value| value.as_str()) == Some(expected)
    })
}

fn invoke_prompt_steer(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if !matches!(
        request.trigger_event.kind,
        EventKind::TurnCompleted | EventKind::ApprovalResolved
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_PROMPT_STEER_TEMPLATE_ID} expected turn.completed or approval.resolved trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = prompt_steer_config(&request.coupling.config)?;
    match config.action {
        PromptSteerAction::RequestContinuation => invoke_prompt_steer_continuation(request, config),
        PromptSteerAction::SetReadPlan => invoke_prompt_steer_read_plan(request, config),
    }
}

fn invoke_prompt_steer_continuation(
    request: CouplingInvocation,
    config: PromptSteerConfig,
) -> CooldisResult<CouplingExecutionResult> {
    let parent_turn_id = config
        .parent_turn_id
        .or_else(|| {
            payload_string(
                &request.trigger_event.payload,
                &["parent_turn_id", "turn_id"],
            )
        })
        .ok_or_else(|| {
            CooldisError::RuntimeFactory(
                "std::prompt.steer request_continuation requires parent_turn_id".to_string(),
            )
        })?;
    let next_turn_input = config.next_turn_input.ok_or_else(|| {
        CooldisError::RuntimeFactory(
            "std::prompt.steer request_continuation requires next_turn_input".to_string(),
        )
    })?;
    let payload = TurnContinueRequestedPayload {
        subject: TurnContinuationSubject {
            loop_id: config.loop_id.unwrap_or_else(|| "prompt.steer".to_string()),
            parent_turn_id,
        },
        snapshot_id: request.activation.snapshot_id.clone(),
        next_turn_input,
    };
    let mut payload = serde_json::to_value(payload).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::prompt.steer payload codec failed: {err}"))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            json!(EventKind::TurnContinueRequested.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            json!(STD_PROMPT_STEER_TEMPLATE_ID),
        );
        object.insert(
            "trigger_event_id".to_string(),
            json!(request.trigger_event.id.to_string()),
        );
        object.insert(
            "trigger_kind".to_string(),
            json!(request.trigger_event.kind.as_str()),
        );
        object.insert(
            "reason".to_string(),
            json!(
                config
                    .reason
                    .unwrap_or_else(|| "prompt steering requested continuation".to_string())
            ),
        );
    }

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::TurnContinueRequested,
            payload,
        }],
    })
}

fn invoke_prompt_steer_read_plan(
    request: CouplingInvocation,
    config: PromptSteerConfig,
) -> CooldisResult<CouplingExecutionResult> {
    let checkpoint_event_id = config.checkpoint_event_id.ok_or_else(|| {
        CooldisError::RuntimeFactory(
            "std::prompt.steer set_read_plan requires checkpoint_event_id".to_string(),
        )
    })?;
    let checkpoint_stream_id = config
        .checkpoint_stream_id
        .unwrap_or_else(|| request.trigger_event.stream_id.as_str().to_string());
    let event_role = config
        .event_role
        .unwrap_or_else(|| "instruction_checkpoint".to_string());
    let read_plan_name = config
        .read_plan_name
        .unwrap_or_else(|| "instructions.default".to_string());
    let pipeline_id = config
        .pipeline_id
        .unwrap_or_else(|| "context.instructions".to_string());
    let payload = json!({
        "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": checkpoint_stream_id,
        "checkpoint_event_id": checkpoint_event_id.to_string(),
        "template_id": STD_PROMPT_STEER_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.as_str(),
        "snapshot_id": request.activation.snapshot_id,
        "reason": config
            .reason
            .unwrap_or_else(|| "prompt steering selected read plan".to_string()),
        "read_plan": {
            "schema": CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": checkpoint_stream_id,
            "frontier": "compile_frontier",
            "entries": [{
                "kind": "event_ref",
                "stream_id": checkpoint_stream_id,
                "event_id": checkpoint_event_id.to_string(),
                "event_role": event_role,
            }],
        },
    });

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::ContextReadPlanSet,
            payload,
        }],
    })
}

fn invoke_prompt_dynamic_instructions(
    request: CouplingInvocation,
) -> CooldisResult<CouplingExecutionResult> {
    if !matches!(
        request.trigger_event.kind,
        EventKind::ManifestBindCompleted | EventKind::ContextCompileCompleted
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID} expected manifest.bind.completed or context.compile.completed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = prompt_dynamic_instructions_config(&request.coupling.config)?;
    let instruction_text = config
        .instruction_text
        .or_else(|| instruction_text_from_payload(&request.trigger_event.payload))
        .unwrap_or_else(|| {
            format!(
                "Instruction checkpoint from {} {}.",
                request.trigger_event.kind, request.trigger_event.id
            )
        });
    let instruction_name = config
        .instruction_name
        .unwrap_or_else(|| "instructions.default".to_string());
    let read_plan_name = config
        .read_plan_name
        .unwrap_or_else(|| instruction_name.clone());
    let pipeline_id = config
        .pipeline_id
        .unwrap_or_else(|| "context.instructions".to_string());
    let instruction_event_id = config
        .instruction_event_id
        .unwrap_or_else(EventRecordId::new);
    let source_ranges = context_spill_source_ranges(&request);
    let content_hash = sha256_hex(instruction_text.as_bytes());
    let derived_context_stream = format!(
        "derived:context:{}",
        request.trigger_event.coordinates.thread_id
    );
    let summary_payload = json!({
        "schema": EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": instruction_text,
        "covered_ranges": source_ranges_json(&source_ranges),
        "content": {
            "sha256": content_hash,
        },
        "template_id": STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID,
        "instruction_name": instruction_name,
        "source_event_id": request.trigger_event.id.to_string(),
        "source_kind": request.trigger_event.kind.as_str(),
        "snapshot_id": request.activation.snapshot_id,
    });
    let read_plan_payload = json!({
        "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": derived_context_stream,
        "instruction_event_id": instruction_event_id.to_string(),
        "template_id": STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.as_str(),
        "read_plan": {
            "schema": CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": derived_context_stream,
            "frontier": "compile_frontier",
            "entries": [{
                "kind": "event_ref",
                "stream_id": derived_context_stream,
                "event_id": instruction_event_id.to_string(),
                "event_role": "instruction_checkpoint",
            }],
        },
    });

    Ok(CouplingExecutionResult {
        discharges: vec![
            CouplingDischarge {
                event_id: Some(instruction_event_id),
                stream: "derived:context".to_string(),
                kind: EventKind::ContextSummaryCompleted,
                payload: summary_payload,
            },
            CouplingDischarge {
                event_id: None,
                stream: "derived:context".to_string(),
                kind: EventKind::ContextReadPlanSet,
                payload: read_plan_payload,
            },
        ],
    })
}

fn instruction_text_from_payload(payload: &JsonValue) -> Option<String> {
    [
        "instruction_text",
        "instructions",
        "system_instruction",
        "summary",
        "text",
    ]
    .into_iter()
    .find_map(|key| {
        payload
            .get(key)
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
    })
}

fn invoke_failure_deadletter(
    request: CouplingInvocation,
) -> CooldisResult<CouplingExecutionResult> {
    if !matches!(
        request.trigger_event.kind,
        EventKind::CouplingRunFailed | EventKind::LoopBlocked
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_FAILURE_DEADLETTER_TEMPLATE_ID} expected coupling.run.failed or loop.blocked trigger, got {}",
            request.trigger_event.kind
        )));
    }
    if request
        .trigger_event
        .payload
        .get("role")
        .and_then(|value| value.as_str())
        == Some("deadletter_projection")
    {
        return Ok(CouplingExecutionResult::default());
    }
    let config = failure_deadletter_config(&request.coupling.config)?;
    let reason = config
        .reason
        .or_else(|| {
            request
                .trigger_event
                .payload
                .get("reason")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "projected into deadletter stream".to_string());
    let queue = config.queue.unwrap_or_else(|| "default".to_string());
    let payload = json!({
        "schema": EventKind::CouplingRunFailed.payload_schema_id(),
        "role": "deadletter_projection",
        "template_id": STD_FAILURE_DEADLETTER_TEMPLATE_ID,
        "status": "deadlettered",
        "queue": queue,
        "snapshot_id": request.activation.snapshot_id,
        "deadletter_id": format!("deadletter:{}", request.trigger_event.id),
        "source_event_id": request.trigger_event.id.to_string(),
        "source_kind": request.trigger_event.kind.as_str(),
        "source_stream_id": request.trigger_event.stream_id.as_str(),
        "source_sequence": request.trigger_event.sequence.get(),
        "reason": reason,
        "failure": request.trigger_event.payload,
    });

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "derived:deadletter".to_string(),
            kind: EventKind::CouplingRunFailed,
            payload,
        }],
    })
}

fn invoke_permission_tool_gate(
    request: CouplingInvocation,
) -> CooldisResult<CouplingExecutionResult> {
    if request.trigger_event.kind != EventKind::ToolCallRequested {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_PERMISSION_TOOL_GATE_TEMPLATE_ID} expected tool.call.requested trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = permission_tool_gate_config(&request.coupling.config)?;
    let requested =
        serde_json::from_value::<ToolCallRequestedPayload>(request.trigger_event.payload.clone())
            .map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "std::permission.tool_gate trigger payload codec failed: {err}"
            ))
        })?;

    match config.decision {
        PermissionToolGateDecision::Allow => permission_tool_decision_result(
            &request,
            &requested,
            ToolCallDecisionOutcomePayload::Allow,
            config.reason,
        ),
        PermissionToolGateDecision::Rewrite => {
            let arguments = config.arguments.ok_or_else(|| {
                CooldisError::RuntimeFactory(
                    "std::permission.tool_gate rewrite requires arguments".to_string(),
                )
            })?;
            permission_tool_decision_result(
                &request,
                &requested,
                ToolCallDecisionOutcomePayload::Rewrite { arguments },
                config.reason,
            )
        }
        PermissionToolGateDecision::Deny => {
            let reason = config
                .reason
                .unwrap_or_else(|| "denied by std::permission.tool_gate".to_string());
            permission_tool_decision_result(
                &request,
                &requested,
                ToolCallDecisionOutcomePayload::Deny {
                    reason: reason.clone(),
                },
                Some(reason),
            )
        }
        PermissionToolGateDecision::Wait => {
            let payload = ToolCallSuspendedPayload {
                subject: requested.subject,
                snapshot_id: request.activation.snapshot_id.clone(),
                approval_id: config.approval_id,
                reason: config
                    .reason
                    .or_else(|| Some("waiting on std::permission.tool_gate".to_string())),
            };
            let mut payload = serde_json::to_value(payload).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "std::permission.tool_gate suspended payload codec failed: {err}"
                ))
            })?;
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "schema".to_string(),
                    json!(EventKind::ToolCallSuspended.payload_schema_id()),
                );
                object.insert(
                    "template_id".to_string(),
                    json!(STD_PERMISSION_TOOL_GATE_TEMPLATE_ID),
                );
                object.insert("tool_name".to_string(), json!(requested.tool_name));
                object.insert(
                    "request_event_id".to_string(),
                    json!(request.trigger_event.id.to_string()),
                );
            }
            Ok(CouplingExecutionResult {
                discharges: vec![CouplingDischarge {
                    event_id: None,
                    stream: "control".to_string(),
                    kind: EventKind::ToolCallSuspended,
                    payload,
                }],
            })
        }
    }
}

fn invoke_permission_approval_gate(
    request: CouplingInvocation,
) -> CooldisResult<CouplingExecutionResult> {
    if request.trigger_event.kind != EventKind::ToolCallRequested {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID} expected tool.call.requested trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = permission_approval_gate_config(&request.coupling.config)?;
    let requested =
        serde_json::from_value::<ToolCallRequestedPayload>(request.trigger_event.payload.clone())
            .map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "std::permission.approval_gate trigger payload codec failed: {err}"
            ))
        })?;
    let approval_id = config.approval_id.unwrap_or_else(|| {
        format!(
            "approval:{}:{}",
            requested.subject.turn_id, requested.subject.call_id
        )
    });
    let subject = requested.subject.clone();
    let snapshot_id = request.activation.snapshot_id.clone();
    let tool_name = requested.tool_name.clone();
    let request_event_id = request.trigger_event.id.to_string();
    let reason = config
        .reason
        .or_else(|| Some("operator approval required".to_string()));
    let resume_token = config.resume_token.unwrap_or_else(|| approval_id.clone());

    let approval_requested = json!({
        "schema": EventKind::ApprovalRequested.payload_schema_id(),
        "template_id": STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID,
        "approval_id": approval_id.clone(),
        "kind": "tool.call",
        "subject": {
            "turn_id": subject.turn_id.clone(),
            "call_id": subject.call_id.clone(),
        },
        "snapshot_id": snapshot_id.clone(),
        "tool_name": tool_name,
        "request_event_id": request_event_id,
        "reason": reason.clone(),
        "resume_token": resume_token.clone(),
    });

    let suspended_payload = ToolCallSuspendedPayload {
        subject,
        snapshot_id,
        approval_id: Some(approval_id),
        reason,
    };
    let mut suspended = serde_json::to_value(suspended_payload).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::permission.approval_gate suspended payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = suspended.as_object_mut() {
        object.insert(
            "schema".to_string(),
            json!(EventKind::ToolCallSuspended.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            json!(STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID),
        );
        object.insert(
            "approval_requested_event_role".to_string(),
            json!("approval_request"),
        );
        object.insert(
            "request_event_id".to_string(),
            approval_requested["request_event_id"].clone(),
        );
        object.insert(
            "tool_name".to_string(),
            approval_requested["tool_name"].clone(),
        );
        object.insert(
            "resume_token".to_string(),
            approval_requested["resume_token"].clone(),
        );
    }

    Ok(CouplingExecutionResult {
        discharges: vec![
            CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: EventKind::ApprovalRequested,
                payload: approval_requested,
            },
            CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: EventKind::ToolCallSuspended,
                payload: suspended,
            },
        ],
    })
}

fn permission_tool_decision_result(
    request: &CouplingInvocation,
    requested: &ToolCallRequestedPayload,
    outcome: ToolCallDecisionOutcomePayload,
    reason: Option<String>,
) -> CooldisResult<CouplingExecutionResult> {
    let payload = ToolCallDecisionPayload {
        subject: requested.subject.clone(),
        snapshot_id: request.activation.snapshot_id.clone(),
        outcome,
        admissible: None,
    };
    let mut payload = serde_json::to_value(payload).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::permission.tool_gate decision payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            json!(EventKind::ToolCallDecision.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            json!(STD_PERMISSION_TOOL_GATE_TEMPLATE_ID),
        );
        object.insert("tool_name".to_string(), json!(requested.tool_name));
        object.insert(
            "request_event_id".to_string(),
            json!(request.trigger_event.id.to_string()),
        );
        if let Some(reason) = reason {
            object.insert("reason".to_string(), json!(reason));
        }
    }

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::ToolCallDecision,
            payload,
        }],
    })
}

fn invoke_schedule_cron(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    match request.trigger_event.kind {
        EventKind::MandateStarted => Ok(CouplingExecutionResult::default()),
        EventKind::TimerFired => {
            let config = schedule_cron_config(&request.coupling.config)?;
            invoke_schedule_cron_timer_fired(request, config)
        }
        kind => Err(CooldisError::RuntimeFactory(format!(
            "{STD_SCHEDULE_CRON_TEMPLATE_ID} expected timer.fired trigger, got {kind}"
        ))),
    }
}

fn invoke_schedule_cron_timer_fired(
    request: CouplingInvocation,
    config: ScheduleCronConfig,
) -> CooldisResult<CouplingExecutionResult> {
    let timer = timer_fired_payload(&request)?;
    let Some((mandate_event, mandate)) = timer_fired_mandate(&request, &timer)? else {
        return Ok(CouplingExecutionResult::default());
    };
    if !config.mandate_scope.matches(&mandate.subject) {
        return Ok(CouplingExecutionResult::default());
    }

    let max_occurrences = mandate.max_occurrences.unwrap_or(config.max_occurrences);
    let schedule_id = config
        .schedule_id
        .clone()
        .unwrap_or_else(|| "default".to_string());
    if timer.occurrence_index >= u64::from(max_occurrences) {
        let mut discharge = schedule_budget_exhausted_discharge(
            &request,
            &schedule_id,
            timer.mandate_event_id,
            Some(mandate.mandate_id.as_str()),
            timer.occurrence_index,
            max_occurrences,
        );
        if let Some(object) = discharge.payload.as_object_mut() {
            object.insert(
                "occurrence_index".to_string(),
                json!(timer.occurrence_index),
            );
            object.insert(
                "timer_event_id".to_string(),
                json!(request.trigger_event.id.to_string()),
            );
        }
        return Ok(CouplingExecutionResult {
            discharges: vec![discharge],
        });
    }

    let parent_turn_id = config.parent_turn_id.ok_or_else(|| {
        CooldisError::RuntimeFactory(
            "std::schedule.cron continuation requires parent_turn_id".to_string(),
        )
    })?;
    let input_template = mandate
        .input_template
        .clone()
        .or(config.next_turn_input)
        .ok_or_else(|| {
            CooldisError::RuntimeFactory(
                "std::schedule.cron continuation requires input_template".to_string(),
            )
        })?;
    let next_turn_input = render_schedule_input_template(&input_template, &timer.scheduled_for);
    let payload = TurnContinueRequestedPayload {
        subject: TurnContinuationSubject {
            loop_id: config
                .loop_id
                .or_else(|| mandate.subject.loop_id.clone())
                .unwrap_or_else(|| "default".to_string()),
            parent_turn_id,
        },
        snapshot_id: mandate.snapshot_id.clone(),
        next_turn_input,
    };
    let mut payload = serde_json::to_value(payload).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::schedule.cron payload codec failed: {err}"))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            json!(EventKind::TurnContinueRequested.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            json!(STD_SCHEDULE_CRON_TEMPLATE_ID),
        );
        object.insert(
            "reason".to_string(),
            json!(
                config
                    .reason
                    .unwrap_or_else(|| "scheduled occurrence accepted".to_string())
            ),
        );
        object.insert(
            "schedule".to_string(),
            json!({
                "schedule_id": schedule_id,
                "mandate_id": mandate.mandate_id,
                "mandate_event_id": mandate_event.id.to_string(),
                "timer_event_id": request.trigger_event.id.to_string(),
                "scheduled_for": timer.scheduled_for,
                "occurrence_index": timer.occurrence_index,
                "max_occurrences": max_occurrences,
            }),
        );
    }

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::TurnContinueRequested,
            payload,
        }],
    })
}

fn timer_fired_payload(request: &CouplingInvocation) -> CooldisResult<TimerFiredPayload> {
    serde_json::from_value(request.trigger_event.payload.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::schedule.cron timer payload failed: {err}"))
    })
}

fn timer_fired_mandate(
    request: &CouplingInvocation,
    timer: &TimerFiredPayload,
) -> CooldisResult<Option<(EventRecord, MandateStartedPayload)>> {
    if !request
        .trigger_event
        .provenance
        .source_event_ids
        .contains(&timer.mandate_event_id)
    {
        return Ok(None);
    }
    let Some(event) = request.source_events.iter().find(|event| {
        event.id == timer.mandate_event_id && event.kind == EventKind::MandateStarted
    }) else {
        return Ok(None);
    };
    let mandate =
        serde_json::from_value::<MandateStartedPayload>(event.payload.clone()).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "std::schedule.cron mandate payload failed: {err}"
            ))
        })?;
    Ok(Some((event.clone(), mandate)))
}

fn render_schedule_input_template(template: &str, scheduled_for: &str) -> String {
    template.replace("{scheduled_for}", scheduled_for)
}

fn schedule_budget_exhausted_discharge(
    request: &CouplingInvocation,
    schedule_id: &str,
    mandate_event_id: EventRecordId,
    mandate_id: Option<&str>,
    occurrence: u64,
    max_occurrences: u32,
) -> CouplingDischarge {
    CouplingDischarge {
        event_id: None,
        stream: "control".to_string(),
        kind: EventKind::LoopBudgetExhausted,
        payload: json!({
            "schema": EventKind::LoopBudgetExhausted.payload_schema_id(),
            "template_id": STD_SCHEDULE_CRON_TEMPLATE_ID,
            "snapshot_id": request.activation.snapshot_id,
            "mandate_event_id": mandate_event_id.to_string(),
            "mandate_id": mandate_id,
            "schedule_id": schedule_id,
            "occurrence": occurrence,
            "max_occurrences": max_occurrences,
            "reason": format!(
                "schedule budget exhausted after occurrence {occurrence}/{max_occurrences}"
            ),
        }),
    }
}

fn invoke_supervisor_child_completion(
    request: CouplingInvocation,
) -> CooldisResult<CouplingExecutionResult> {
    if !matches!(
        request.trigger_event.kind,
        EventKind::TurnCompleted | EventKind::CouplingRunCompleted
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID} expected turn.completed or coupling.run.completed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = supervisor_child_completion_config(&request.coupling.config)?;
    let Some(completion) = supervisor_child_completion_fact(&request, &config)? else {
        return Ok(CouplingExecutionResult::default());
    };

    let discharge = match config.on_completed {
        SupervisorChildCompletionAction::CompleteLoop => {
            let mut payload = JsonMap::from_iter([
                (
                    "schema".to_string(),
                    json!(EventKind::LoopCompleted.payload_schema_id()),
                ),
                (
                    "template_id".to_string(),
                    json!(STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID),
                ),
                (
                    "snapshot_id".to_string(),
                    json!(request.activation.snapshot_id),
                ),
                (
                    "trigger_event_id".to_string(),
                    json!(request.trigger_event.id.to_string()),
                ),
                (
                    "trigger_kind".to_string(),
                    json!(request.trigger_event.kind.as_str()),
                ),
                (
                    "reason".to_string(),
                    json!(
                        config
                            .reason
                            .unwrap_or_else(|| { "supervised child work completed".to_string() })
                    ),
                ),
                (
                    "child".to_string(),
                    supervisor_child_completion_json(&completion),
                ),
            ]);
            insert_optional_string(
                &mut payload,
                "parent_thread_id",
                completion.parent_thread_id.clone(),
            );

            CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: EventKind::LoopCompleted,
                payload: JsonValue::Object(payload),
            }
        }
        SupervisorChildCompletionAction::RequestContinuation => {
            let parent_turn_id = config
                .parent_turn_id
                .or_else(|| {
                    payload_string(
                        &request.trigger_event.payload,
                        &["parent_turn_id", "turn_id"],
                    )
                })
                .ok_or_else(|| {
                    CooldisError::RuntimeFactory(
                    "std::supervisor.child_completion request_continuation requires parent_turn_id"
                        .to_string(),
                )
                })?;
            let next_turn_input = config.next_turn_input.ok_or_else(|| {
                CooldisError::RuntimeFactory(
                    "std::supervisor.child_completion request_continuation requires next_turn_input"
                        .to_string(),
                )
            })?;
            let payload = TurnContinueRequestedPayload {
                subject: TurnContinuationSubject {
                    loop_id: config.loop_id.unwrap_or_else(|| "supervisor".to_string()),
                    parent_turn_id,
                },
                snapshot_id: request.activation.snapshot_id.clone(),
                next_turn_input,
            };
            let mut payload = serde_json::to_value(payload).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "std::supervisor.child_completion continuation payload codec failed: {err}"
                ))
            })?;
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "schema".to_string(),
                    json!(EventKind::TurnContinueRequested.payload_schema_id()),
                );
                object.insert(
                    "template_id".to_string(),
                    json!(STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID),
                );
                object.insert(
                    "trigger_event_id".to_string(),
                    json!(request.trigger_event.id.to_string()),
                );
                object.insert(
                    "trigger_kind".to_string(),
                    json!(request.trigger_event.kind.as_str()),
                );
                object.insert(
                    "reason".to_string(),
                    json!(config.reason.unwrap_or_else(|| {
                        "supervised child completion requested continuation".to_string()
                    })),
                );
                object.insert(
                    "child".to_string(),
                    supervisor_child_completion_json(&completion),
                );
            }
            CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: EventKind::TurnContinueRequested,
                payload,
            }
        }
    };

    Ok(CouplingExecutionResult {
        discharges: vec![discharge],
    })
}

#[derive(Clone, Debug, Default)]
struct SupervisorChildCompletionFact {
    parent_thread_id: Option<String>,
    child_thread_id: Option<String>,
    child_turn_id: Option<String>,
    status: String,
    completed_coupling_id: Option<String>,
    completed_trigger_event_id: Option<EventRecordId>,
    completed_discharged_event_ids: Vec<EventRecordId>,
}

fn supervisor_child_completion_fact(
    request: &CouplingInvocation,
    config: &SupervisorChildCompletionConfig,
) -> CooldisResult<Option<SupervisorChildCompletionFact>> {
    match request.trigger_event.kind {
        EventKind::TurnCompleted => Ok(Some(SupervisorChildCompletionFact {
            parent_thread_id: config
                .parent_thread_id
                .clone()
                .or_else(|| payload_string(&request.trigger_event.payload, &["parent_thread_id"])),
            child_thread_id: config
                .child_thread_id
                .clone()
                .or_else(|| payload_string(&request.trigger_event.payload, &["child_thread_id"]))
                .or_else(|| Some(request.trigger_event.coordinates.thread_id.to_string())),
            child_turn_id: config.child_turn_id.clone().or_else(|| {
                payload_string(
                    &request.trigger_event.payload,
                    &["child_turn_id", "turn_id"],
                )
            }),
            status: payload_string(&request.trigger_event.payload, &["status"])
                .unwrap_or_else(|| "completed".to_string()),
            completed_coupling_id: None,
            completed_trigger_event_id: None,
            completed_discharged_event_ids: Vec::new(),
        })),
        EventKind::CouplingRunCompleted => {
            let completed = serde_json::from_value::<CouplingRunReceipt>(
                request.trigger_event.payload.clone(),
            )
            .map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "std::supervisor.child_completion trigger payload is not a coupling run receipt: {err}"
                ))
            })?;
            if completed.status != CouplingRunStatus::Completed {
                return Ok(None);
            }
            if let Some(watch_coupling_id) = &config.watch_coupling_id
                && completed.coupling_id != *watch_coupling_id
            {
                return Ok(None);
            }
            Ok(Some(SupervisorChildCompletionFact {
                parent_thread_id: config
                    .parent_thread_id
                    .clone()
                    .or_else(|| {
                        payload_string(&request.trigger_event.payload, &["parent_thread_id"])
                    })
                    .or_else(|| {
                        source_payload_string(&request.source_events, &["parent_thread_id"])
                    }),
                child_thread_id: config
                    .child_thread_id
                    .clone()
                    .or_else(|| {
                        payload_string(&request.trigger_event.payload, &["child_thread_id"])
                    })
                    .or_else(|| {
                        source_payload_string(&request.source_events, &["child_thread_id"])
                    }),
                child_turn_id: config
                    .child_turn_id
                    .clone()
                    .or_else(|| {
                        payload_string(
                            &request.trigger_event.payload,
                            &["child_turn_id", "turn_id"],
                        )
                    })
                    .or_else(|| {
                        source_payload_string(&request.source_events, &["child_turn_id", "turn_id"])
                    }),
                status: payload_string(&request.trigger_event.payload, &["child_status", "status"])
                    .unwrap_or_else(|| "completed".to_string()),
                completed_coupling_id: Some(completed.coupling_id),
                completed_trigger_event_id: Some(completed.trigger_event_id),
                completed_discharged_event_ids: completed.discharged_event_ids,
            }))
        }
        _ => Ok(None),
    }
}

fn supervisor_child_completion_json(completion: &SupervisorChildCompletionFact) -> JsonValue {
    let mut child = JsonMap::from_iter([("status".to_string(), json!(completion.status))]);
    insert_optional_string(
        &mut child,
        "parent_thread_id",
        completion.parent_thread_id.clone(),
    );
    insert_optional_string(
        &mut child,
        "child_thread_id",
        completion.child_thread_id.clone(),
    );
    insert_optional_string(
        &mut child,
        "child_turn_id",
        completion.child_turn_id.clone(),
    );
    if let Some(coupling_id) = &completion.completed_coupling_id {
        child.insert("completed_coupling_id".to_string(), json!(coupling_id));
    }
    if let Some(trigger_event_id) = completion.completed_trigger_event_id {
        child.insert(
            "completed_trigger_event_id".to_string(),
            json!(trigger_event_id.to_string()),
        );
    }
    if !completion.completed_discharged_event_ids.is_empty() {
        child.insert(
            "completed_discharged_event_ids".to_string(),
            json!(
                completion
                    .completed_discharged_event_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            ),
        );
    }
    JsonValue::Object(child)
}

fn invoke_retry_with_budget(request: CouplingInvocation) -> CooldisResult<CouplingExecutionResult> {
    if request.trigger_event.kind != EventKind::CouplingRunFailed {
        return Err(CooldisError::RuntimeFactory(format!(
            "{STD_RETRY_WITH_BUDGET_TEMPLATE_ID} expected coupling.run.failed trigger, got {}",
            request.trigger_event.kind
        )));
    }
    let config = retry_with_budget_config(&request.coupling.config)?;
    let attempt = request
        .trigger_event
        .payload
        .get("attempt")
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(1)
        .max(1);
    let error_class = request
        .trigger_event
        .payload
        .get("error_class")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    if !retry_error_class_allowed(&config, error_class.as_deref()) {
        let reason = format!(
            "retry denied for error class {}",
            error_class.as_deref().unwrap_or("unknown")
        );
        return Ok(CouplingExecutionResult {
            discharges: vec![retry_budget_exhausted_discharge(
                &request,
                attempt,
                config.max_attempts,
                error_class,
                reason,
            )],
        });
    }
    if attempt >= config.max_attempts {
        return Ok(CouplingExecutionResult {
            discharges: vec![retry_budget_exhausted_discharge(
                &request,
                attempt,
                config.max_attempts,
                error_class,
                format!(
                    "retry budget exhausted after attempt {attempt}/{}",
                    config.max_attempts
                ),
            )],
        });
    }

    let parent_turn_id = config.parent_turn_id.ok_or_else(|| {
        CooldisError::RuntimeFactory(
            "std::retry.with_budget continuation requires parent_turn_id".to_string(),
        )
    })?;
    let next_turn_input = config.next_turn_input.ok_or_else(|| {
        CooldisError::RuntimeFactory(
            "std::retry.with_budget continuation requires next_turn_input".to_string(),
        )
    })?;
    let next_attempt = attempt + 1;
    let payload = TurnContinueRequestedPayload {
        subject: TurnContinuationSubject {
            loop_id: config.loop_id.unwrap_or_else(|| "default".to_string()),
            parent_turn_id,
        },
        snapshot_id: request.activation.snapshot_id.clone(),
        next_turn_input,
    };
    let mut payload = serde_json::to_value(payload).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::retry.with_budget payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            json!(EventKind::TurnContinueRequested.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            json!(STD_RETRY_WITH_BUDGET_TEMPLATE_ID),
        );
        object.insert(
            "reason".to_string(),
            json!(
                config
                    .reason
                    .unwrap_or_else(|| "retry requested by std::retry.with_budget".to_string())
            ),
        );
        object.insert(
            "retry".to_string(),
            json!({
                "attempt": next_attempt,
                "previous_attempt": attempt,
                "max_attempts": config.max_attempts,
                "failed_event_id": request.trigger_event.id.to_string(),
                "error_class": error_class,
            }),
        );
    }

    Ok(CouplingExecutionResult {
        discharges: vec![CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: EventKind::TurnContinueRequested,
            payload,
        }],
    })
}

fn retry_budget_exhausted_discharge(
    request: &CouplingInvocation,
    attempt: u32,
    max_attempts: u32,
    error_class: Option<String>,
    reason: String,
) -> CouplingDischarge {
    CouplingDischarge {
        event_id: None,
        stream: "control".to_string(),
        kind: EventKind::LoopBudgetExhausted,
        payload: json!({
            "schema": EventKind::LoopBudgetExhausted.payload_schema_id(),
            "template_id": STD_RETRY_WITH_BUDGET_TEMPLATE_ID,
            "snapshot_id": request.activation.snapshot_id,
            "failed_event_id": request.trigger_event.id.to_string(),
            "attempt": attempt,
            "max_attempts": max_attempts,
            "error_class": error_class,
            "reason": reason,
        }),
    }
}

fn retry_error_class_allowed(config: &RetryWithBudgetConfig, error_class: Option<&str>) -> bool {
    if config.retryable_error_classes.is_empty() {
        return true;
    }
    let Some(error_class) = error_class else {
        return false;
    };
    config
        .retryable_error_classes
        .iter()
        .any(|allowed| allowed == error_class)
}

fn payload_string(payload: &JsonValue, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        payload
            .get(key)
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
    })
}

fn source_payload_string(events: &[EventRecord], keys: &[&str]) -> Option<String> {
    events
        .iter()
        .rev()
        .find_map(|event| payload_string(&event.payload, keys))
}

fn insert_optional_string(map: &mut JsonMap<String, JsonValue>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        map.insert(key.to_string(), json!(value));
    }
}

fn context_spill_source_ranges(request: &CouplingInvocation) -> Vec<ObservationSourceRange> {
    if !request.trigger_event.provenance.source_ranges.is_empty() {
        return request.trigger_event.provenance.source_ranges.clone();
    }
    request
        .source_cut
        .entries
        .iter()
        .map(|entry| ObservationSourceRange {
            stream_id: crate::EventStreamId::new(entry.stream_id.clone()),
            from_sequence: EventSequence::new(1),
            to_sequence: EventSequence::new(entry.max_sequence),
        })
        .collect()
}

fn context_spill_summary_text(request: &CouplingInvocation) -> String {
    let truncated = request
        .trigger_event
        .payload
        .get("truncated_text_bytes")
        .and_then(|value| value.as_i64())
        .unwrap_or_default();
    format!(
        "Spilled {truncated} bytes from context.compile.completed {}.",
        request.trigger_event.id
    )
}

fn source_ranges_json(source_ranges: &[ObservationSourceRange]) -> Vec<JsonValue> {
    source_ranges
        .iter()
        .map(|range| {
            json!({
                "stream_id": range.stream_id.as_str(),
                "from_sequence": range.from_sequence.get(),
                "to_sequence": range.to_sequence.get(),
            })
        })
        .collect()
}

fn truncate_read_plan_entries(
    source_ranges: &[ObservationSourceRange],
    retain_tail_events: i64,
    reason: &str,
) -> Vec<JsonValue> {
    source_ranges
        .iter()
        .flat_map(|range| {
            let from_sequence = range.from_sequence.get();
            let to_sequence = range.to_sequence.get();
            if to_sequence < from_sequence {
                return Vec::new();
            }
            let retain_from_sequence = from_sequence.max(to_sequence - retain_tail_events + 1);
            let mut entries = Vec::new();
            if retain_from_sequence > from_sequence {
                entries.push(json!({
                    "kind": "drop_range",
                    "stream_id": range.stream_id.as_str(),
                    "reason": reason,
                    "range": {
                        "from": read_plan_from_cursor(range.from_sequence),
                        "to": {
                            "sequence": retain_from_sequence - 1
                        }
                    }
                }));
            }
            entries.push(json!({
                "kind": "raw_range",
                "stream_id": range.stream_id.as_str(),
                "range": {
                    "from": read_plan_from_cursor(EventSequence::new(retain_from_sequence)),
                    "to": {
                        "sequence": to_sequence
                    }
                }
            }));
            entries
        })
        .collect()
}

fn summary_checkpoint_entries(
    summary_event_id: EventRecordId,
    source_ranges: &[ObservationSourceRange],
) -> Vec<JsonValue> {
    if source_ranges.is_empty() {
        return vec![json!({
            "kind": "event_ref",
            "event_id": summary_event_id.to_string(),
            "event_role": "summary_checkpoint",
        })];
    }
    source_ranges
        .iter()
        .map(|range| {
            json!({
                "kind": "event_ref",
                "stream_id": range.stream_id.as_str(),
                "event_id": summary_event_id.to_string(),
                "event_role": "summary_checkpoint",
                "covers": {
                    "from": read_plan_from_cursor(range.from_sequence),
                    "to": {
                        "sequence": range.to_sequence.get()
                    }
                }
            })
        })
        .collect()
}

fn read_plan_from_cursor(sequence: EventSequence) -> JsonValue {
    if sequence.get() <= 1 {
        JsonValue::String("start".to_string())
    } else {
        json!({ "sequence": sequence.get() - 1 })
    }
}

fn queue_task_config(value: &JsonValue) -> CooldisResult<QueueTaskConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::queue.task config codec failed: {err}"))
    })
}

fn queue_completion_config(value: &JsonValue) -> CooldisResult<QueueCompletionCallbackConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::queue.completion_callback config codec failed: {err}"
        ))
    })
}

fn context_spill_config(value: &JsonValue) -> CooldisResult<ContextSpillConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::context.spill config codec failed: {err}"))
    })
}

fn context_truncate_config(value: &JsonValue) -> CooldisResult<ContextTruncateConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::context.truncate config codec failed: {err}"))
    })
}

fn context_summarize_config(value: &JsonValue) -> CooldisResult<ContextSummarizeConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::context.summarize config codec failed: {err}"))
    })
}

fn memory_extract_config(value: &JsonValue) -> CooldisResult<MemoryExtractConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::memory.extract config codec failed: {err}"))
    })
}

fn memory_recall_config(value: &JsonValue) -> CooldisResult<MemoryRecallConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::memory.recall config codec failed: {err}"))
    })
}

fn prompt_steer_config(value: &JsonValue) -> CooldisResult<PromptSteerConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::prompt.steer config codec failed: {err}"))
    })
}

fn prompt_dynamic_instructions_config(
    value: &JsonValue,
) -> CooldisResult<PromptDynamicInstructionsConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::prompt.dynamic_instructions config codec failed: {err}"
        ))
    })
}

fn permission_tool_gate_config(value: &JsonValue) -> CooldisResult<PermissionToolGateConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::permission.tool_gate config codec failed: {err}"
        ))
    })
}

fn permission_approval_gate_config(
    value: &JsonValue,
) -> CooldisResult<PermissionApprovalGateConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::permission.approval_gate config codec failed: {err}"
        ))
    })
}

fn failure_deadletter_config(value: &JsonValue) -> CooldisResult<FailureDeadletterConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::failure.deadletter config codec failed: {err}"
        ))
    })
}

fn retry_with_budget_config(value: &JsonValue) -> CooldisResult<RetryWithBudgetConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::retry.with_budget config codec failed: {err}"))
    })
}

fn schedule_cron_config(value: &JsonValue) -> CooldisResult<ScheduleCronConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::schedule.cron config codec failed: {err}"))
    })
}

fn supervisor_spawn_config(value: &JsonValue) -> CooldisResult<SupervisorSpawnConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!("std::supervisor.spawn config codec failed: {err}"))
    })
}

fn supervisor_child_completion_config(
    value: &JsonValue,
) -> CooldisResult<SupervisorChildCompletionConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "std::supervisor.child_completion config codec failed: {err}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID, STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID,
        STD_SUPERVISOR_SPAWN_TEMPLATE_ID, StdlibCouplingExecutor,
    };
    use crate::{
        AgentManifestCouplingBudget, AgentManifestCouplingQuota, BoundCoupling,
        BoundCouplingFunction, BoundCouplingSelector, BoundCouplingSet, BoundCouplingSink,
        CouplingRole, CouplingScheduler, EventKind, EventProvenance, EventRecord, EventRecordId,
        EventSequence, EventStore, EventStreamId, InMemorySessionStore, MandateCatchUpPolicy,
        MandateSchedulePayload, MandateStartedPayload, MandateSubject, NewEventRecord,
        ObservationSourceRange, ThreadCoordinates, ThreadSpawnRequestedPayload, TimerFiredPayload,
    };
    use serde_json::json;

    #[test]
    fn stdlib_executor_supports_exact_runtime_executable_catalog_templates() {
        let catalog = crate::coupling_template_catalog_v1();
        let declared = catalog
            .templates
            .iter()
            .filter(|template| template.runtime_executable)
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>();
        let supported = catalog
            .templates
            .iter()
            .filter(|template| StdlibCouplingExecutor::supports_template(&template.id))
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(supported, declared);
    }

    #[tokio::test]
    async fn std_queue_task_and_completion_callback_discharge_control_facts() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let submitted = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnSubmitted,
                    json!({
                        "turn_id": "turn-1",
                        "entry_id": "entry-1",
                    }),
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_queue_task_coupling(), std_queue_completion_callback()],
                ),
                submitted,
            )
            .await
            .unwrap();

        let control_events = store
            .read_events(&scheduler.stream_id_for(&coordinates, "control"), None)
            .await
            .unwrap();

        assert!(receipt.runs.iter().any(|run| {
            run.coupling_id == "std::queue.task" && run.discharged_event_ids.len() == 1
        }));
        assert!(receipt.runs.iter().any(|run| {
            run.coupling_id == "std::queue.completion_callback"
                && run.discharged_event_ids.len() == 1
        }));

        let waiting = control_events
            .iter()
            .find(|event| event.kind == EventKind::TurnWaiting)
            .unwrap();
        assert_eq!(
            waiting.payload["schema"],
            EventKind::TurnWaiting.payload_schema_id()
        );
        assert_eq!(waiting.payload["template_id"], "std::queue.task");
        assert_eq!(waiting.payload["turn_id"], "turn-1");
        assert_eq!(
            waiting.provenance.discharged_by.as_deref(),
            Some("coupling:std::queue.task")
        );

        let completed = control_events
            .iter()
            .find(|event| event.kind == EventKind::LoopCompleted)
            .unwrap();
        assert_eq!(
            completed.payload["schema"],
            EventKind::LoopCompleted.payload_schema_id()
        );
        assert_eq!(
            completed.payload["template_id"],
            "std::queue.completion_callback"
        );
        assert_eq!(
            completed.payload["completed_coupling_id"],
            "std::queue.task"
        );
        assert_eq!(
            completed.provenance.discharged_by.as_deref(),
            Some("coupling:std::queue.completion_callback")
        );
    }

    #[tokio::test]
    async fn std_context_spill_discharges_summary_and_read_plan_with_same_event_id() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let source_range = ObservationSourceRange {
            stream_id: thread_stream.clone(),
            from_sequence: EventSequence::new(1),
            to_sequence: EventSequence::new(4),
        };
        let compiled = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ContextCompileCompleted,
                    json!({
                        "schema": EventKind::ContextCompileCompleted.payload_schema_id(),
                        "truncated_text_bytes": 640,
                        "read_plan": {
                            "schema": "cooldis.context.read_plan/1",
                            "name": "history.default",
                            "source_stream": thread_stream.as_str(),
                            "frontier": "compile_frontier",
                            "entries": []
                        }
                    }),
                    EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        source_ranges: vec![source_range],
                        discharged_by: Some("projection:test-context-compiler".to_string()),
                        function: Some("test_context_compile/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_context_spill_coupling()]),
                compiled,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::context.spill");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 2);

        let derived_events = store
            .read_events(
                &scheduler.stream_id_for(&coordinates, "derived:context"),
                None,
            )
            .await
            .unwrap();
        let summary = derived_events
            .iter()
            .find(|event| event.kind == EventKind::ContextSummaryCompleted)
            .unwrap();
        let read_plan = derived_events
            .iter()
            .find(|event| event.kind == EventKind::ContextReadPlanSet)
            .unwrap();
        assert_eq!(
            read_plan.payload["summary_event_id"],
            summary.id.to_string()
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][0]["event_id"],
            summary.id.to_string()
        );
        assert_eq!(
            summary.provenance.discharged_by.as_deref(),
            Some("coupling:std::context.spill")
        );
    }

    #[tokio::test]
    async fn std_context_truncate_discharges_drop_and_tail_read_plan() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let source_range = ObservationSourceRange {
            stream_id: thread_stream.clone(),
            from_sequence: EventSequence::new(1),
            to_sequence: EventSequence::new(10),
        };
        let compiled = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ContextCompileCompleted,
                    json!({
                        "schema": EventKind::ContextCompileCompleted.payload_schema_id(),
                        "truncated_text_bytes": 1200,
                        "read_plan": {
                            "schema": "cooldis.context.read_plan/1",
                            "name": "history.default",
                            "source_stream": thread_stream.as_str(),
                            "frontier": "compile_frontier",
                            "entries": []
                        }
                    }),
                    EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        source_ranges: vec![source_range],
                        discharged_by: Some("projection:test-context-compiler".to_string()),
                        function: Some("test_context_compile/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_context_truncate_coupling()]),
                compiled,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::context.truncate");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store
            .read_events(&scheduler.stream_id_for(&coordinates, "control"), None)
            .await
            .unwrap();
        let read_plan = control_events
            .iter()
            .find(|event| event.kind == EventKind::ContextReadPlanSet)
            .unwrap();
        assert_eq!(
            read_plan.payload["schema"],
            EventKind::ContextReadPlanSet.payload_schema_id()
        );
        assert_eq!(read_plan.payload["template_id"], "std::context.truncate");
        assert_eq!(read_plan.payload["retain_tail_events"], 3);
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][0]["kind"],
            "drop_range"
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][0]["range"]["to"]["sequence"],
            7
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][1]["kind"],
            "raw_range"
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][1]["range"]["from"]["sequence"],
            7
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][1]["range"]["to"]["sequence"],
            10
        );
        assert_eq!(
            read_plan.provenance.discharged_by.as_deref(),
            Some("coupling:std::context.truncate")
        );
    }

    #[tokio::test]
    async fn std_context_summarize_discharges_summary_checkpoint_and_read_plan() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let completed = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({
                        "schema": EventKind::TurnCompleted.payload_schema_id(),
                        "turn_id": "turn-1",
                        "output_text": "The user wants SQLite first, S2 later, and explicit segment maps.",
                    }),
                    EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("runtime:provider-loop".to_string()),
                        function: Some("turn_completion/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_context_summarize_coupling()]),
                completed.clone(),
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::context.summarize");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 2);

        let derived_events = store
            .read_events(
                &scheduler.stream_id_for(&coordinates, "derived:context"),
                None,
            )
            .await
            .unwrap();
        let summary = derived_events
            .iter()
            .find(|event| event.kind == EventKind::ContextSummaryCompleted)
            .unwrap();
        let read_plan = derived_events
            .iter()
            .find(|event| event.kind == EventKind::ContextReadPlanSet)
            .unwrap();
        assert_eq!(summary.payload["template_id"], "std::context.summarize");
        assert_eq!(
            summary.payload["text"],
            "The user wants SQLite first, S2 later, and explicit segment maps."
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][0]["event_id"],
            summary.id.to_string()
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][0]["event_role"],
            "summary_checkpoint"
        );
        assert_eq!(
            summary.provenance.discharged_by.as_deref(),
            Some("coupling:std::context.summarize")
        );
    }

    #[tokio::test]
    async fn std_prompt_steer_requests_continuation_or_sets_instruction_read_plan() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let completed = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({
                        "schema": EventKind::TurnCompleted.payload_schema_id(),
                        "turn_id": "turn-1",
                        "output_text": "Need one more clarification turn.",
                    }),
                    EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("runtime:provider-loop".to_string()),
                        function: Some("turn_completion/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let continuation_receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_prompt_steer_continuation_coupling()],
                ),
                completed,
            )
            .await
            .unwrap();

        assert_eq!(continuation_receipt.runs.len(), 1);
        assert_eq!(
            continuation_receipt.runs[0].coupling_id,
            "std::prompt.steer"
        );
        assert_eq!(continuation_receipt.runs[0].discharged_event_ids.len(), 1);

        let control_stream = scheduler.stream_id_for(&coordinates, "control");
        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let continuation = control_events
            .iter()
            .find(|event| event.kind == EventKind::TurnContinueRequested)
            .unwrap();
        assert_eq!(
            continuation.payload["schema"],
            EventKind::TurnContinueRequested.payload_schema_id()
        );
        assert_eq!(continuation.payload["template_id"], "std::prompt.steer");
        assert_eq!(
            continuation.payload["next_turn_input"],
            "Ask the user to pick the deployment lane."
        );

        let approval = store
            .append_events(
                &control_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::ApprovalResolved,
                    json!({
                        "schema": EventKind::ApprovalResolved.payload_schema_id(),
                        "approval_id": "approval-instructions",
                        "decision": "approved",
                    }),
                )],
            )
            .await
            .unwrap();
        let read_plan_receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_prompt_steer_read_plan_coupling()]),
                approval,
            )
            .await
            .unwrap();
        assert_eq!(read_plan_receipt.runs.len(), 1);
        assert_eq!(read_plan_receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let read_plan = control_events
            .iter()
            .find(|event| event.kind == EventKind::ContextReadPlanSet)
            .unwrap();
        assert_eq!(read_plan.payload["template_id"], "std::prompt.steer");
        assert_eq!(read_plan.payload["pipeline_id"], "context.instructions");
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][0]["event_role"],
            "instruction_checkpoint"
        );
        assert_eq!(
            read_plan.provenance.discharged_by.as_deref(),
            Some("coupling:std::prompt.steer")
        );
    }

    #[tokio::test]
    async fn std_failure_deadletter_projects_failed_control_fact_to_derived_stream() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let failed = store
            .append_events(
                &control_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::CouplingRunFailed,
                    json!({
                        "coupling_id": "std::queue.task",
                        "status": "failed",
                        "reason": "remote service unavailable",
                        "root_event_id": EventRecordId::new().to_string(),
                        "trigger_event_id": EventRecordId::new().to_string(),
                        "trigger_stream_id": "thread:test",
                        "trigger_sequence": 1,
                        "snapshot_id": "snapshot-a",
                        "depth": 0,
                        "source_cut": {"entries": []},
                        "source_event_ids": [],
                        "discharged_event_ids": [],
                        "function_ref": "op://std-queue-task/run@sha256:test",
                        "config_hash": "sha256:queue-task",
                        "budget_spent": {"discharge_events": 0}
                    }),
                    EventProvenance {
                        source_streams: vec![EventStreamId::for_thread(&coordinates)],
                        discharged_by: Some("coupling:std::queue.task".to_string()),
                        function: Some("op://std-queue-task/run@sha256:test".to_string()),
                        config_hash: Some("sha256:queue-task".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_failure_deadletter_coupling()]),
                failed.clone(),
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::failure.deadletter");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let deadletter_events = store
            .read_events(
                &scheduler.stream_id_for(&coordinates, "derived:deadletter"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(deadletter_events.len(), 1);
        let deadletter = &deadletter_events[0];
        assert_eq!(deadletter.kind, EventKind::CouplingRunFailed);
        assert_eq!(
            deadletter.payload["schema"],
            EventKind::CouplingRunFailed.payload_schema_id()
        );
        assert_eq!(deadletter.payload["template_id"], "std::failure.deadletter");
        assert_eq!(deadletter.payload["status"], "deadlettered");
        assert_eq!(
            deadletter.payload["source_event_id"],
            failed[0].id.to_string()
        );
        assert_eq!(
            deadletter.payload["failure"]["coupling_id"],
            "std::queue.task"
        );
        assert_eq!(
            deadletter.provenance.discharged_by.as_deref(),
            Some("coupling:std::failure.deadletter")
        );
    }

    #[tokio::test]
    async fn std_memory_extract_projects_completed_turn_to_derived_memory() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let completed = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({
                        "turn_id": "turn-1",
                        "output_text": "User prefers SQLite first, then S2 as stream backend.",
                    }),
                    EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("runtime:provider-loop".to_string()),
                        function: Some("turn_completion/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_memory_extract_coupling()]),
                completed.clone(),
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::memory.extract");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let memory_events = store
            .read_events(
                &scheduler.stream_id_for(&coordinates, "derived:memory"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(memory_events.len(), 1);
        let memory = &memory_events[0];
        assert_eq!(memory.kind, EventKind::ContextSummaryCompleted);
        assert_eq!(
            memory.payload["schema"],
            EventKind::ContextSummaryCompleted.payload_schema_id()
        );
        assert_eq!(memory.payload["template_id"], "std::memory.extract");
        assert_eq!(memory.payload["memory_kind"], "observation");
        assert_eq!(
            memory.payload["source_event_id"],
            completed[0].id.to_string()
        );
        assert_eq!(
            memory.payload["text"],
            "User prefers SQLite first, then S2 as stream backend."
        );
        assert_eq!(
            memory.provenance.discharged_by.as_deref(),
            Some("coupling:std::memory.extract")
        );
    }

    #[tokio::test]
    async fn std_memory_recall_projects_memory_summaries_to_read_plan() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let memory_stream = EventStreamId::new(format!("derived:memory:{}", coordinates.thread_id));
        let memory = store
            .append_events(
                &memory_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ContextSummaryCompleted,
                    json!({
                        "schema": EventKind::ContextSummaryCompleted.payload_schema_id(),
                        "role": "summary_checkpoint",
                        "text": "User prefers SQLite first, then S2 as stream backend.",
                        "covered_ranges": [{
                            "stream_id": thread_stream.as_str(),
                            "from_sequence": 1,
                            "to_sequence": 4
                        }],
                        "content": {
                            "sha256": "sha256:memory"
                        },
                        "template_id": "std::memory.extract",
                        "memory_kind": "observation"
                    }),
                    EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("coupling:std::memory.extract".to_string()),
                        function: Some("op://std-memory-extract/run@sha256:test".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();
        let submitted = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnSubmitted,
                    json!({
                        "turn_id": "turn-2",
                        "input_text": "What should we use for V1 stream storage?",
                    }),
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_memory_recall_coupling()]),
                submitted,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::memory.recall");
        assert_eq!(receipt.runs[0].source_event_ids, vec![memory[0].id]);
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let context_events = store
            .read_events(
                &scheduler.stream_id_for(&coordinates, "derived:context"),
                None,
            )
            .await
            .unwrap();
        let read_plan = context_events
            .iter()
            .find(|event| event.kind == EventKind::ContextReadPlanSet)
            .unwrap();
        assert_eq!(
            read_plan.payload["schema"],
            EventKind::ContextReadPlanSet.payload_schema_id()
        );
        assert_eq!(read_plan.payload["template_id"], "std::memory.recall");
        assert_eq!(read_plan.payload["name"], "memory.default");
        assert_eq!(read_plan.payload["pipeline_id"], "context.memory");
        assert_eq!(read_plan.payload["source_id"], memory_stream.as_str());
        assert_eq!(
            read_plan.payload["read_plan"]["source_stream"],
            memory_stream.as_str()
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][0]["event_id"],
            memory[0].id.to_string()
        );
        assert_eq!(
            read_plan.payload["read_plan"]["entries"][0]["event_role"],
            "memory_checkpoint"
        );
        assert_eq!(
            read_plan.provenance.discharged_by.as_deref(),
            Some("coupling:std::memory.recall")
        );
    }

    #[tokio::test]
    async fn std_retry_with_budget_requests_continuation_for_retryable_failure() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let failed = append_failed_coupling_run(
            &store,
            &coordinates,
            &control_stream,
            json!({
                "attempt": 1,
                "error_class": "retryable",
                "reason": "provider network hiccup",
            }),
        )
        .await;

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_retry_with_budget_coupling(2)]),
                failed,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::retry.with_budget");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let retry = control_events
            .iter()
            .find(|event| event.kind == EventKind::TurnContinueRequested)
            .unwrap();
        assert_eq!(
            retry.payload["schema"],
            EventKind::TurnContinueRequested.payload_schema_id()
        );
        assert_eq!(retry.payload["template_id"], "std::retry.with_budget");
        assert_eq!(retry.payload["subject"]["parent_turn_id"], "turn-1");
        assert_eq!(retry.payload["next_turn_input"], "retry last failed step");
        assert_eq!(retry.payload["retry"]["attempt"], 2);
        assert_eq!(retry.payload["retry"]["max_attempts"], 2);
        assert_eq!(
            retry.provenance.discharged_by.as_deref(),
            Some("coupling:std::retry.with_budget")
        );
    }

    #[tokio::test]
    async fn std_retry_with_budget_emits_budget_exhausted_when_limit_reached() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let failed = append_failed_coupling_run(
            &store,
            &coordinates,
            &control_stream,
            json!({
                "attempt": 2,
                "error_class": "retryable",
                "reason": "provider network hiccup",
            }),
        )
        .await;

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![std_retry_with_budget_coupling(2)]),
                failed,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::retry.with_budget");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let exhausted = control_events
            .iter()
            .find(|event| event.kind == EventKind::LoopBudgetExhausted)
            .unwrap();
        assert_eq!(
            exhausted.payload["schema"],
            EventKind::LoopBudgetExhausted.payload_schema_id()
        );
        assert_eq!(exhausted.payload["template_id"], "std::retry.with_budget");
        assert_eq!(exhausted.payload["attempt"], 2);
        assert_eq!(exhausted.payload["max_attempts"], 2);
        assert_eq!(
            exhausted.payload["reason"],
            "retry budget exhausted after attempt 2/2"
        );
    }

    #[tokio::test]
    async fn std_schedule_cron_mandate_started_does_not_discharge() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mandate = append_schedule_mandate_started(
            &store,
            &coordinates,
            &control_stream,
            MandateSubject {
                thread_id: Some(coordinates.thread_id.to_string()),
                loop_id: Some("loop-nightly".to_string()),
            },
            2,
            "run summary for {scheduled_for}",
        )
        .await;
        let mut coupling = std_schedule_cron_coupling();
        coupling.trigger_kind = EventKind::MandateStarted;

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![coupling]),
                vec![mandate],
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::schedule.cron");
        assert!(receipt.runs[0].discharged_event_ids.is_empty());

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        assert!(control_events.iter().all(|event| !matches!(
            event.kind,
            EventKind::TurnContinueRequested | EventKind::LoopBudgetExhausted
        )));
    }

    #[tokio::test]
    async fn std_schedule_cron_requests_continuation_for_timer_fired() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mandate = append_schedule_mandate_started(
            &store,
            &coordinates,
            &control_stream,
            MandateSubject {
                thread_id: Some(coordinates.thread_id.to_string()),
                loop_id: Some("loop-nightly".to_string()),
            },
            2,
            "run summary for {scheduled_for}",
        )
        .await;
        let fired = append_timer_fired(
            &store,
            &coordinates,
            &control_stream,
            mandate.id,
            1,
            "2026-01-01T00:01:00.000Z",
            mandate.id,
        )
        .await;

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_schedule_cron_timer_coupling(json!({
                        "max_occurrences": 2,
                        "schedule_id": "nightly-summary",
                        "parent_turn_id": "turn-nightly-root",
                        "loop_id": "loop-nightly",
                        "mandate_scope": "match_all",
                    }))],
                ),
                fired,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::schedule.cron");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let continuation = control_events
            .iter()
            .find(|event| event.kind == EventKind::TurnContinueRequested)
            .unwrap();
        assert_eq!(
            continuation.payload["schema"],
            EventKind::TurnContinueRequested.payload_schema_id()
        );
        assert_eq!(continuation.payload["template_id"], "std::schedule.cron");
        assert_eq!(
            continuation.payload["next_turn_input"],
            "run summary for 2026-01-01T00:01:00.000Z"
        );
        assert_eq!(
            continuation.payload["schedule"]["mandate_event_id"],
            mandate.id.to_string()
        );
        assert_eq!(continuation.payload["schedule"]["occurrence_index"], 1);
        assert_eq!(continuation.payload["schedule"]["max_occurrences"], 2);
    }

    #[tokio::test]
    async fn std_schedule_cron_emits_budget_exhausted_for_timer_fired_at_cap() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mandate = append_schedule_mandate_started(
            &store,
            &coordinates,
            &control_stream,
            MandateSubject {
                thread_id: Some(coordinates.thread_id.to_string()),
                loop_id: Some("loop-nightly".to_string()),
            },
            2,
            "run summary",
        )
        .await;
        let fired = append_timer_fired(
            &store,
            &coordinates,
            &control_stream,
            mandate.id,
            2,
            "2026-01-01T00:02:00.000Z",
            mandate.id,
        )
        .await;

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_schedule_cron_timer_coupling(json!({
                        "max_occurrences": 2,
                        "schedule_id": "nightly-summary",
                        "parent_turn_id": "turn-nightly-root",
                        "loop_id": "loop-nightly",
                        "mandate_scope": "match_all",
                    }))],
                ),
                fired,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::schedule.cron");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        assert!(
            control_events
                .iter()
                .all(|event| event.kind != EventKind::TurnContinueRequested)
        );
        let exhausted = control_events
            .iter()
            .find(|event| event.kind == EventKind::LoopBudgetExhausted)
            .unwrap();
        assert_eq!(
            exhausted.payload["schema"],
            EventKind::LoopBudgetExhausted.payload_schema_id()
        );
        assert_eq!(exhausted.payload["template_id"], "std::schedule.cron");
        assert_eq!(
            exhausted.payload["mandate_event_id"],
            mandate.id.to_string()
        );
        assert_eq!(exhausted.payload["occurrence_index"], 2);
        assert_eq!(exhausted.payload["max_occurrences"], 2);
    }

    #[tokio::test]
    async fn std_schedule_cron_ignores_timer_fired_for_other_mandate_scope() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mandate = append_schedule_mandate_started(
            &store,
            &coordinates,
            &control_stream,
            MandateSubject {
                thread_id: Some(coordinates.thread_id.to_string()),
                loop_id: Some("loop-nightly".to_string()),
            },
            2,
            "run summary for {scheduled_for}",
        )
        .await;
        let fired = append_timer_fired(
            &store,
            &coordinates,
            &control_stream,
            mandate.id,
            1,
            "2026-01-01T00:01:00.000Z",
            mandate.id,
        )
        .await;

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_schedule_cron_timer_coupling(json!({
                        "max_occurrences": 2,
                        "schedule_id": "nightly-summary",
                        "parent_turn_id": "turn-nightly-root",
                        "loop_id": "loop-nightly",
                        "mandate_scope": {
                            "subject": {
                                "thread_id": coordinates.thread_id.to_string(),
                                "loop_id": "loop-other",
                            }
                        },
                    }))],
                ),
                fired,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::schedule.cron");
        assert!(receipt.runs[0].discharged_event_ids.is_empty());

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        assert!(control_events.iter().all(|event| !matches!(
            event.kind,
            EventKind::TurnContinueRequested | EventKind::LoopBudgetExhausted
        )));
    }

    #[tokio::test]
    async fn std_supervisor_spawn_discharges_spawn_request_and_parent_waiting() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let submitted = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnSubmitted,
                    json!({
                        "schema": EventKind::TurnSubmitted.payload_schema_id(),
                        "turn_id": "parent-turn-1",
                        "entry_id": "entry-1",
                        "input_text": "delegate the release audit",
                    }),
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_supervisor_spawn_coupling(json!({
                        "child_agent_ref": "agent://release-worker",
                        "initial_submission": "collect release evidence",
                        "parent_turn_id": "parent-turn-1",
                        "correlation_id": "spawn-release-worker-1",
                        "block_parent": true,
                        "reason": "delegate release evidence collection",
                    }))],
                ),
                submitted,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(
            receipt.runs[0].coupling_id,
            STD_SUPERVISOR_SPAWN_TEMPLATE_ID
        );
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 2);

        let control_stream = scheduler.stream_id_for(&coordinates, "control");
        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let requested = control_events
            .iter()
            .find(|event| event.kind == EventKind::ThreadSpawnRequested)
            .unwrap();
        assert_eq!(
            requested.payload["schema"],
            EventKind::ThreadSpawnRequested.payload_schema_id()
        );
        assert_eq!(
            requested.payload["template_id"],
            STD_SUPERVISOR_SPAWN_TEMPLATE_ID
        );
        let payload: ThreadSpawnRequestedPayload =
            serde_json::from_value(requested.payload.clone()).unwrap();
        assert_eq!(payload.parent_thread_id, coordinates.thread_id);
        assert_eq!(payload.parent_turn_id.as_deref(), Some("parent-turn-1"));
        assert_eq!(payload.child_agent_ref, "agent://release-worker");
        assert_eq!(payload.initial_submission, "collect release evidence");
        assert_eq!(payload.correlation_id, "spawn-release-worker-1");
        assert!(payload.block_parent);
        assert_eq!(
            requested.provenance.discharged_by.as_deref(),
            Some("coupling:std::supervisor.spawn")
        );

        let waiting = control_events
            .iter()
            .find(|event| event.kind == EventKind::TurnWaiting)
            .unwrap();
        assert_eq!(
            waiting.payload["schema"],
            EventKind::TurnWaiting.payload_schema_id()
        );
        assert_eq!(
            waiting.payload["template_id"],
            STD_SUPERVISOR_SPAWN_TEMPLATE_ID
        );
        assert_eq!(waiting.payload["turn_id"], "parent-turn-1");
        assert_eq!(
            waiting.payload["waiting_on_event_id"],
            requested.id.to_string()
        );
        assert_eq!(waiting.payload["correlation_id"], "spawn-release-worker-1");
    }

    #[tokio::test]
    async fn std_supervisor_spawn_without_threads_spawn_grant_is_refused_and_recorded() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let submitted = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnSubmitted,
                    json!({
                        "schema": EventKind::TurnSubmitted.payload_schema_id(),
                        "turn_id": "parent-turn-1",
                    }),
                )],
            )
            .await
            .unwrap();
        let mut coupling = std_supervisor_spawn_coupling(json!({
            "initial_submission": "collect release evidence",
            "block_parent": true,
        }));
        coupling.grants.retain(|grant| grant != "threads.spawn");

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new("snapshot-a", vec![coupling]),
                submitted,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(
            receipt.runs[0].coupling_id,
            STD_SUPERVISOR_SPAWN_TEMPLATE_ID
        );
        assert_eq!(receipt.runs[0].status, crate::CouplingRunStatus::Failed);
        assert!(
            receipt.runs[0]
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("threads.spawn"))
        );

        let control_stream = scheduler.stream_id_for(&coordinates, "control");
        let control_events = store.read_events(&control_stream, None).await.unwrap();
        assert!(
            control_events
                .iter()
                .all(|event| event.kind != EventKind::ThreadSpawnRequested)
        );
        assert!(control_events.iter().any(|event| {
            event.kind == EventKind::CouplingRunFailed
                && event
                    .payload
                    .get("reason")
                    .and_then(|reason| reason.as_str())
                    .is_some_and(|reason| reason.contains("threads.spawn"))
        }));
    }

    #[tokio::test]
    async fn std_supervisor_child_completion_joins_child_turn_to_parent_control_fact() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let completed = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({
                        "schema": EventKind::TurnCompleted.payload_schema_id(),
                        "turn_id": "child-turn-1",
                        "parent_thread_id": coordinates.thread_id.to_string(),
                        "child_thread_id": "child-thread-1",
                        "status": "completed",
                        "output_text": "child finished release evidence collection",
                    }),
                    EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("runtime:child-thread".to_string()),
                        function: Some("child_turn_completion/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_supervisor_child_completion_coupling()],
                ),
                completed,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(
            receipt.runs[0].coupling_id,
            STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID
        );
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_stream = scheduler.stream_id_for(&coordinates, "control");
        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let joined = control_events
            .iter()
            .find(|event| event.kind == EventKind::LoopCompleted)
            .unwrap();
        assert_eq!(
            joined.payload["schema"],
            EventKind::LoopCompleted.payload_schema_id()
        );
        assert_eq!(
            joined.payload["template_id"],
            STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID
        );
        assert_eq!(joined.payload["child"]["child_thread_id"], "child-thread-1");
        assert_eq!(joined.payload["child"]["child_turn_id"], "child-turn-1");
        assert_eq!(
            joined.provenance.discharged_by.as_deref(),
            Some("coupling:std::supervisor.child_completion")
        );
    }

    #[tokio::test]
    async fn std_permission_tool_gate_allows_tool_call() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let requested =
            append_tool_call_requested(&store, &coordinates, &thread_stream, "call-allow").await;

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_permission_tool_gate_coupling(json!({
                        "decision": "allow",
                        "reason": "allowed by V1 tool gate fixture",
                    }))],
                ),
                requested,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::permission.tool_gate");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let decision = control_events
            .iter()
            .find(|event| event.kind == EventKind::ToolCallDecision)
            .unwrap();
        assert_eq!(
            decision.payload["schema"],
            EventKind::ToolCallDecision.payload_schema_id()
        );
        assert_eq!(decision.payload["template_id"], "std::permission.tool_gate");
        assert_eq!(decision.payload["outcome"]["decision"], "allow");
        assert_eq!(
            decision.provenance.discharged_by.as_deref(),
            Some("coupling:std::permission.tool_gate")
        );
    }

    #[tokio::test]
    async fn std_permission_tool_gate_waits_with_suspension() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let requested =
            append_tool_call_requested(&store, &coordinates, &thread_stream, "call-wait").await;

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_permission_tool_gate_coupling(json!({
                        "decision": "wait",
                        "approval_id": "approval-shell-call",
                        "reason": "operator approval required",
                    }))],
                ),
                requested,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(receipt.runs[0].coupling_id, "std::permission.tool_gate");
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let suspended = control_events
            .iter()
            .find(|event| event.kind == EventKind::ToolCallSuspended)
            .unwrap();
        assert_eq!(
            suspended.payload["schema"],
            EventKind::ToolCallSuspended.payload_schema_id()
        );
        assert_eq!(
            suspended.payload["template_id"],
            "std::permission.tool_gate"
        );
        assert_eq!(suspended.payload["approval_id"], "approval-shell-call");
        assert_eq!(suspended.payload["subject"]["call_id"], "call-wait");
    }

    #[tokio::test]
    async fn std_permission_approval_gate_requests_and_suspends_tool_call() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let store = InMemorySessionStore::default();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let requested =
            append_tool_call_requested(&store, &coordinates, &thread_stream, "call-approve").await;
        let request_event_id = requested[0].id.to_string();

        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_permission_approval_gate_coupling(json!({
                        "approval_id": "approval-shell-call",
                        "reason": "operator approval required",
                        "resume_token": "resume-shell-call",
                    }))],
                ),
                requested,
            )
            .await
            .unwrap();

        assert_eq!(receipt.runs.len(), 1);
        assert_eq!(
            receipt.runs[0].coupling_id,
            STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID
        );
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 2);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let approval = control_events
            .iter()
            .find(|event| event.kind == EventKind::ApprovalRequested)
            .unwrap();
        assert_eq!(
            approval.payload["schema"],
            EventKind::ApprovalRequested.payload_schema_id()
        );
        assert_eq!(
            approval.payload["template_id"],
            STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID
        );
        assert_eq!(approval.payload["approval_id"], "approval-shell-call");
        assert_eq!(approval.payload["kind"], "tool.call");
        assert_eq!(approval.payload["subject"]["turn_id"], "turn-1");
        assert_eq!(approval.payload["subject"]["call_id"], "call-approve");
        assert_eq!(approval.payload["request_event_id"], request_event_id);
        assert_eq!(approval.payload["resume_token"], "resume-shell-call");
        assert_eq!(
            approval.provenance.discharged_by.as_deref(),
            Some("coupling:std::permission.approval_gate")
        );

        let suspended = control_events
            .iter()
            .find(|event| event.kind == EventKind::ToolCallSuspended)
            .unwrap();
        assert_eq!(
            suspended.payload["schema"],
            EventKind::ToolCallSuspended.payload_schema_id()
        );
        assert_eq!(
            suspended.payload["template_id"],
            STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID
        );
        assert_eq!(suspended.payload["approval_id"], "approval-shell-call");
        assert_eq!(
            suspended.payload["approval_requested_event_role"],
            "approval_request"
        );
        assert_eq!(suspended.payload["subject"]["call_id"], "call-approve");
        assert_eq!(suspended.payload["request_event_id"], request_event_id);
        assert_eq!(suspended.payload["resume_token"], "resume-shell-call");
    }

    async fn append_failed_coupling_run(
        store: &InMemorySessionStore,
        coordinates: &ThreadCoordinates,
        control_stream: &EventStreamId,
        fields: serde_json::Value,
    ) -> Vec<crate::EventRecord> {
        let mut payload = json!({
            "coupling_id": "std::queue.task",
            "status": "failed",
            "root_event_id": EventRecordId::new().to_string(),
            "trigger_event_id": EventRecordId::new().to_string(),
            "trigger_stream_id": "thread:test",
            "trigger_sequence": 1,
            "snapshot_id": "snapshot-a",
            "depth": 0,
            "source_cut": {"entries": []},
            "source_event_ids": [],
            "discharged_event_ids": [],
            "function_ref": "op://std-queue-task/run@sha256:test",
            "config_hash": "sha256:queue-task",
            "budget_spent": {"discharge_events": 0}
        });
        if let Some(object) = payload.as_object_mut()
            && let Some(fields) = fields.as_object()
        {
            object.extend(fields.clone());
        }
        store
            .append_events(
                control_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::CouplingRunFailed,
                    payload,
                    EventProvenance {
                        source_streams: vec![EventStreamId::for_thread(coordinates)],
                        discharged_by: Some("coupling:std::queue.task".to_string()),
                        function: Some("op://std-queue-task/run@sha256:test".to_string()),
                        config_hash: Some("sha256:queue-task".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
    }

    async fn append_schedule_mandate_started(
        store: &InMemorySessionStore,
        coordinates: &ThreadCoordinates,
        control_stream: &EventStreamId,
        subject: MandateSubject,
        max_occurrences: u32,
        input_template: &str,
    ) -> EventRecord {
        store
            .append_events(
                control_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::MandateStarted,
                    serde_json::to_value(MandateStartedPayload {
                        subject,
                        mandate_id: "mandate-nightly-summary".to_string(),
                        snapshot_id: "schedule.v1".to_string(),
                        thread_id: Some(coordinates.thread_id.to_string()),
                        max_continuations: None,
                        expires_at_ms: None,
                        schedule: Some(MandateSchedulePayload::Interval { every_ms: 60_000 }),
                        max_occurrences: Some(max_occurrences),
                        catch_up: Some(MandateCatchUpPolicy::SkipMissed),
                        input_template: Some(input_template.to_string()),
                    })
                    .unwrap(),
                )],
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
    }

    async fn append_timer_fired(
        store: &InMemorySessionStore,
        coordinates: &ThreadCoordinates,
        control_stream: &EventStreamId,
        mandate_event_id: EventRecordId,
        occurrence_index: u64,
        scheduled_for: &str,
        provenance_event_id: EventRecordId,
    ) -> Vec<EventRecord> {
        let mut record = NewEventRecord::witnessed(
            coordinates.clone(),
            EventKind::TimerFired,
            serde_json::to_value(TimerFiredPayload {
                mandate_event_id,
                scheduled_for: scheduled_for.to_string(),
                occurrence_index,
                catch_up: false,
            })
            .unwrap(),
        );
        record.provenance = EventProvenance {
            source_streams: vec![control_stream.clone()],
            source_event_ids: vec![provenance_event_id],
            ..EventProvenance::default()
        };
        store
            .append_events(control_stream, vec![record])
            .await
            .unwrap()
    }

    async fn append_tool_call_requested(
        store: &InMemorySessionStore,
        coordinates: &ThreadCoordinates,
        thread_stream: &EventStreamId,
        call_id: &str,
    ) -> Vec<crate::EventRecord> {
        store
            .append_events(
                thread_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ToolCallRequested,
                    json!({
                        "schema": EventKind::ToolCallRequested.payload_schema_id(),
                        "subject": {
                            "turn_id": "turn-1",
                            "call_id": call_id,
                        },
                        "snapshot_id": "snapshot-a",
                        "tool_name": "shell.exec",
                        "arguments": {
                            "cmd": "date",
                        },
                    }),
                    EventProvenance {
                        source_streams: vec![EventStreamId::for_thread(coordinates)],
                        discharged_by: Some("runtime:provider-loop".to_string()),
                        function: Some("provider_tool_request/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
    }

    fn std_queue_task_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::queue.task".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::TurnSubmitted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::TurnSubmitted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::TurnWaiting],
            },
            function_ref: format!("op://std-queue-task/run@sha256:{}", "a".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-queue-task".to_string(),
                artifact_hash: "a".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({}),
            config_hash: "sha256:queue-task".to_string(),
        }
    }

    fn std_queue_completion_callback() -> BoundCoupling {
        BoundCoupling {
            id: "std::queue.completion_callback".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::CouplingRunCompleted,
            trigger_match: [("coupling_id".to_string(), json!("std::queue.task"))]
                .into_iter()
                .collect(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![EventKind::CouplingRunCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::LoopCompleted],
            },
            function_ref: format!(
                "op://std-queue-completion-callback/run@sha256:{}",
                "b".repeat(64)
            ),
            function: BoundCouplingFunction {
                name: "std-queue-completion-callback".to_string(),
                artifact_hash: "b".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:control".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({
                "watch_coupling_id": "std::queue.task",
                "on_completed": "complete_loop",
            }),
            config_hash: "sha256:queue-callback".to_string(),
        }
    }

    fn std_context_spill_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::context.spill".to_string(),
            role: CouplingRole::Projection,
            trigger_kind: EventKind::ContextCompileCompleted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::ContextCompileCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "derived:context".to_string(),
                kinds: vec![
                    EventKind::ContextSummaryCompleted,
                    EventKind::ContextReadPlanSet,
                ],
            },
            function_ref: format!("op://std-context-spill/run@sha256:{}", "c".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-context-spill".to_string(),
                artifact_hash: "c".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:derived:context".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config: json!({}),
            config_hash: "sha256:context-spill".to_string(),
        }
    }

    fn std_context_truncate_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::context.truncate".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::ContextCompileCompleted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::ContextCompileCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::ContextReadPlanSet],
            },
            function_ref: format!("op://std-context-truncate/run@sha256:{}", "d".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-context-truncate".to_string(),
                artifact_hash: "d".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({
                "retain_tail_events": 3,
                "reason": "fixture keeps only the raw tail",
            }),
            config_hash: "sha256:context-truncate".to_string(),
        }
    }

    fn std_context_summarize_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::context.summarize".to_string(),
            role: CouplingRole::Projection,
            trigger_kind: EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::SessionEntryAppended, EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "derived:context".to_string(),
                kinds: vec![
                    EventKind::ContextSummaryCompleted,
                    EventKind::ContextReadPlanSet,
                ],
            },
            function_ref: format!("op://std-context-summarize/run@sha256:{}", "e".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-context-summarize".to_string(),
                artifact_hash: "e".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:derived:context".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config: json!({}),
            config_hash: "sha256:context-summarize".to_string(),
        }
    }

    fn std_prompt_steer_continuation_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::prompt.steer".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::TurnContinueRequested],
            },
            function_ref: format!("op://std-prompt-steer/run@sha256:{}", "h".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-prompt-steer".to_string(),
                artifact_hash: "h".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({
                "action": "request_continuation",
                "parent_turn_id": "turn-1",
                "loop_id": "prompt-steer",
                "next_turn_input": "Ask the user to pick the deployment lane.",
                "reason": "need explicit release lane choice"
            }),
            config_hash: "sha256:prompt-steer-continue".to_string(),
        }
    }

    fn std_prompt_steer_read_plan_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::prompt.steer".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::ApprovalResolved,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![EventKind::ApprovalResolved],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::ContextReadPlanSet],
            },
            function_ref: format!("op://std-prompt-steer/run@sha256:{}", "h".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-prompt-steer".to_string(),
                artifact_hash: "h".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:control".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({
                "action": "set_read_plan",
                "checkpoint_event_id": EventRecordId::new().to_string(),
                "checkpoint_stream_id": "derived:context:instruction-fixture",
                "event_role": "instruction_checkpoint",
                "reason": "approved steering instructions"
            }),
            config_hash: "sha256:prompt-steer-read-plan".to_string(),
        }
    }

    fn std_failure_deadletter_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::failure.deadletter".to_string(),
            role: CouplingRole::Projection,
            trigger_kind: EventKind::CouplingRunFailed,
            trigger_match: [("status".to_string(), json!("failed"))]
                .into_iter()
                .collect(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![EventKind::CouplingRunFailed, EventKind::LoopBlocked],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "derived:deadletter".to_string(),
                kinds: vec![EventKind::CouplingRunFailed],
            },
            function_ref: format!("op://std-failure-deadletter/run@sha256:{}", "d".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-failure-deadletter".to_string(),
                artifact_hash: "d".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:control".to_string(),
                "stream.write:derived:deadletter".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({
                "reason": "deadletter failed control facts for inspection",
            }),
            config_hash: "sha256:failure-deadletter".to_string(),
        }
    }

    fn std_retry_with_budget_coupling(max_attempts: u32) -> BoundCoupling {
        BoundCoupling {
            id: "std::retry.with_budget".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::CouplingRunFailed,
            trigger_match: [("status".to_string(), json!("failed"))]
                .into_iter()
                .collect(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![EventKind::CouplingRunFailed],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    EventKind::TurnContinueRequested,
                    EventKind::LoopBudgetExhausted,
                ],
            },
            function_ref: format!("op://std-retry-with-budget/run@sha256:{}", "e".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-retry-with-budget".to_string(),
                artifact_hash: "e".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:control".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({
                "max_attempts": max_attempts,
                "parent_turn_id": "turn-1",
                "loop_id": "loop-1",
                "next_turn_input": "retry last failed step",
                "retryable_error_classes": ["retryable"],
            }),
            config_hash: "sha256:retry-with-budget".to_string(),
        }
    }

    fn std_schedule_cron_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::schedule.cron".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::TimerFired,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![
                    EventKind::MandateStarted,
                    EventKind::MandateRevoked,
                    EventKind::TimerFired,
                ],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    EventKind::TurnContinueRequested,
                    EventKind::LoopBudgetExhausted,
                ],
            },
            function_ref: format!("op://std-schedule-cron/run@sha256:{}", "s".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-schedule-cron".to_string(),
                artifact_hash: "s".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:control".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({
                "max_occurrences": 2,
                "parent_turn_id": "turn-nightly-root",
                "loop_id": "loop-nightly",
                "next_turn_input": "run scheduled nightly summary",
            }),
            config_hash: "sha256:schedule-cron".to_string(),
        }
    }

    fn std_schedule_cron_timer_coupling(config: serde_json::Value) -> BoundCoupling {
        let mut coupling = std_schedule_cron_coupling();
        coupling.config = config;
        coupling
    }

    fn std_supervisor_spawn_coupling(config: serde_json::Value) -> BoundCoupling {
        BoundCoupling {
            id: STD_SUPERVISOR_SPAWN_TEMPLATE_ID.to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::TurnSubmitted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::TurnSubmitted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::ThreadSpawnRequested, EventKind::TurnWaiting],
            },
            function_ref: format!("op://std-supervisor-spawn/run@sha256:{}", "i".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-supervisor-spawn".to_string(),
                artifact_hash: "i".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
                "threads.spawn".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config,
            config_hash: "sha256:supervisor-spawn".to_string(),
        }
    }

    fn std_supervisor_child_completion_coupling() -> BoundCoupling {
        BoundCoupling {
            id: STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID.to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::TurnContinueRequested, EventKind::LoopCompleted],
            },
            function_ref: format!(
                "op://std-supervisor-child-completion/run@sha256:{}",
                "j".repeat(64)
            ),
            function: BoundCouplingFunction {
                name: "std-supervisor-child-completion".to_string(),
                artifact_hash: "j".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({
                "on_completed": "complete_loop",
                "reason": "child work joined back to supervisor"
            }),
            config_hash: "sha256:supervisor-child-completion".to_string(),
        }
    }

    fn std_permission_tool_gate_coupling(config: serde_json::Value) -> BoundCoupling {
        BoundCoupling {
            id: "std::permission.tool_gate".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::ToolCallRequested,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::ToolCallRequested],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::ToolCallDecision, EventKind::ToolCallSuspended],
            },
            function_ref: format!(
                "op://std-permission-tool-gate/run@sha256:{}",
                "p".repeat(64)
            ),
            function: BoundCouplingFunction {
                name: "std-permission-tool-gate".to_string(),
                artifact_hash: "p".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config,
            config_hash: "sha256:permission-tool-gate".to_string(),
        }
    }

    fn std_permission_approval_gate_coupling(config: serde_json::Value) -> BoundCoupling {
        BoundCoupling {
            id: STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID.to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::ToolCallRequested,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::ToolCallRequested],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::ApprovalRequested, EventKind::ToolCallSuspended],
            },
            function_ref: format!(
                "op://std-permission-approval-gate/run@sha256:{}",
                "q".repeat(64)
            ),
            function: BoundCouplingFunction {
                name: "std-permission-approval-gate".to_string(),
                artifact_hash: "q".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config,
            config_hash: "sha256:permission-approval-gate".to_string(),
        }
    }

    fn std_memory_extract_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::memory.extract".to_string(),
            role: CouplingRole::Projection,
            trigger_kind: EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::TurnCompleted, EventKind::ToolCallCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "derived:memory".to_string(),
                kinds: vec![EventKind::ContextSummaryCompleted],
            },
            function_ref: format!("op://std-memory-extract/run@sha256:{}", "f".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-memory-extract".to_string(),
                artifact_hash: "f".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:derived:memory".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({}),
            config_hash: "sha256:memory-extract".to_string(),
        }
    }

    fn std_memory_recall_coupling() -> BoundCoupling {
        BoundCoupling {
            id: "std::memory.recall".to_string(),
            role: CouplingRole::Projection,
            trigger_kind: EventKind::TurnSubmitted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "derived:memory".to_string(),
                kinds: vec![EventKind::ContextSummaryCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "derived:context".to_string(),
                kinds: vec![EventKind::ContextReadPlanSet],
            },
            function_ref: format!("op://std-memory-recall/run@sha256:{}", "f".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-memory-recall".to_string(),
                artifact_hash: "f".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:derived:memory".to_string(),
                "stream.write:derived:context".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: json!({}),
            config_hash: "sha256:memory-recall".to_string(),
        }
    }
}
