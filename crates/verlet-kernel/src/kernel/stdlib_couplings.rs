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

#[async_trait::async_trait]
impl crate::kernel::coupling_scheduler::CouplingExecutor for StdlibCouplingExecutor {
    async fn invoke(
        &self,
        request: crate::kernel::coupling_scheduler::CouplingInvocation,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::kernel::coupling_scheduler::CouplingExecutionResult,
    > {
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
            id => Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!("stdlib coupling executor does not implement template {id:?}"),
            )),
        }
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default)]
struct QueueTaskConfig {
    task_prefix: Option<String>,
    reason: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default)]
struct ContextSpillConfig {
    summary_event_id: Option<verlet_history::EventRecordId>,
    summary_text: Option<String>,
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
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

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default)]
struct ContextSummarizeConfig {
    summary_event_id: Option<verlet_history::EventRecordId>,
    summary_text: Option<String>,
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
    reason: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default)]
struct MemoryExtractConfig {
    #[serde(alias = "memory_event_id")]
    checkpoint_event_id: Option<verlet_history::EventRecordId>,
    #[serde(alias = "memory_text")]
    text: Option<String>,
    #[serde(alias = "memory_kind")]
    observation_kind: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
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

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default)]
struct PromptDynamicInstructionsConfig {
    instruction_event_id: Option<verlet_history::EventRecordId>,
    instruction_text: Option<String>,
    instruction_name: Option<String>,
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default)]
struct PromptSteerConfig {
    action: PromptSteerAction,
    reason: Option<String>,
    loop_id: Option<String>,
    parent_turn_id: Option<String>,
    next_turn_input: Option<String>,
    read_plan_name: Option<String>,
    pipeline_id: Option<String>,
    checkpoint_event_id: Option<verlet_history::EventRecordId>,
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

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PromptSteerAction {
    #[default]
    RequestContinuation,
    SetReadPlan,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default)]
struct PermissionToolGateConfig {
    decision: PermissionToolGateDecision,
    reason: Option<String>,
    approval_id: Option<String>,
    arguments: Option<serde_json::Value>,
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

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default)]
struct PermissionApprovalGateConfig {
    approval_id: Option<String>,
    reason: Option<String>,
    resume_token: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum PermissionToolGateDecision {
    #[default]
    Allow,
    Rewrite,
    Deny,
    Wait,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default)]
struct FailureDeadletterConfig {
    reason: Option<String>,
    queue: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
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

#[derive(Clone, Debug, serde::Deserialize)]
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

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScheduleCronMandateScope {
    MatchAll,
    Subject(crate::kernel::control_decision::MandateSubject),
}

impl Default for ScheduleCronMandateScope {
    fn default() -> Self {
        Self::MatchAll
    }
}

impl ScheduleCronMandateScope {
    fn matches(&self, subject: &crate::kernel::control_decision::MandateSubject) -> bool {
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

#[derive(Clone, Debug, serde::Deserialize)]
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

#[derive(Clone, Debug, Default, serde::Deserialize)]
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

#[derive(Clone, Debug, serde::Deserialize)]
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

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum SupervisorChildCompletionAction {
    #[default]
    CompleteLoop,
    RequestContinuation,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum QueueCompletionAction {
    #[default]
    CompleteLoop,
    RequestContinuation,
}

fn invoke_queue_task(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if request.trigger_event.kind != verlet_history::EventKind::TurnSubmitted {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_QUEUE_TASK_TEMPLATE_ID} expected turn.submitted trigger, got {}",
                request.trigger_event.kind
            ),
        ));
    }
    let config = queue_task_config(&request.coupling.config)?;
    let turn_id = request
        .trigger_event
        .payload
        .get("turn_id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "std::queue.task trigger payload is missing turn_id".to_string(),
            )
        })?;
    let task_prefix = config.task_prefix.as_deref().unwrap_or("task");
    let mut payload = serde_json::Map::from_iter([
        (
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::TurnWaiting.payload_schema_id()),
        ),
        (
            "template_id".to_string(),
            serde_json::json!(STD_QUEUE_TASK_TEMPLATE_ID),
        ),
        (
            "snapshot_id".to_string(),
            serde_json::json!(request.activation.snapshot_id),
        ),
        ("turn_id".to_string(), serde_json::json!(turn_id)),
        (
            "task_id".to_string(),
            serde_json::json!(format!("{task_prefix}:{}", request.trigger_event.id)),
        ),
        (
            "waiting_on_event_id".to_string(),
            serde_json::json!(request.trigger_event.id.to_string()),
        ),
        ("status".to_string(), serde_json::json!("queued")),
        (
            "reason".to_string(),
            serde_json::json!(
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
        payload.insert("entry_id".to_string(), serde_json::json!(entry_id));
    }

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: verlet_history::EventKind::TurnWaiting,
            payload: serde_json::Value::Object(payload),
        }],
    })
}

fn invoke_queue_completion_callback(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if request.trigger_event.kind != verlet_history::EventKind::CouplingRunCompleted {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID} expected coupling.run.completed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
    }
    let config = queue_completion_config(&request.coupling.config)?;
    let completed = serde_json::from_value::<crate::kernel::coupling_scheduler::CouplingRunReceipt>(
        request.trigger_event.payload.clone(),
    )
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::queue.completion_callback trigger payload is not a coupling run receipt: {err}"
        ))
    })?;
    if completed.status != crate::kernel::coupling_scheduler::CouplingRunStatus::Completed {
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult::default());
    }
    if let Some(watch_coupling_id) = &config.watch_coupling_id
        && completed.coupling_id != *watch_coupling_id
    {
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult::default());
    }

    let discharge = match config.on_completed {
        QueueCompletionAction::CompleteLoop => {
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: verlet_history::EventKind::LoopCompleted,
                payload: serde_json::json!({
                    "schema": verlet_history::EventKind::LoopCompleted.payload_schema_id(),
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
            }
        }
        QueueCompletionAction::RequestContinuation => {
            let next_turn_input = config.next_turn_input.ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "std::queue.completion_callback request_continuation requires next_turn_input"
                        .to_string(),
                )
            })?;
            let parent_turn_id = config.parent_turn_id.ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "std::queue.completion_callback request_continuation requires parent_turn_id"
                        .to_string(),
                )
            })?;
            let payload = crate::kernel::control_decision::TurnContinueRequestedPayload {
                subject: crate::kernel::control_decision::TurnContinuationSubject {
                    loop_id: config.loop_id.unwrap_or_else(|| "default".to_string()),
                    parent_turn_id,
                },
                snapshot_id: request.activation.snapshot_id,
                next_turn_input,
            };
            let mut payload = serde_json::to_value(payload).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "std::queue.completion_callback continuation payload codec failed: {err}"
                ))
            })?;
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "schema".to_string(),
                    serde_json::json!(
                        verlet_history::EventKind::TurnContinueRequested.payload_schema_id()
                    ),
                );
                object.insert(
                    "template_id".to_string(),
                    serde_json::json!(STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID),
                );
                object.insert(
                    "completed_coupling_id".to_string(),
                    serde_json::json!(completed.coupling_id),
                );
            }
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: verlet_history::EventKind::TurnContinueRequested,
                payload,
            }
        }
    };

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![discharge],
    })
}

fn invoke_supervisor_spawn(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if !matches!(
        request.trigger_event.kind,
        verlet_history::EventKind::TurnSubmitted | verlet_history::EventKind::ToolCallRequested
    ) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_SUPERVISOR_SPAWN_TEMPLATE_ID} expected turn.submitted or tool.call.requested trigger, got {}",
                request.trigger_event.kind
            ),
        ));
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
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
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

    let payload = verlet_history::ThreadSpawnRequestedPayload {
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
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::supervisor.spawn request payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::ThreadSpawnRequested.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            serde_json::json!(STD_SUPERVISOR_SPAWN_TEMPLATE_ID),
        );
        object.insert(
            "snapshot_id".to_string(),
            serde_json::json!(request.activation.snapshot_id),
        );
        object.insert(
            "trigger_event_id".to_string(),
            serde_json::json!(request.trigger_event.id.to_string()),
        );
        object.insert(
            "trigger_kind".to_string(),
            serde_json::json!(request.trigger_event.kind.to_string()),
        );
        object.insert(
            "reason".to_string(),
            serde_json::json!(
                config
                    .reason
                    .unwrap_or_else(|| "supervisor spawn requested".to_string())
            ),
        );
    }

    let request_event_id = verlet_history::EventRecordId::new();
    let mut discharges = vec![crate::kernel::coupling_scheduler::CouplingDischarge {
        event_id: Some(request_event_id),
        stream: "control".to_string(),
        kind: verlet_history::EventKind::ThreadSpawnRequested,
        payload,
    }];

    if config.block_parent {
        let parent_turn_id = parent_turn_id.ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "std::supervisor.spawn block_parent requires parent_turn_id or trigger turn_id"
                    .to_string(),
            )
        })?;
        discharges.push(crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: verlet_history::EventKind::TurnWaiting,
            payload: serde_json::json!({
                "schema": verlet_history::EventKind::TurnWaiting.payload_schema_id(),
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

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult { discharges })
}

fn invoke_context_spill(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if request.trigger_event.kind != verlet_history::EventKind::ContextCompileCompleted {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_CONTEXT_SPILL_TEMPLATE_ID} expected context.compile.completed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
    }
    let config = context_spill_config(&request.coupling.config)?;
    let source_ranges = context_spill_source_ranges(&request);
    let summary_text = config
        .summary_text
        .unwrap_or_else(|| context_spill_summary_text(&request));
    let summary_event_id = config
        .summary_event_id
        .unwrap_or_else(verlet_history::EventRecordId::new);
    let read_plan_name = config
        .read_plan_name
        .unwrap_or_else(|| "history.default".to_string());
    let pipeline_id = config
        .pipeline_id
        .unwrap_or_else(|| "context.default".to_string());
    let summary_hash = verlet_agent::contracts::sha256_hex(summary_text.as_bytes());
    let summary_payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": summary_text,
        "covered_ranges": source_ranges_json(&source_ranges),
        "content": {
            "sha256": summary_hash,
        },
        "template_id": STD_CONTEXT_SPILL_TEMPLATE_ID,
        "compile_event_id": request.trigger_event.id.to_string(),
    });
    let read_plan_payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": request.trigger_event.stream_id.as_str(),
        "summary_event_id": summary_event_id.to_string(),
        "template_id": STD_CONTEXT_SPILL_TEMPLATE_ID,
        "read_plan": {
            "schema": verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": request.trigger_event.stream_id.as_str(),
            "frontier": "compile_frontier",
            "entries": summary_checkpoint_entries(summary_event_id, &source_ranges),
        },
    });

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: Some(summary_event_id),
                stream: "derived:context".to_string(),
                kind: verlet_history::EventKind::ContextSummaryCompleted,
                payload: summary_payload,
            },
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "derived:context".to_string(),
                kind: verlet_history::EventKind::ContextReadPlanSet,
                payload: read_plan_payload,
            },
        ],
    })
}

fn invoke_context_truncate(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if request.trigger_event.kind != verlet_history::EventKind::ContextCompileCompleted {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_CONTEXT_TRUNCATE_TEMPLATE_ID} expected context.compile.completed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
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
    let payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": request.trigger_event.stream_id.as_str(),
        "template_id": STD_CONTEXT_TRUNCATE_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.to_string(),
        "retain_tail_events": retain_tail_events,
        "reason": reason,
        "read_plan": {
            "schema": verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": request.trigger_event.stream_id.as_str(),
            "frontier": "compile_frontier",
            "entries": entries,
        },
    });

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: verlet_history::EventKind::ContextReadPlanSet,
            payload,
        }],
    })
}

fn invoke_context_summarize(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if !matches!(
        request.trigger_event.kind,
        verlet_history::EventKind::ContextCompileCompleted
            | verlet_history::EventKind::TurnCompleted
    ) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_CONTEXT_SUMMARIZE_TEMPLATE_ID} expected context.compile.completed or turn.completed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
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
    let summary_event_id = config
        .summary_event_id
        .unwrap_or_else(verlet_history::EventRecordId::new);
    let read_plan_name = config
        .read_plan_name
        .unwrap_or_else(|| "history.default".to_string());
    let pipeline_id = config
        .pipeline_id
        .unwrap_or_else(|| "context.summarize".to_string());
    let reason = config
        .reason
        .unwrap_or_else(|| "summary checkpoint selected".to_string());
    let content_hash = verlet_agent::contracts::sha256_hex(summary_text.as_bytes());
    let summary_payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": summary_text,
        "covered_ranges": source_ranges_json(&source_ranges),
        "content": {
            "sha256": content_hash,
        },
        "template_id": STD_CONTEXT_SUMMARIZE_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.to_string(),
        "snapshot_id": request.activation.snapshot_id,
        "reason": reason,
    });
    let read_plan_payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": request.trigger_event.stream_id.as_str(),
        "summary_event_id": summary_event_id.to_string(),
        "template_id": STD_CONTEXT_SUMMARIZE_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.to_string(),
        "read_plan": {
            "schema": verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": request.trigger_event.stream_id.as_str(),
            "frontier": "compile_frontier",
            "entries": summary_checkpoint_entries(summary_event_id, &source_ranges),
        },
    });

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: Some(summary_event_id),
                stream: "derived:context".to_string(),
                kind: verlet_history::EventKind::ContextSummaryCompleted,
                payload: summary_payload,
            },
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "derived:context".to_string(),
                kind: verlet_history::EventKind::ContextReadPlanSet,
                payload: read_plan_payload,
            },
        ],
    })
}

fn invoke_memory_extract(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if !matches!(
        request.trigger_event.kind,
        verlet_history::EventKind::TurnCompleted | verlet_history::EventKind::ToolCallCompleted
    ) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_MEMORY_EXTRACT_TEMPLATE_ID} expected turn.completed or tool.call.completed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
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
    let content_hash = verlet_agent::contracts::sha256_hex(memory_text.as_bytes());
    let payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": memory_text,
        "covered_ranges": source_ranges_json(&source_ranges),
        "content": {
            "sha256": content_hash,
        },
        "template_id": STD_MEMORY_EXTRACT_TEMPLATE_ID,
        "memory_kind": memory_kind,
        "source_event_id": request.trigger_event.id.to_string(),
        "source_kind": request.trigger_event.kind.to_string(),
        "snapshot_id": request.activation.snapshot_id,
    });

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: memory_event_id,
            stream: "derived:memory".to_string(),
            kind: verlet_history::EventKind::ContextSummaryCompleted,
            payload,
        }],
    })
}

fn memory_text_from_payload(payload: &serde_json::Value) -> Option<String> {
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

fn invoke_memory_recall(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if !matches!(
        request.trigger_event.kind,
        verlet_history::EventKind::TurnSubmitted
            | verlet_history::EventKind::ContextCompileCompleted
    ) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_MEMORY_RECALL_TEMPLATE_ID} expected turn.submitted or context.compile.completed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
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
        .filter(|event| event.kind == verlet_history::EventKind::ContextSummaryCompleted)
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
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult::default());
    }
    let source_stream = memories[0].stream_id.as_str();
    let selected_event_ids = memories
        .iter()
        .map(|event| event.id.to_string())
        .collect::<Vec<_>>();
    let entries = memories
        .iter()
        .map(|event| {
            serde_json::json!({
                "kind": "event_ref",
                "stream_id": event.stream_id.as_str(),
                "event_id": event.id.to_string(),
                "event_role": "memory_checkpoint",
            })
        })
        .collect::<Vec<_>>();
    let payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": source_stream,
        "template_id": STD_MEMORY_RECALL_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.to_string(),
        "snapshot_id": request.activation.snapshot_id,
        "selected_event_ids": selected_event_ids,
        "read_plan": {
            "schema": verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1,
            "name": read_plan_name,
            "source_stream": source_stream,
            "frontier": "compile_frontier",
            "entries": entries,
        },
    });

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "derived:context".to_string(),
            kind: verlet_history::EventKind::ContextReadPlanSet,
            payload,
        }],
    })
}

fn memory_kind_matches(payload: &serde_json::Value, expected: Option<&str>) -> bool {
    expected.is_none_or(|expected| {
        payload.get("memory_kind").and_then(|value| value.as_str()) == Some(expected)
    })
}

fn invoke_prompt_steer(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if !matches!(
        request.trigger_event.kind,
        verlet_history::EventKind::TurnCompleted | verlet_history::EventKind::ApprovalResolved
    ) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_PROMPT_STEER_TEMPLATE_ID} expected turn.completed or approval.resolved trigger, got {}",
                request.trigger_event.kind
            ),
        ));
    }
    let config = prompt_steer_config(&request.coupling.config)?;
    match config.action {
        PromptSteerAction::RequestContinuation => invoke_prompt_steer_continuation(request, config),
        PromptSteerAction::SetReadPlan => invoke_prompt_steer_read_plan(request, config),
    }
}

fn invoke_prompt_steer_continuation(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
    config: PromptSteerConfig,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    let parent_turn_id = config
        .parent_turn_id
        .or_else(|| {
            payload_string(
                &request.trigger_event.payload,
                &["parent_turn_id", "turn_id"],
            )
        })
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "std::prompt.steer request_continuation requires parent_turn_id".to_string(),
            )
        })?;
    let next_turn_input = config.next_turn_input.ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "std::prompt.steer request_continuation requires next_turn_input".to_string(),
        )
    })?;
    let payload = crate::kernel::control_decision::TurnContinueRequestedPayload {
        subject: crate::kernel::control_decision::TurnContinuationSubject {
            loop_id: config.loop_id.unwrap_or_else(|| "prompt.steer".to_string()),
            parent_turn_id,
        },
        snapshot_id: request.activation.snapshot_id.clone(),
        next_turn_input,
    };
    let mut payload = serde_json::to_value(payload).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::prompt.steer payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::TurnContinueRequested.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            serde_json::json!(STD_PROMPT_STEER_TEMPLATE_ID),
        );
        object.insert(
            "trigger_event_id".to_string(),
            serde_json::json!(request.trigger_event.id.to_string()),
        );
        object.insert(
            "trigger_kind".to_string(),
            serde_json::json!(request.trigger_event.kind.to_string()),
        );
        object.insert(
            "reason".to_string(),
            serde_json::json!(
                config
                    .reason
                    .unwrap_or_else(|| "prompt steering requested continuation".to_string())
            ),
        );
    }

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: verlet_history::EventKind::TurnContinueRequested,
            payload,
        }],
    })
}

fn invoke_prompt_steer_read_plan(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
    config: PromptSteerConfig,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    let checkpoint_event_id = config.checkpoint_event_id.ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
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
    let payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": checkpoint_stream_id,
        "checkpoint_event_id": checkpoint_event_id.to_string(),
        "template_id": STD_PROMPT_STEER_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.to_string(),
        "snapshot_id": request.activation.snapshot_id,
        "reason": config
            .reason
            .unwrap_or_else(|| "prompt steering selected read plan".to_string()),
        "read_plan": {
            "schema": verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1,
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

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: verlet_history::EventKind::ContextReadPlanSet,
            payload,
        }],
    })
}

fn invoke_prompt_dynamic_instructions(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if !matches!(
        request.trigger_event.kind,
        verlet_history::EventKind::ManifestBindCompleted
            | verlet_history::EventKind::ContextCompileCompleted
    ) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID} expected manifest.bind.completed or context.compile.completed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
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
        .unwrap_or_else(verlet_history::EventRecordId::new);
    let source_ranges = context_spill_source_ranges(&request);
    let content_hash = verlet_agent::contracts::sha256_hex(instruction_text.as_bytes());
    let derived_context_stream = format!(
        "derived:context:{}",
        request.trigger_event.coordinates.thread_id
    );
    let summary_payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
        "role": "summary_checkpoint",
        "text": instruction_text,
        "covered_ranges": source_ranges_json(&source_ranges),
        "content": {
            "sha256": content_hash,
        },
        "template_id": STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID,
        "instruction_name": instruction_name,
        "source_event_id": request.trigger_event.id.to_string(),
        "source_kind": request.trigger_event.kind.to_string(),
        "snapshot_id": request.activation.snapshot_id,
    });
    let read_plan_payload = serde_json::json!({
        "schema": verlet_history::EventKind::ContextReadPlanSet.payload_schema_id(),
        "scope": "thread",
        "name": read_plan_name,
        "pipeline_id": pipeline_id,
        "source_id": derived_context_stream,
        "instruction_event_id": instruction_event_id.to_string(),
        "template_id": STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID,
        "trigger_event_id": request.trigger_event.id.to_string(),
        "trigger_kind": request.trigger_event.kind.to_string(),
        "read_plan": {
            "schema": verlet_history::CONTEXT_READ_PLAN_SCHEMA_V1,
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

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: Some(instruction_event_id),
                stream: "derived:context".to_string(),
                kind: verlet_history::EventKind::ContextSummaryCompleted,
                payload: summary_payload,
            },
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "derived:context".to_string(),
                kind: verlet_history::EventKind::ContextReadPlanSet,
                payload: read_plan_payload,
            },
        ],
    })
}

fn instruction_text_from_payload(payload: &serde_json::Value) -> Option<String> {
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
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if !matches!(
        request.trigger_event.kind,
        verlet_history::EventKind::CouplingRunFailed | verlet_history::EventKind::LoopBlocked
    ) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_FAILURE_DEADLETTER_TEMPLATE_ID} expected coupling.run.failed or loop.blocked trigger, got {}",
                request.trigger_event.kind
            ),
        ));
    }
    if request
        .trigger_event
        .payload
        .get("role")
        .and_then(|value| value.as_str())
        == Some("deadletter_projection")
    {
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult::default());
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
    let payload = serde_json::json!({
        "schema": verlet_history::EventKind::CouplingRunFailed.payload_schema_id(),
        "role": "deadletter_projection",
        "template_id": STD_FAILURE_DEADLETTER_TEMPLATE_ID,
        "status": "deadlettered",
        "queue": queue,
        "snapshot_id": request.activation.snapshot_id,
        "deadletter_id": format!("deadletter:{}", request.trigger_event.id),
        "source_event_id": request.trigger_event.id.to_string(),
        "source_kind": request.trigger_event.kind.to_string(),
        "source_stream_id": request.trigger_event.stream_id.as_str(),
        "source_sequence": request.trigger_event.sequence.get(),
        "reason": reason,
        "failure": request.trigger_event.payload,
    });

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "derived:deadletter".to_string(),
            kind: verlet_history::EventKind::CouplingRunFailed,
            payload,
        }],
    })
}

fn invoke_permission_tool_gate(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if request.trigger_event.kind != verlet_history::EventKind::ToolCallRequested {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_PERMISSION_TOOL_GATE_TEMPLATE_ID} expected tool.call.requested trigger, got {}",
                request.trigger_event.kind
            ),
        ));
    }
    let config = permission_tool_gate_config(&request.coupling.config)?;
    let requested = serde_json::from_value::<
        crate::kernel::control_decision::ToolCallRequestedPayload,
    >(request.trigger_event.payload.clone())
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::permission.tool_gate trigger payload codec failed: {err}"
        ))
    })?;

    match config.decision {
        PermissionToolGateDecision::Allow => permission_tool_decision_result(
            &request,
            &requested,
            crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
            config.reason,
        ),
        PermissionToolGateDecision::Rewrite => {
            let arguments = config.arguments.ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "std::permission.tool_gate rewrite requires arguments".to_string(),
                )
            })?;
            permission_tool_decision_result(
                &request,
                &requested,
                crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Rewrite {
                    arguments,
                },
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
                crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Deny {
                    reason: reason.clone(),
                },
                Some(reason),
            )
        }
        PermissionToolGateDecision::Wait => {
            let payload = crate::kernel::control_decision::ToolCallSuspendedPayload {
                subject: requested.subject,
                snapshot_id: request.activation.snapshot_id.clone(),
                approval_id: config.approval_id,
                reason: config
                    .reason
                    .or_else(|| Some("waiting on std::permission.tool_gate".to_string())),
            };
            let mut payload = serde_json::to_value(payload).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "std::permission.tool_gate suspended payload codec failed: {err}"
                ))
            })?;
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "schema".to_string(),
                    serde_json::json!(
                        verlet_history::EventKind::ToolCallSuspended.payload_schema_id()
                    ),
                );
                object.insert(
                    "template_id".to_string(),
                    serde_json::json!(STD_PERMISSION_TOOL_GATE_TEMPLATE_ID),
                );
                object.insert(
                    "tool_name".to_string(),
                    serde_json::json!(requested.tool_name),
                );
                object.insert(
                    "request_event_id".to_string(),
                    serde_json::json!(request.trigger_event.id.to_string()),
                );
            }
            Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
                discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
                    event_id: None,
                    stream: "control".to_string(),
                    kind: verlet_history::EventKind::ToolCallSuspended,
                    payload,
                }],
            })
        }
    }
}

fn invoke_permission_approval_gate(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if request.trigger_event.kind != verlet_history::EventKind::ToolCallRequested {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID} expected tool.call.requested trigger, got {}",
                request.trigger_event.kind
            ),
        ));
    }
    let config = permission_approval_gate_config(&request.coupling.config)?;
    let requested = serde_json::from_value::<
        crate::kernel::control_decision::ToolCallRequestedPayload,
    >(request.trigger_event.payload.clone())
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
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

    let approval_requested = serde_json::json!({
        "schema": verlet_history::EventKind::ApprovalRequested.payload_schema_id(),
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

    let suspended_payload = crate::kernel::control_decision::ToolCallSuspendedPayload {
        subject,
        snapshot_id,
        approval_id: Some(approval_id),
        reason,
    };
    let mut suspended = serde_json::to_value(suspended_payload).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::permission.approval_gate suspended payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = suspended.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::ToolCallSuspended.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            serde_json::json!(STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID),
        );
        object.insert(
            "approval_requested_event_role".to_string(),
            serde_json::json!("approval_request"),
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

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: verlet_history::EventKind::ApprovalRequested,
                payload: approval_requested,
            },
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: verlet_history::EventKind::ToolCallSuspended,
                payload: suspended,
            },
        ],
    })
}

fn permission_tool_decision_result(
    request: &crate::kernel::coupling_scheduler::CouplingInvocation,
    requested: &crate::kernel::control_decision::ToolCallRequestedPayload,
    outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload,
    reason: Option<String>,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    let payload = crate::kernel::control_decision::ToolCallDecisionPayload {
        subject: requested.subject.clone(),
        snapshot_id: request.activation.snapshot_id.clone(),
        outcome,
        admissible: None,
    };
    let mut payload = serde_json::to_value(payload).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::permission.tool_gate decision payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::ToolCallDecision.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            serde_json::json!(STD_PERMISSION_TOOL_GATE_TEMPLATE_ID),
        );
        object.insert(
            "tool_name".to_string(),
            serde_json::json!(requested.tool_name),
        );
        object.insert(
            "request_event_id".to_string(),
            serde_json::json!(request.trigger_event.id.to_string()),
        );
        if let Some(reason) = reason {
            object.insert("reason".to_string(), serde_json::json!(reason));
        }
    }

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: verlet_history::EventKind::ToolCallDecision,
            payload,
        }],
    })
}

fn invoke_schedule_cron(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    match request.trigger_event.kind {
        verlet_history::EventKind::MandateStarted => {
            Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult::default())
        }
        verlet_history::EventKind::TimerFired => {
            let config = schedule_cron_config(&request.coupling.config)?;
            invoke_schedule_cron_timer_fired(request, config)
        }
        kind => Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!("{STD_SCHEDULE_CRON_TEMPLATE_ID} expected timer.fired trigger, got {kind}"),
        )),
    }
}

fn invoke_schedule_cron_timer_fired(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
    config: ScheduleCronConfig,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    let timer = timer_fired_payload(&request)?;
    let Some((mandate_event, mandate)) = timer_fired_mandate(&request, &timer)? else {
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult::default());
    };
    if !config.mandate_scope.matches(&mandate.subject) {
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult::default());
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
                serde_json::json!(timer.occurrence_index),
            );
            object.insert(
                "timer_event_id".to_string(),
                serde_json::json!(request.trigger_event.id.to_string()),
            );
        }
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
            discharges: vec![discharge],
        });
    }

    let parent_turn_id = config.parent_turn_id.ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "std::schedule.cron continuation requires parent_turn_id".to_string(),
        )
    })?;
    let input_template = mandate
        .input_template
        .clone()
        .or(config.next_turn_input)
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "std::schedule.cron continuation requires input_template".to_string(),
            )
        })?;
    let next_turn_input = render_schedule_input_template(&input_template, &timer.scheduled_for);
    let payload = crate::kernel::control_decision::TurnContinueRequestedPayload {
        subject: crate::kernel::control_decision::TurnContinuationSubject {
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
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::schedule.cron payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::TurnContinueRequested.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            serde_json::json!(STD_SCHEDULE_CRON_TEMPLATE_ID),
        );
        object.insert(
            "reason".to_string(),
            serde_json::json!(
                config
                    .reason
                    .unwrap_or_else(|| "scheduled occurrence accepted".to_string())
            ),
        );
        object.insert(
            "schedule".to_string(),
            serde_json::json!({
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

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: verlet_history::EventKind::TurnContinueRequested,
            payload,
        }],
    })
}

fn timer_fired_payload(
    request: &crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::TimerFiredPayload> {
    serde_json::from_value(request.trigger_event.payload.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::schedule.cron timer payload failed: {err}"
        ))
    })
}

fn timer_fired_mandate(
    request: &crate::kernel::coupling_scheduler::CouplingInvocation,
    timer: &verlet_history::TimerFiredPayload,
) -> crate::kernel::runtime_host::VerletResult<
    Option<(
        verlet_history::EventRecord,
        crate::kernel::control_decision::MandateStartedPayload,
    )>,
> {
    if !request
        .trigger_event
        .provenance
        .source_event_ids
        .contains(&timer.mandate_event_id)
    {
        return Ok(None);
    }
    let Some(event) = request.source_events.iter().find(|event| {
        event.id == timer.mandate_event_id
            && event.kind == verlet_history::EventKind::MandateStarted
    }) else {
        return Ok(None);
    };
    let mandate = serde_json::from_value::<crate::kernel::control_decision::MandateStartedPayload>(
        event.payload.clone(),
    )
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::schedule.cron mandate payload failed: {err}"
        ))
    })?;
    Ok(Some((event.clone(), mandate)))
}

fn render_schedule_input_template(template: &str, scheduled_for: &str) -> String {
    template.replace("{scheduled_for}", scheduled_for)
}

fn schedule_budget_exhausted_discharge(
    request: &crate::kernel::coupling_scheduler::CouplingInvocation,
    schedule_id: &str,
    mandate_event_id: verlet_history::EventRecordId,
    mandate_id: Option<&str>,
    occurrence: u64,
    max_occurrences: u32,
) -> crate::kernel::coupling_scheduler::CouplingDischarge {
    crate::kernel::coupling_scheduler::CouplingDischarge {
        event_id: None,
        stream: "control".to_string(),
        kind: verlet_history::EventKind::LoopBudgetExhausted,
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::LoopBudgetExhausted.payload_schema_id(),
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
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if !matches!(
        request.trigger_event.kind,
        verlet_history::EventKind::TurnCompleted | verlet_history::EventKind::CouplingRunCompleted
    ) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID} expected turn.completed or coupling.run.completed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
    }
    let config = supervisor_child_completion_config(&request.coupling.config)?;
    let Some(completion) = supervisor_child_completion_fact(&request, &config)? else {
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult::default());
    };

    let discharge = match config.on_completed {
        SupervisorChildCompletionAction::CompleteLoop => {
            let mut payload = serde_json::Map::from_iter([
                (
                    "schema".to_string(),
                    serde_json::json!(verlet_history::EventKind::LoopCompleted.payload_schema_id()),
                ),
                (
                    "template_id".to_string(),
                    serde_json::json!(STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID),
                ),
                (
                    "snapshot_id".to_string(),
                    serde_json::json!(request.activation.snapshot_id),
                ),
                (
                    "trigger_event_id".to_string(),
                    serde_json::json!(request.trigger_event.id.to_string()),
                ),
                (
                    "trigger_kind".to_string(),
                    serde_json::json!(request.trigger_event.kind.to_string()),
                ),
                (
                    "reason".to_string(),
                    serde_json::json!(
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

            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: verlet_history::EventKind::LoopCompleted,
                payload: serde_json::Value::Object(payload),
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
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "std::supervisor.child_completion request_continuation requires parent_turn_id"
                        .to_string(),
                )
                })?;
            let next_turn_input = config.next_turn_input.ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "std::supervisor.child_completion request_continuation requires next_turn_input"
                        .to_string(),
                )
            })?;
            let payload = crate::kernel::control_decision::TurnContinueRequestedPayload {
                subject: crate::kernel::control_decision::TurnContinuationSubject {
                    loop_id: config.loop_id.unwrap_or_else(|| "supervisor".to_string()),
                    parent_turn_id,
                },
                snapshot_id: request.activation.snapshot_id.clone(),
                next_turn_input,
            };
            let mut payload = serde_json::to_value(payload).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "std::supervisor.child_completion continuation payload codec failed: {err}"
                ))
            })?;
            if let Some(object) = payload.as_object_mut() {
                object.insert(
                    "schema".to_string(),
                    serde_json::json!(
                        verlet_history::EventKind::TurnContinueRequested.payload_schema_id()
                    ),
                );
                object.insert(
                    "template_id".to_string(),
                    serde_json::json!(STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID),
                );
                object.insert(
                    "trigger_event_id".to_string(),
                    serde_json::json!(request.trigger_event.id.to_string()),
                );
                object.insert(
                    "trigger_kind".to_string(),
                    serde_json::json!(request.trigger_event.kind.to_string()),
                );
                object.insert(
                    "reason".to_string(),
                    serde_json::json!(config.reason.unwrap_or_else(|| {
                        "supervised child completion requested continuation".to_string()
                    })),
                );
                object.insert(
                    "child".to_string(),
                    supervisor_child_completion_json(&completion),
                );
            }
            crate::kernel::coupling_scheduler::CouplingDischarge {
                event_id: None,
                stream: "control".to_string(),
                kind: verlet_history::EventKind::TurnContinueRequested,
                payload,
            }
        }
    };

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
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
    completed_trigger_event_id: Option<verlet_history::EventRecordId>,
    completed_discharged_event_ids: Vec<verlet_history::EventRecordId>,
}

fn supervisor_child_completion_fact(
    request: &crate::kernel::coupling_scheduler::CouplingInvocation,
    config: &SupervisorChildCompletionConfig,
) -> crate::kernel::runtime_host::VerletResult<Option<SupervisorChildCompletionFact>> {
    match request.trigger_event.kind {
        verlet_history::EventKind::TurnCompleted => Ok(Some(SupervisorChildCompletionFact {
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
        verlet_history::EventKind::CouplingRunCompleted => {
            let completed = serde_json::from_value::<crate::kernel::coupling_scheduler::CouplingRunReceipt>(
                request.trigger_event.payload.clone(),
            )
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "std::supervisor.child_completion trigger payload is not a coupling run receipt: {err}"
                ))
            })?;
            if completed.status != crate::kernel::coupling_scheduler::CouplingRunStatus::Completed {
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

fn supervisor_child_completion_json(
    completion: &SupervisorChildCompletionFact,
) -> serde_json::Value {
    let mut child =
        serde_json::Map::from_iter([("status".to_string(), serde_json::json!(completion.status))]);
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
        child.insert(
            "completed_coupling_id".to_string(),
            serde_json::json!(coupling_id),
        );
    }
    if let Some(trigger_event_id) = completion.completed_trigger_event_id {
        child.insert(
            "completed_trigger_event_id".to_string(),
            serde_json::json!(trigger_event_id.to_string()),
        );
    }
    if !completion.completed_discharged_event_ids.is_empty() {
        child.insert(
            "completed_discharged_event_ids".to_string(),
            serde_json::json!(
                completion
                    .completed_discharged_event_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            ),
        );
    }
    serde_json::Value::Object(child)
}

fn invoke_retry_with_budget(
    request: crate::kernel::coupling_scheduler::CouplingInvocation,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::coupling_scheduler::CouplingExecutionResult,
> {
    if request.trigger_event.kind != verlet_history::EventKind::CouplingRunFailed {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "{STD_RETRY_WITH_BUDGET_TEMPLATE_ID} expected coupling.run.failed trigger, got {}",
                request.trigger_event.kind
            ),
        ));
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
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
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
        return Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
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
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "std::retry.with_budget continuation requires parent_turn_id".to_string(),
        )
    })?;
    let next_turn_input = config.next_turn_input.ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "std::retry.with_budget continuation requires next_turn_input".to_string(),
        )
    })?;
    let next_attempt = attempt + 1;
    let payload = crate::kernel::control_decision::TurnContinueRequestedPayload {
        subject: crate::kernel::control_decision::TurnContinuationSubject {
            loop_id: config.loop_id.unwrap_or_else(|| "default".to_string()),
            parent_turn_id,
        },
        snapshot_id: request.activation.snapshot_id.clone(),
        next_turn_input,
    };
    let mut payload = serde_json::to_value(payload).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::retry.with_budget payload codec failed: {err}"
        ))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::TurnContinueRequested.payload_schema_id()),
        );
        object.insert(
            "template_id".to_string(),
            serde_json::json!(STD_RETRY_WITH_BUDGET_TEMPLATE_ID),
        );
        object.insert(
            "reason".to_string(),
            serde_json::json!(
                config
                    .reason
                    .unwrap_or_else(|| "retry requested by std::retry.with_budget".to_string())
            ),
        );
        object.insert(
            "retry".to_string(),
            serde_json::json!({
                "attempt": next_attempt,
                "previous_attempt": attempt,
                "max_attempts": config.max_attempts,
                "failed_event_id": request.trigger_event.id.to_string(),
                "error_class": error_class,
            }),
        );
    }

    Ok(crate::kernel::coupling_scheduler::CouplingExecutionResult {
        discharges: vec![crate::kernel::coupling_scheduler::CouplingDischarge {
            event_id: None,
            stream: "control".to_string(),
            kind: verlet_history::EventKind::TurnContinueRequested,
            payload,
        }],
    })
}

fn retry_budget_exhausted_discharge(
    request: &crate::kernel::coupling_scheduler::CouplingInvocation,
    attempt: u32,
    max_attempts: u32,
    error_class: Option<String>,
    reason: String,
) -> crate::kernel::coupling_scheduler::CouplingDischarge {
    crate::kernel::coupling_scheduler::CouplingDischarge {
        event_id: None,
        stream: "control".to_string(),
        kind: verlet_history::EventKind::LoopBudgetExhausted,
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::LoopBudgetExhausted.payload_schema_id(),
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

fn payload_string(payload: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        payload
            .get(key)
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
    })
}

fn source_payload_string(events: &[verlet_history::EventRecord], keys: &[&str]) -> Option<String> {
    events
        .iter()
        .rev()
        .find_map(|event| payload_string(&event.payload, keys))
}

fn insert_optional_string(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<String>,
) {
    if let Some(value) = value {
        map.insert(key.to_string(), serde_json::json!(value));
    }
}

fn context_spill_source_ranges(
    request: &crate::kernel::coupling_scheduler::CouplingInvocation,
) -> Vec<verlet_history::ObservationSourceRange> {
    if !request.trigger_event.provenance.source_ranges.is_empty() {
        return request.trigger_event.provenance.source_ranges.clone();
    }
    request
        .source_cut
        .entries
        .iter()
        .map(|entry| verlet_history::ObservationSourceRange {
            stream_id: verlet_history::EventStreamId::new(entry.stream_id.clone()),
            from_sequence: verlet_history::EventSequence::new(1),
            to_sequence: verlet_history::EventSequence::new(entry.max_sequence),
        })
        .collect()
}

fn context_spill_summary_text(
    request: &crate::kernel::coupling_scheduler::CouplingInvocation,
) -> String {
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

fn source_ranges_json(
    source_ranges: &[verlet_history::ObservationSourceRange],
) -> Vec<serde_json::Value> {
    source_ranges
        .iter()
        .map(|range| {
            serde_json::json!({
                "stream_id": range.stream_id.as_str(),
                "from_sequence": range.from_sequence.get(),
                "to_sequence": range.to_sequence.get(),
            })
        })
        .collect()
}

fn truncate_read_plan_entries(
    source_ranges: &[verlet_history::ObservationSourceRange],
    retain_tail_events: i64,
    reason: &str,
) -> Vec<serde_json::Value> {
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
                entries.push(serde_json::json!({
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
            entries.push(serde_json::json!({
                "kind": "raw_range",
                "stream_id": range.stream_id.as_str(),
                "range": {
                    "from": read_plan_from_cursor(verlet_history::EventSequence::new(retain_from_sequence)),
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
    summary_event_id: verlet_history::EventRecordId,
    source_ranges: &[verlet_history::ObservationSourceRange],
) -> Vec<serde_json::Value> {
    if source_ranges.is_empty() {
        return vec![serde_json::json!({
            "kind": "event_ref",
            "event_id": summary_event_id.to_string(),
            "event_role": "summary_checkpoint",
        })];
    }
    source_ranges
        .iter()
        .map(|range| {
            serde_json::json!({
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

fn read_plan_from_cursor(sequence: verlet_history::EventSequence) -> serde_json::Value {
    if sequence.get() <= 1 {
        serde_json::Value::String("start".to_string())
    } else {
        serde_json::json!({ "sequence": sequence.get() - 1 })
    }
}

fn queue_task_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<QueueTaskConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::queue.task config codec failed: {err}"
        ))
    })
}

fn queue_completion_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<QueueCompletionCallbackConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::queue.completion_callback config codec failed: {err}"
        ))
    })
}

fn context_spill_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<ContextSpillConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::context.spill config codec failed: {err}"
        ))
    })
}

fn context_truncate_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<ContextTruncateConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::context.truncate config codec failed: {err}"
        ))
    })
}

fn context_summarize_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<ContextSummarizeConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::context.summarize config codec failed: {err}"
        ))
    })
}

fn memory_extract_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<MemoryExtractConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::memory.extract config codec failed: {err}"
        ))
    })
}

fn memory_recall_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<MemoryRecallConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::memory.recall config codec failed: {err}"
        ))
    })
}

fn prompt_steer_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<PromptSteerConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::prompt.steer config codec failed: {err}"
        ))
    })
}

fn prompt_dynamic_instructions_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<PromptDynamicInstructionsConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::prompt.dynamic_instructions config codec failed: {err}"
        ))
    })
}

fn permission_tool_gate_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<PermissionToolGateConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::permission.tool_gate config codec failed: {err}"
        ))
    })
}

fn permission_approval_gate_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<PermissionApprovalGateConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::permission.approval_gate config codec failed: {err}"
        ))
    })
}

fn failure_deadletter_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<FailureDeadletterConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::failure.deadletter config codec failed: {err}"
        ))
    })
}

fn retry_with_budget_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<RetryWithBudgetConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::retry.with_budget config codec failed: {err}"
        ))
    })
}

fn schedule_cron_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<ScheduleCronConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::schedule.cron config codec failed: {err}"
        ))
    })
}

fn supervisor_spawn_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<SupervisorSpawnConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::supervisor.spawn config codec failed: {err}"
        ))
    })
}

fn supervisor_child_completion_config(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<SupervisorChildCompletionConfig> {
    serde_json::from_value(value.clone()).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "std::supervisor.child_completion config codec failed: {err}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use verlet_history::EventStore as _;

    #[test]
    fn stdlib_executor_supports_exact_runtime_executable_catalog_templates() {
        let catalog = crate::agent::coupling_templates::coupling_template_catalog_v1();
        let declared = catalog
            .templates
            .iter()
            .filter(|template| template.runtime_executable)
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>();
        let supported = catalog
            .templates
            .iter()
            .filter(|template| {
                crate::kernel::stdlib_couplings::StdlibCouplingExecutor::supports_template(
                    &template.id,
                )
            })
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(supported, declared);
    }

    #[tokio::test]
    async fn std_queue_task_and_completion_callback_discharge_control_facts() {
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let submitted = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnSubmitted,
                    serde_json::json!({
                        "turn_id": "turn-1",
                        "entry_id": "entry-1",
                    }),
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
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
            .find(|event| event.kind == verlet_history::EventKind::TurnWaiting)
            .unwrap();
        assert_eq!(
            waiting.payload["schema"],
            verlet_history::EventKind::TurnWaiting.payload_schema_id()
        );
        assert_eq!(waiting.payload["template_id"], "std::queue.task");
        assert_eq!(waiting.payload["turn_id"], "turn-1");
        assert_eq!(
            waiting.provenance.discharged_by.as_deref(),
            Some("coupling:std::queue.task")
        );

        let completed = control_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::LoopCompleted)
            .unwrap();
        assert_eq!(
            completed.payload["schema"],
            verlet_history::EventKind::LoopCompleted.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let source_range = verlet_history::ObservationSourceRange {
            stream_id: thread_stream.clone(),
            from_sequence: verlet_history::EventSequence::new(1),
            to_sequence: verlet_history::EventSequence::new(4),
        };
        let compiled = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ContextCompileCompleted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::ContextCompileCompleted.payload_schema_id(),
                        "truncated_text_bytes": 640,
                        "read_plan": {
                            "schema": "cooldis.context.read_plan/1",
                            "name": "history.default",
                            "source_stream": thread_stream.as_str(),
                            "frontier": "compile_frontier",
                            "entries": []
                        }
                    }),
                    verlet_history::EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        source_ranges: vec![source_range],
                        discharged_by: Some("projection:test-context-compiler".to_string()),
                        function: Some("test_context_compile/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_context_spill_coupling()],
                ),
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
            .find(|event| event.kind == verlet_history::EventKind::ContextSummaryCompleted)
            .unwrap();
        let read_plan = derived_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::ContextReadPlanSet)
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let source_range = verlet_history::ObservationSourceRange {
            stream_id: thread_stream.clone(),
            from_sequence: verlet_history::EventSequence::new(1),
            to_sequence: verlet_history::EventSequence::new(10),
        };
        let compiled = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ContextCompileCompleted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::ContextCompileCompleted.payload_schema_id(),
                        "truncated_text_bytes": 1200,
                        "read_plan": {
                            "schema": "cooldis.context.read_plan/1",
                            "name": "history.default",
                            "source_stream": thread_stream.as_str(),
                            "frontier": "compile_frontier",
                            "entries": []
                        }
                    }),
                    verlet_history::EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        source_ranges: vec![source_range],
                        discharged_by: Some("projection:test-context-compiler".to_string()),
                        function: Some("test_context_compile/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_context_truncate_coupling()],
                ),
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
            .find(|event| event.kind == verlet_history::EventKind::ContextReadPlanSet)
            .unwrap();
        assert_eq!(
            read_plan.payload["schema"],
            verlet_history::EventKind::ContextReadPlanSet.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let completed = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnCompleted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::TurnCompleted.payload_schema_id(),
                        "turn_id": "turn-1",
                        "output_text": "The user wants SQLite first, S2 later, and explicit segment maps.",
                    }),
                    verlet_history::EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("runtime:provider-loop".to_string()),
                        function: Some("turn_completion/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_context_summarize_coupling()],
                ),
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
            .find(|event| event.kind == verlet_history::EventKind::ContextSummaryCompleted)
            .unwrap();
        let read_plan = derived_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::ContextReadPlanSet)
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let completed = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnCompleted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::TurnCompleted.payload_schema_id(),
                        "turn_id": "turn-1",
                        "output_text": "Need one more clarification turn.",
                    }),
                    verlet_history::EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("runtime:provider-loop".to_string()),
                        function: Some("turn_completion/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let continuation_receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
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
            .find(|event| event.kind == verlet_history::EventKind::TurnContinueRequested)
            .unwrap();
        assert_eq!(
            continuation.payload["schema"],
            verlet_history::EventKind::TurnContinueRequested.payload_schema_id()
        );
        assert_eq!(continuation.payload["template_id"], "std::prompt.steer");
        assert_eq!(
            continuation.payload["next_turn_input"],
            "Ask the user to pick the deployment lane."
        );

        let approval = store
            .append_events(
                &control_stream,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::ApprovalResolved,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::ApprovalResolved.payload_schema_id(),
                        "approval_id": "approval-instructions",
                        "decision": "approved",
                    }),
                )],
            )
            .await
            .unwrap();
        let read_plan_receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_prompt_steer_read_plan_coupling()],
                ),
                approval,
            )
            .await
            .unwrap();
        assert_eq!(read_plan_receipt.runs.len(), 1);
        assert_eq!(read_plan_receipt.runs[0].discharged_event_ids.len(), 1);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let read_plan = control_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::ContextReadPlanSet)
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let failed = store
            .append_events(
                &control_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::CouplingRunFailed,
                    serde_json::json!({
                        "coupling_id": "std::queue.task",
                        "status": "failed",
                        "reason": "remote service unavailable",
                        "root_event_id": verlet_history::EventRecordId::new().to_string(),
                        "trigger_event_id": verlet_history::EventRecordId::new().to_string(),
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
                    verlet_history::EventProvenance {
                        source_streams: vec![verlet_history::EventStreamId::for_thread(
                            &coordinates,
                        )],
                        discharged_by: Some("coupling:std::queue.task".to_string()),
                        function: Some("op://std-queue-task/run@sha256:test".to_string()),
                        config_hash: Some("sha256:queue-task".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_failure_deadletter_coupling()],
                ),
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
        assert_eq!(
            deadletter.kind,
            verlet_history::EventKind::CouplingRunFailed
        );
        assert_eq!(
            deadletter.payload["schema"],
            verlet_history::EventKind::CouplingRunFailed.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let completed = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnCompleted,
                    serde_json::json!({
                        "turn_id": "turn-1",
                        "output_text": "User prefers SQLite first, then S2 as stream backend.",
                    }),
                    verlet_history::EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("runtime:provider-loop".to_string()),
                        function: Some("turn_completion/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_memory_extract_coupling()],
                ),
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
        assert_eq!(
            memory.kind,
            verlet_history::EventKind::ContextSummaryCompleted
        );
        assert_eq!(
            memory.payload["schema"],
            verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let memory_stream =
            verlet_history::EventStreamId::new(format!("derived:memory:{}", coordinates.thread_id));
        let memory = store
            .append_events(
                &memory_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ContextSummaryCompleted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
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
                    verlet_history::EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("coupling:std::memory.extract".to_string()),
                        function: Some("op://std-memory-extract/run@sha256:test".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();
        let submitted = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnSubmitted,
                    serde_json::json!({
                        "turn_id": "turn-2",
                        "input_text": "What should we use for V1 stream storage?",
                    }),
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_memory_recall_coupling()],
                ),
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
            .find(|event| event.kind == verlet_history::EventKind::ContextReadPlanSet)
            .unwrap();
        assert_eq!(
            read_plan.payload["schema"],
            verlet_history::EventKind::ContextReadPlanSet.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let failed = append_failed_coupling_run(
            &store,
            &coordinates,
            &control_stream,
            serde_json::json!({
                "attempt": 1,
                "error_class": "retryable",
                "reason": "provider network hiccup",
            }),
        )
        .await;

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_retry_with_budget_coupling(2)],
                ),
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
            .find(|event| event.kind == verlet_history::EventKind::TurnContinueRequested)
            .unwrap();
        assert_eq!(
            retry.payload["schema"],
            verlet_history::EventKind::TurnContinueRequested.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let failed = append_failed_coupling_run(
            &store,
            &coordinates,
            &control_stream,
            serde_json::json!({
                "attempt": 2,
                "error_class": "retryable",
                "reason": "provider network hiccup",
            }),
        )
        .await;

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_retry_with_budget_coupling(2)],
                ),
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
            .find(|event| event.kind == verlet_history::EventKind::LoopBudgetExhausted)
            .unwrap();
        assert_eq!(
            exhausted.payload["schema"],
            verlet_history::EventKind::LoopBudgetExhausted.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mandate = append_schedule_mandate_started(
            &store,
            &coordinates,
            &control_stream,
            crate::kernel::control_decision::MandateSubject {
                thread_id: Some(coordinates.thread_id.to_string()),
                loop_id: Some("loop-nightly".to_string()),
            },
            2,
            "run summary for {scheduled_for}",
        )
        .await;
        let mut coupling = std_schedule_cron_coupling();
        coupling.trigger_kind = verlet_history::EventKind::MandateStarted;

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new("snapshot-a", vec![coupling]),
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
            verlet_history::EventKind::TurnContinueRequested
                | verlet_history::EventKind::LoopBudgetExhausted
        )));
    }

    #[tokio::test]
    async fn std_schedule_cron_requests_continuation_for_timer_fired() {
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mandate = append_schedule_mandate_started(
            &store,
            &coordinates,
            &control_stream,
            crate::kernel::control_decision::MandateSubject {
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

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_schedule_cron_timer_coupling(serde_json::json!({
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
            .find(|event| event.kind == verlet_history::EventKind::TurnContinueRequested)
            .unwrap();
        assert_eq!(
            continuation.payload["schema"],
            verlet_history::EventKind::TurnContinueRequested.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mandate = append_schedule_mandate_started(
            &store,
            &coordinates,
            &control_stream,
            crate::kernel::control_decision::MandateSubject {
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

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_schedule_cron_timer_coupling(serde_json::json!({
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
                .all(|event| event.kind != verlet_history::EventKind::TurnContinueRequested)
        );
        let exhausted = control_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::LoopBudgetExhausted)
            .unwrap();
        assert_eq!(
            exhausted.payload["schema"],
            verlet_history::EventKind::LoopBudgetExhausted.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mandate = append_schedule_mandate_started(
            &store,
            &coordinates,
            &control_stream,
            crate::kernel::control_decision::MandateSubject {
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

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_schedule_cron_timer_coupling(serde_json::json!({
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
            verlet_history::EventKind::TurnContinueRequested
                | verlet_history::EventKind::LoopBudgetExhausted
        )));
    }

    #[tokio::test]
    async fn std_supervisor_spawn_discharges_spawn_request_and_parent_waiting() {
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let submitted = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnSubmitted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::TurnSubmitted.payload_schema_id(),
                        "turn_id": "parent-turn-1",
                        "entry_id": "entry-1",
                        "input_text": "delegate the release audit",
                    }),
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_supervisor_spawn_coupling(serde_json::json!({
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
            crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID
        );
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 2);

        let control_stream = scheduler.stream_id_for(&coordinates, "control");
        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let requested = control_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::ThreadSpawnRequested)
            .unwrap();
        assert_eq!(
            requested.payload["schema"],
            verlet_history::EventKind::ThreadSpawnRequested.payload_schema_id()
        );
        assert_eq!(
            requested.payload["template_id"],
            crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID
        );
        let payload: verlet_history::ThreadSpawnRequestedPayload =
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
            .find(|event| event.kind == verlet_history::EventKind::TurnWaiting)
            .unwrap();
        assert_eq!(
            waiting.payload["schema"],
            verlet_history::EventKind::TurnWaiting.payload_schema_id()
        );
        assert_eq!(
            waiting.payload["template_id"],
            crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID
        );
        assert_eq!(waiting.payload["turn_id"], "parent-turn-1");
        assert_eq!(
            waiting.payload["waiting_on_event_id"],
            requested.id.to_string()
        );
        assert_eq!(waiting.payload["correlation_id"], "spawn-release-worker-1");
    }

    #[tokio::test]
    async fn std_supervisor_child_completion_joins_child_turn_to_parent_control_fact() {
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let completed = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnCompleted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::TurnCompleted.payload_schema_id(),
                        "turn_id": "child-turn-1",
                        "parent_thread_id": coordinates.thread_id.to_string(),
                        "child_thread_id": "child-thread-1",
                        "status": "completed",
                        "output_text": "child finished release evidence collection",
                    }),
                    verlet_history::EventProvenance {
                        source_streams: vec![thread_stream.clone()],
                        discharged_by: Some("runtime:child-thread".to_string()),
                        function: Some("child_turn_completion/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
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
            crate::kernel::stdlib_couplings::STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID
        );
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

        let control_stream = scheduler.stream_id_for(&coordinates, "control");
        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let joined = control_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::LoopCompleted)
            .unwrap();
        assert_eq!(
            joined.payload["schema"],
            verlet_history::EventKind::LoopCompleted.payload_schema_id()
        );
        assert_eq!(
            joined.payload["template_id"],
            crate::kernel::stdlib_couplings::STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let requested =
            append_tool_call_requested(&store, &coordinates, &thread_stream, "call-allow").await;

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_permission_tool_gate_coupling(serde_json::json!({
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
            .find(|event| event.kind == verlet_history::EventKind::ToolCallDecision)
            .unwrap();
        assert_eq!(
            decision.payload["schema"],
            verlet_history::EventKind::ToolCallDecision.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let requested =
            append_tool_call_requested(&store, &coordinates, &thread_stream, "call-wait").await;

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_permission_tool_gate_coupling(serde_json::json!({
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
            .find(|event| event.kind == verlet_history::EventKind::ToolCallSuspended)
            .unwrap();
        assert_eq!(
            suspended.payload["schema"],
            verlet_history::EventKind::ToolCallSuspended.payload_schema_id()
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
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let store = verlet_history::InMemorySessionStore::default();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let requested =
            append_tool_call_requested(&store, &coordinates, &thread_stream, "call-approve").await;
        let request_event_id = requested[0].id.to_string();

        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(&store, &executor);
        let receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_permission_approval_gate_coupling(serde_json::json!({
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
            crate::kernel::stdlib_couplings::STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID
        );
        assert_eq!(receipt.runs[0].discharged_event_ids.len(), 2);

        let control_events = store.read_events(&control_stream, None).await.unwrap();
        let approval = control_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::ApprovalRequested)
            .unwrap();
        assert_eq!(
            approval.payload["schema"],
            verlet_history::EventKind::ApprovalRequested.payload_schema_id()
        );
        assert_eq!(
            approval.payload["template_id"],
            crate::kernel::stdlib_couplings::STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID
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
            .find(|event| event.kind == verlet_history::EventKind::ToolCallSuspended)
            .unwrap();
        assert_eq!(
            suspended.payload["schema"],
            verlet_history::EventKind::ToolCallSuspended.payload_schema_id()
        );
        assert_eq!(
            suspended.payload["template_id"],
            crate::kernel::stdlib_couplings::STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID
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
        store: &verlet_history::InMemorySessionStore,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        control_stream: &verlet_history::EventStreamId,
        fields: serde_json::Value,
    ) -> Vec<verlet_history::EventRecord> {
        let mut payload = serde_json::json!({
            "coupling_id": "std::queue.task",
            "status": "failed",
            "root_event_id": verlet_history::EventRecordId::new().to_string(),
            "trigger_event_id": verlet_history::EventRecordId::new().to_string(),
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
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::CouplingRunFailed,
                    payload,
                    verlet_history::EventProvenance {
                        source_streams: vec![verlet_history::EventStreamId::for_thread(
                            coordinates,
                        )],
                        discharged_by: Some("coupling:std::queue.task".to_string()),
                        function: Some("op://std-queue-task/run@sha256:test".to_string()),
                        config_hash: Some("sha256:queue-task".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
    }

    async fn append_schedule_mandate_started(
        store: &verlet_history::InMemorySessionStore,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        control_stream: &verlet_history::EventStreamId,
        subject: crate::kernel::control_decision::MandateSubject,
        max_occurrences: u32,
        input_template: &str,
    ) -> verlet_history::EventRecord {
        store
            .append_events(
                control_stream,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::MandateStarted,
                    serde_json::to_value(crate::kernel::control_decision::MandateStartedPayload {
                        subject,
                        mandate_id: "mandate-nightly-summary".to_string(),
                        snapshot_id: "schedule.v1".to_string(),
                        thread_id: Some(coordinates.thread_id.to_string()),
                        max_continuations: None,
                        expires_at_ms: None,
                        schedule: Some(
                            crate::kernel::control_decision::MandateSchedulePayload::Interval {
                                every_ms: 60_000,
                            },
                        ),
                        max_occurrences: Some(max_occurrences),
                        catch_up: Some(
                            crate::kernel::control_decision::MandateCatchUpPolicy::SkipMissed,
                        ),
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
        store: &verlet_history::InMemorySessionStore,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        control_stream: &verlet_history::EventStreamId,
        mandate_event_id: verlet_history::EventRecordId,
        occurrence_index: u64,
        scheduled_for: &str,
        provenance_event_id: verlet_history::EventRecordId,
    ) -> Vec<verlet_history::EventRecord> {
        let mut record = verlet_history::NewEventRecord::witnessed(
            coordinates.clone(),
            verlet_history::EventKind::TimerFired,
            serde_json::to_value(verlet_history::TimerFiredPayload {
                mandate_event_id,
                scheduled_for: scheduled_for.to_string(),
                occurrence_index,
                catch_up: false,
            })
            .unwrap(),
        );
        record.provenance = verlet_history::EventProvenance {
            source_streams: vec![control_stream.clone()],
            source_event_ids: vec![provenance_event_id],
            ..verlet_history::EventProvenance::default()
        };
        store
            .append_events(control_stream, vec![record])
            .await
            .unwrap()
    }

    async fn append_tool_call_requested(
        store: &verlet_history::InMemorySessionStore,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        thread_stream: &verlet_history::EventStreamId,
        call_id: &str,
    ) -> Vec<verlet_history::EventRecord> {
        store
            .append_events(
                thread_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ToolCallRequested,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::ToolCallRequested.payload_schema_id(),
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
                    verlet_history::EventProvenance {
                        source_streams: vec![verlet_history::EventStreamId::for_thread(
                            coordinates,
                        )],
                        discharged_by: Some("runtime:provider-loop".to_string()),
                        function: Some("provider_tool_request/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
    }

    fn std_queue_task_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::queue.task".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::TurnSubmitted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::TurnSubmitted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::TurnWaiting],
            },
            function_ref: format!("op://std-queue-task/run@sha256:{}", "a".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-queue-task".to_string(),
                artifact_hash: "a".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({}),
            config_hash: "sha256:queue-task".to_string(),
        }
    }

    fn std_queue_completion_callback() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::queue.completion_callback".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::CouplingRunCompleted,
            trigger_match: [(
                "coupling_id".to_string(),
                serde_json::json!("std::queue.task"),
            )]
            .into_iter()
            .collect(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::CouplingRunCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::LoopCompleted],
            },
            function_ref: format!(
                "op://std-queue-completion-callback/run@sha256:{}",
                "b".repeat(64)
            ),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-queue-completion-callback".to_string(),
                artifact_hash: "b".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({
                "watch_coupling_id": "std::queue.task",
                "on_completed": "complete_loop",
            }),
            config_hash: "sha256:queue-callback".to_string(),
        }
    }

    fn std_context_spill_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::context.spill".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Projection,
            trigger_kind: verlet_history::EventKind::ContextCompileCompleted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::ContextCompileCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "derived:context".to_string(),
                kinds: vec![
                    verlet_history::EventKind::ContextSummaryCompleted,
                    verlet_history::EventKind::ContextReadPlanSet,
                ],
            },
            function_ref: format!("op://std-context-spill/run@sha256:{}", "c".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-context-spill".to_string(),
                artifact_hash: "c".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config: serde_json::json!({}),
            config_hash: "sha256:context-spill".to_string(),
        }
    }

    fn std_context_truncate_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::context.truncate".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::ContextCompileCompleted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::ContextCompileCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::ContextReadPlanSet],
            },
            function_ref: format!("op://std-context-truncate/run@sha256:{}", "d".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-context-truncate".to_string(),
                artifact_hash: "d".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({
                "retain_tail_events": 3,
                "reason": "fixture keeps only the raw tail",
            }),
            config_hash: "sha256:context-truncate".to_string(),
        }
    }

    fn std_context_summarize_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::context.summarize".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Projection,
            trigger_kind: verlet_history::EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![
                    verlet_history::EventKind::SessionEntryAppended,
                    verlet_history::EventKind::TurnCompleted,
                ],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "derived:context".to_string(),
                kinds: vec![
                    verlet_history::EventKind::ContextSummaryCompleted,
                    verlet_history::EventKind::ContextReadPlanSet,
                ],
            },
            function_ref: format!("op://std-context-summarize/run@sha256:{}", "e".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-context-summarize".to_string(),
                artifact_hash: "e".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config: serde_json::json!({}),
            config_hash: "sha256:context-summarize".to_string(),
        }
    }

    fn std_prompt_steer_continuation_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::prompt.steer".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::TurnContinueRequested],
            },
            function_ref: format!("op://std-prompt-steer/run@sha256:{}", "h".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-prompt-steer".to_string(),
                artifact_hash: "h".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({
                "action": "request_continuation",
                "parent_turn_id": "turn-1",
                "loop_id": "prompt-steer",
                "next_turn_input": "Ask the user to pick the deployment lane.",
                "reason": "need explicit release lane choice"
            }),
            config_hash: "sha256:prompt-steer-continue".to_string(),
        }
    }

    fn std_prompt_steer_read_plan_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::prompt.steer".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::ApprovalResolved,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::ApprovalResolved],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::ContextReadPlanSet],
            },
            function_ref: format!("op://std-prompt-steer/run@sha256:{}", "h".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-prompt-steer".to_string(),
                artifact_hash: "h".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({
                "action": "set_read_plan",
                "checkpoint_event_id": verlet_history::EventRecordId::new().to_string(),
                "checkpoint_stream_id": "derived:context:instruction-fixture",
                "event_role": "instruction_checkpoint",
                "reason": "approved steering instructions"
            }),
            config_hash: "sha256:prompt-steer-read-plan".to_string(),
        }
    }

    fn std_failure_deadletter_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::failure.deadletter".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Projection,
            trigger_kind: verlet_history::EventKind::CouplingRunFailed,
            trigger_match: [("status".to_string(), serde_json::json!("failed"))]
                .into_iter()
                .collect(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::CouplingRunFailed,
                    verlet_history::EventKind::LoopBlocked,
                ],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "derived:deadletter".to_string(),
                kinds: vec![verlet_history::EventKind::CouplingRunFailed],
            },
            function_ref: format!("op://std-failure-deadletter/run@sha256:{}", "d".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-failure-deadletter".to_string(),
                artifact_hash: "d".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({
                "reason": "deadletter failed control facts for inspection",
            }),
            config_hash: "sha256:failure-deadletter".to_string(),
        }
    }

    fn std_retry_with_budget_coupling(
        max_attempts: u32,
    ) -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::retry.with_budget".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::CouplingRunFailed,
            trigger_match: [("status".to_string(), serde_json::json!("failed"))]
                .into_iter()
                .collect(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![verlet_history::EventKind::CouplingRunFailed],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::TurnContinueRequested,
                    verlet_history::EventKind::LoopBudgetExhausted,
                ],
            },
            function_ref: format!("op://std-retry-with-budget/run@sha256:{}", "e".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-retry-with-budget".to_string(),
                artifact_hash: "e".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({
                "max_attempts": max_attempts,
                "parent_turn_id": "turn-1",
                "loop_id": "loop-1",
                "next_turn_input": "retry last failed step",
                "retryable_error_classes": ["retryable"],
            }),
            config_hash: "sha256:retry-with-budget".to_string(),
        }
    }

    fn std_schedule_cron_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::schedule.cron".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::TimerFired,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::MandateStarted,
                    verlet_history::EventKind::MandateRevoked,
                    verlet_history::EventKind::TimerFired,
                ],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::TurnContinueRequested,
                    verlet_history::EventKind::LoopBudgetExhausted,
                ],
            },
            function_ref: format!("op://std-schedule-cron/run@sha256:{}", "s".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-schedule-cron".to_string(),
                artifact_hash: "s".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({
                "max_occurrences": 2,
                "parent_turn_id": "turn-nightly-root",
                "loop_id": "loop-nightly",
                "next_turn_input": "run scheduled nightly summary",
            }),
            config_hash: "sha256:schedule-cron".to_string(),
        }
    }

    fn std_schedule_cron_timer_coupling(
        config: serde_json::Value,
    ) -> crate::agent::manifest_bind::BoundCoupling {
        let mut coupling = std_schedule_cron_coupling();
        coupling.config = config;
        coupling
    }

    fn std_supervisor_spawn_coupling(
        config: serde_json::Value,
    ) -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID.to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::TurnSubmitted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::TurnSubmitted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::ThreadSpawnRequested,
                    verlet_history::EventKind::TurnWaiting,
                ],
            },
            function_ref: format!("op://std-supervisor-spawn/run@sha256:{}", "i".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-supervisor-spawn".to_string(),
                artifact_hash: "i".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config,
            config_hash: "sha256:supervisor-spawn".to_string(),
        }
    }

    fn std_supervisor_child_completion_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: crate::kernel::stdlib_couplings::STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID
                .to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::TurnContinueRequested,
                    verlet_history::EventKind::LoopCompleted,
                ],
            },
            function_ref: format!(
                "op://std-supervisor-child-completion/run@sha256:{}",
                "j".repeat(64)
            ),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-supervisor-child-completion".to_string(),
                artifact_hash: "j".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({
                "on_completed": "complete_loop",
                "reason": "child work joined back to supervisor"
            }),
            config_hash: "sha256:supervisor-child-completion".to_string(),
        }
    }

    fn std_permission_tool_gate_coupling(
        config: serde_json::Value,
    ) -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::permission.tool_gate".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::ToolCallRequested,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::ToolCallRequested],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::ToolCallDecision,
                    verlet_history::EventKind::ToolCallSuspended,
                ],
            },
            function_ref: format!(
                "op://std-permission-tool-gate/run@sha256:{}",
                "p".repeat(64)
            ),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-permission-tool-gate".to_string(),
                artifact_hash: "p".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config,
            config_hash: "sha256:permission-tool-gate".to_string(),
        }
    }

    fn std_permission_approval_gate_coupling(
        config: serde_json::Value,
    ) -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: crate::kernel::stdlib_couplings::STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID
                .to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::ToolCallRequested,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::ToolCallRequested],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::ApprovalRequested,
                    verlet_history::EventKind::ToolCallSuspended,
                ],
            },
            function_ref: format!(
                "op://std-permission-approval-gate/run@sha256:{}",
                "q".repeat(64)
            ),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-permission-approval-gate".to_string(),
                artifact_hash: "q".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config,
            config_hash: "sha256:permission-approval-gate".to_string(),
        }
    }

    fn std_memory_extract_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::memory.extract".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Projection,
            trigger_kind: verlet_history::EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![
                    verlet_history::EventKind::TurnCompleted,
                    verlet_history::EventKind::ToolCallCompleted,
                ],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "derived:memory".to_string(),
                kinds: vec![verlet_history::EventKind::ContextSummaryCompleted],
            },
            function_ref: format!("op://std-memory-extract/run@sha256:{}", "f".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-memory-extract".to_string(),
                artifact_hash: "f".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({}),
            config_hash: "sha256:memory-extract".to_string(),
        }
    }

    fn std_memory_recall_coupling() -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: "std::memory.recall".to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Projection,
            trigger_kind: verlet_history::EventKind::TurnSubmitted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "derived:memory".to_string(),
                kinds: vec![verlet_history::EventKind::ContextSummaryCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "derived:context".to_string(),
                kinds: vec![verlet_history::EventKind::ContextReadPlanSet],
            },
            function_ref: format!("op://std-memory-recall/run@sha256:{}", "f".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-memory-recall".to_string(),
                artifact_hash: "f".repeat(64),
                operation_name: Some("run".to_string()),
            },
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config: serde_json::json!({}),
            config_hash: "sha256:memory-recall".to_string(),
        }
    }
}
