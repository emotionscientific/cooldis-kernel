pub const COUPLING_TEMPLATE_CATALOG_SCHEMA_V1: &str = "cooldis.coupling.template_catalog/1";

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CouplingTemplateCatalogV1 {
    pub schema: String,
    pub templates: Vec<CouplingTemplateV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CouplingTemplateV1 {
    pub id: String,
    pub maturity: CouplingTemplateMaturity,
    pub role: crate::agent::manifest_bind::CouplingRole,
    pub runtime_executable: bool,
    pub trigger_kinds: Vec<verlet_history::EventKind>,
    pub source: CouplingTemplateStreamPattern,
    pub sink: CouplingTemplateStreamPattern,
    pub must_have: bool,
    pub channel_decision_required: bool,
    pub summary: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CouplingTemplateMaturity {
    KernelBacked,
    InterfaceOnly,
    ReferenceOnly,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CouplingTemplateStreamPattern {
    pub stream: String,
    pub kinds: Vec<verlet_history::EventKind>,
}

pub fn coupling_template_catalog_v1() -> CouplingTemplateCatalogV1 {
    CouplingTemplateCatalogV1 {
        schema: COUPLING_TEMPLATE_CATALOG_SCHEMA_V1.to_string(),
        templates: vec![
            template(
                "std::queue.task",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[verlet_history::EventKind::TurnSubmitted],
                "thread",
                &[verlet_history::EventKind::TurnSubmitted],
                "control",
                &[
                    verlet_history::EventKind::TurnWaiting,
                    verlet_history::EventKind::CouplingRunCompleted,
                ],
                true,
                false,
                "Turn an event into durable queued work with a later completion fact.",
            ),
            template(
                "std::queue.completion_callback",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[
                    verlet_history::EventKind::CouplingRunCompleted,
                    verlet_history::EventKind::ToolCallCompleted,
                ],
                "control",
                &[verlet_history::EventKind::CouplingRunCompleted],
                "control",
                &[
                    verlet_history::EventKind::TurnContinueRequested,
                    verlet_history::EventKind::LoopCompleted,
                ],
                true,
                false,
                "React to completed queued work by continuing, notifying, or closing the loop.",
            ),
            template(
                "std::context.spill",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Projection,
                &[verlet_history::EventKind::ContextCompileCompleted],
                "thread",
                &[verlet_history::EventKind::ContextCompileCompleted],
                "derived:context",
                &[
                    verlet_history::EventKind::ContextSummaryCompleted,
                    verlet_history::EventKind::ContextReadPlanSet,
                ],
                true,
                false,
                "Project over-budget context into durable context facts for later assembly.",
            ),
            template(
                "std::context.truncate",
                CouplingTemplateMaturity::KernelBacked,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[verlet_history::EventKind::ContextCompileCompleted],
                "thread",
                &[verlet_history::EventKind::ContextCompileCompleted],
                "control",
                &[verlet_history::EventKind::ContextReadPlanSet],
                false,
                false,
                "Select a bounded read plan when the context budget requires dropping raw ranges.",
            ),
            template(
                "std::context.summarize",
                CouplingTemplateMaturity::KernelBacked,
                crate::agent::manifest_bind::CouplingRole::Projection,
                &[
                    verlet_history::EventKind::ContextCompileCompleted,
                    verlet_history::EventKind::TurnCompleted,
                ],
                "thread",
                &[
                    verlet_history::EventKind::SessionEntryAppended,
                    verlet_history::EventKind::TurnCompleted,
                ],
                "derived:context",
                &[
                    verlet_history::EventKind::ContextSummaryCompleted,
                    verlet_history::EventKind::ContextReadPlanSet,
                ],
                false,
                false,
                "Discharge a summary checkpoint plus a read plan over a witnessed source range.",
            ),
            template(
                "std::memory.extract",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Projection,
                &[
                    verlet_history::EventKind::TurnCompleted,
                    verlet_history::EventKind::ToolCallCompleted,
                ],
                "thread",
                &[
                    verlet_history::EventKind::TurnCompleted,
                    verlet_history::EventKind::ToolCallCompleted,
                ],
                "derived:memory",
                &[verlet_history::EventKind::ContextSummaryCompleted],
                false,
                false,
                "Extract durable memory facts from completed turns or tool results.",
            ),
            template(
                "std::memory.recall",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Projection,
                &[
                    verlet_history::EventKind::TurnSubmitted,
                    verlet_history::EventKind::ContextCompileCompleted,
                ],
                "derived:memory",
                &[verlet_history::EventKind::ContextSummaryCompleted],
                "derived:context",
                &[verlet_history::EventKind::ContextReadPlanSet],
                false,
                false,
                "Select memory facts for future context assembly without making memory a primitive.",
            ),
            template(
                "std::permission.approval_gate",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[verlet_history::EventKind::ToolCallRequested],
                "thread",
                &[verlet_history::EventKind::ToolCallRequested],
                "control",
                &[
                    verlet_history::EventKind::ApprovalRequested,
                    verlet_history::EventKind::ToolCallSuspended,
                ],
                false,
                false,
                "Suspend a tool call behind an abstract approval fact; channel-specific HITL delivery remains deferred.",
            ),
            template(
                "std::permission.tool_gate",
                CouplingTemplateMaturity::KernelBacked,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[verlet_history::EventKind::ToolCallRequested],
                "thread",
                &[verlet_history::EventKind::ToolCallRequested],
                "control",
                &[
                    verlet_history::EventKind::ToolCallDecision,
                    verlet_history::EventKind::ToolCallSuspended,
                ],
                false,
                false,
                "Allow, deny, or suspend tool calls through durable control-stream facts.",
            ),
            template(
                "std::prompt.steer",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[
                    verlet_history::EventKind::TurnCompleted,
                    verlet_history::EventKind::ApprovalResolved,
                ],
                "thread",
                &[verlet_history::EventKind::TurnCompleted],
                "control",
                &[
                    verlet_history::EventKind::TurnContinueRequested,
                    verlet_history::EventKind::ContextReadPlanSet,
                ],
                false,
                false,
                "Steer future turns by writing witnessed control facts instead of hidden prompt hooks.",
            ),
            template(
                "std::prompt.dynamic_instructions",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Projection,
                &[
                    verlet_history::EventKind::ManifestBindCompleted,
                    verlet_history::EventKind::ContextCompileCompleted,
                ],
                "thread",
                &[
                    verlet_history::EventKind::ManifestBindCompleted,
                    verlet_history::EventKind::ContextCompileCompleted,
                ],
                "derived:context",
                &[verlet_history::EventKind::ContextReadPlanSet],
                false,
                false,
                "Select versioned instruction material for future context assembly.",
            ),
            template(
                "std::io.channel_ingress",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[verlet_history::EventKind::TurnSubmitted],
                "thread",
                &[verlet_history::EventKind::TurnSubmitted],
                "control",
                &[
                    verlet_history::EventKind::TurnWaiting,
                    verlet_history::EventKind::TurnContinueRequested,
                ],
                false,
                true,
                "Map channel ingress into durable turn/control facts with channel authority outside the kernel.",
            ),
            template(
                "std::io.channel_egress",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[
                    verlet_history::EventKind::TurnCompleted,
                    verlet_history::EventKind::LoopCompleted,
                ],
                "thread",
                &[verlet_history::EventKind::TurnCompleted],
                "control",
                &[
                    verlet_history::EventKind::CouplingRunCompleted,
                    verlet_history::EventKind::CouplingRunFailed,
                ],
                false,
                true,
                "Emit channel egress as a durable operation/coupling result rather than a hidden callback.",
            ),
            template(
                "std::schedule.cron",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[verlet_history::EventKind::TimerFired],
                "control",
                &[
                    verlet_history::EventKind::MandateStarted,
                    verlet_history::EventKind::MandateRevoked,
                    verlet_history::EventKind::TimerFired,
                ],
                "control",
                &[
                    verlet_history::EventKind::TurnContinueRequested,
                    verlet_history::EventKind::LoopBudgetExhausted,
                ],
                false,
                false,
                "Turn witnessed timer firings for a standing mandate into bounded future turn requests.",
            ),
            template(
                "std::supervisor.spawn",
                CouplingTemplateMaturity::KernelBacked,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[
                    verlet_history::EventKind::TurnSubmitted,
                    verlet_history::EventKind::ToolCallRequested,
                ],
                "thread",
                &[
                    verlet_history::EventKind::TurnSubmitted,
                    verlet_history::EventKind::ToolCallRequested,
                ],
                "control",
                &[
                    verlet_history::EventKind::ThreadSpawnRequested,
                    verlet_history::EventKind::TurnWaiting,
                    verlet_history::EventKind::CouplingRunCompleted,
                ],
                false,
                false,
                "Spawn supervised child work through the thread/turn kernel package.",
            ),
            template(
                "std::supervisor.child_completion",
                CouplingTemplateMaturity::KernelBacked,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[
                    verlet_history::EventKind::TurnCompleted,
                    verlet_history::EventKind::CouplingRunCompleted,
                ],
                "thread",
                &[verlet_history::EventKind::TurnCompleted],
                "control",
                &[
                    verlet_history::EventKind::TurnContinueRequested,
                    verlet_history::EventKind::LoopCompleted,
                ],
                false,
                false,
                "Join child thread completion back into a parent continuation or terminal fact.",
            ),
            template(
                "std::retry.with_budget",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Controller,
                &[
                    verlet_history::EventKind::CouplingRunFailed,
                    verlet_history::EventKind::ToolCallCompleted,
                ],
                "control",
                &[verlet_history::EventKind::CouplingRunFailed],
                "control",
                &[
                    verlet_history::EventKind::TurnContinueRequested,
                    verlet_history::EventKind::LoopBudgetExhausted,
                ],
                false,
                false,
                "Retry only from typed failure facts and explicit budget, never from blanket replay.",
            ),
            template(
                "std::failure.deadletter",
                CouplingTemplateMaturity::ReferenceOnly,
                crate::agent::manifest_bind::CouplingRole::Projection,
                &[
                    verlet_history::EventKind::CouplingRunFailed,
                    verlet_history::EventKind::LoopBlocked,
                ],
                "control",
                &[
                    verlet_history::EventKind::CouplingRunFailed,
                    verlet_history::EventKind::LoopBlocked,
                ],
                "derived:deadletter",
                &[verlet_history::EventKind::CouplingRunFailed],
                false,
                false,
                "Project exhausted or blocked work into a durable deadletter stream for inspection.",
            ),
        ],
    }
}

pub fn coupling_template_ids_v1() -> Vec<&'static str> {
    vec![
        "std::queue.task",
        "std::queue.completion_callback",
        "std::context.spill",
        "std::context.truncate",
        "std::context.summarize",
        "std::memory.extract",
        "std::memory.recall",
        "std::permission.approval_gate",
        "std::permission.tool_gate",
        "std::prompt.steer",
        "std::prompt.dynamic_instructions",
        "std::io.channel_ingress",
        "std::io.channel_egress",
        "std::schedule.cron",
        "std::supervisor.spawn",
        "std::supervisor.child_completion",
        "std::retry.with_budget",
        "std::failure.deadletter",
    ]
}

fn template(
    id: &str,
    maturity: CouplingTemplateMaturity,
    role: crate::agent::manifest_bind::CouplingRole,
    trigger_kinds: &[verlet_history::EventKind],
    source_stream: &str,
    source_kinds: &[verlet_history::EventKind],
    sink_stream: &str,
    sink_kinds: &[verlet_history::EventKind],
    must_have: bool,
    channel_decision_required: bool,
    summary: &str,
) -> CouplingTemplateV1 {
    CouplingTemplateV1 {
        id: id.to_string(),
        maturity,
        role,
        runtime_executable: coupling_template_runtime_executable(id),
        trigger_kinds: trigger_kinds.to_vec(),
        source: CouplingTemplateStreamPattern {
            stream: source_stream.to_string(),
            kinds: source_kinds.to_vec(),
        },
        sink: CouplingTemplateStreamPattern {
            stream: sink_stream.to_string(),
            kinds: sink_kinds.to_vec(),
        },
        must_have,
        channel_decision_required,
        summary: summary.to_string(),
    }
}

fn coupling_template_runtime_executable(id: &str) -> bool {
    matches!(
        id,
        "std::queue.task"
            | "std::queue.completion_callback"
            | "std::context.spill"
            | "std::context.truncate"
            | "std::context.summarize"
            | "std::memory.extract"
            | "std::memory.recall"
            | "std::permission.approval_gate"
            | "std::prompt.steer"
            | "std::permission.tool_gate"
            | "std::prompt.dynamic_instructions"
            | "std::schedule.cron"
            | "std::supervisor.spawn"
            | "std::supervisor.child_completion"
            | "std::retry.with_budget"
            | "std::failure.deadletter"
    )
}

#[cfg(test)]
mod tests {

    #[test]
    fn coupling_template_catalog_freezes_v1_ids_and_kernel_vocabulary() {
        let catalog = crate::agent::coupling_templates::coupling_template_catalog_v1();
        assert_eq!(
            catalog.schema,
            crate::agent::coupling_templates::COUPLING_TEMPLATE_CATALOG_SCHEMA_V1
        );
        let ids = catalog
            .templates
            .iter()
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            crate::agent::coupling_templates::coupling_template_ids_v1()
        );
        assert_eq!(
            ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
            ids.len()
        );
        let runtime_executable = catalog
            .templates
            .iter()
            .filter(|template| template.runtime_executable)
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            runtime_executable,
            vec![
                "std::queue.task",
                "std::queue.completion_callback",
                "std::context.spill",
                "std::context.truncate",
                "std::context.summarize",
                "std::memory.extract",
                "std::memory.recall",
                "std::permission.approval_gate",
                "std::permission.tool_gate",
                "std::prompt.steer",
                "std::prompt.dynamic_instructions",
                "std::schedule.cron",
                "std::supervisor.spawn",
                "std::supervisor.child_completion",
                "std::retry.with_budget",
                "std::failure.deadletter"
            ]
        );

        for template in &catalog.templates {
            assert!(!template.trigger_kinds.is_empty(), "{}", template.id);
            assert!(!template.source.kinds.is_empty(), "{}", template.id);
            assert!(!template.sink.kinds.is_empty(), "{}", template.id);
            if template.runtime_executable {
                assert!(!template.channel_decision_required, "{}", template.id);
            }
            match template.role {
                crate::agent::manifest_bind::CouplingRole::Controller => {
                    assert_eq!(template.sink.stream, "control")
                }
                crate::agent::manifest_bind::CouplingRole::Projection => {
                    assert!(
                        template.sink.stream.starts_with("derived:"),
                        "{}",
                        template.id
                    )
                }
            }
            if template.channel_decision_required {
                assert_ne!(
                    template.maturity,
                    crate::agent::coupling_templates::CouplingTemplateMaturity::KernelBacked
                );
            }
        }
    }
}
